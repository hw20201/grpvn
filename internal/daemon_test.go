package internal

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func daemonEventually(t *testing.T, check func() bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !check() {
		select {
		case <-ctx.Done():
			t.Fatal("daemon condition timed out")
		case <-ticker.C:
		}
	}
}

func startTestDaemon(t *testing.T, db *sql.DB, interval time.Duration) <-chan struct{} {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := RunDaemon(ctx, db, interval, io.Discard); err != nil {
			t.Error(err)
		}
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(10 * time.Second):
			t.Error("daemon did not exit")
		}
	})
	daemonEventually(t, func() bool { running, err := DaemonRunning(context.Background(), db); return err == nil && running })
	return done
}

func TestDaemonPullsAndPushesWithoutAgentCommands(t *testing.T) {
	serverDB := newTestDB(t)
	server := testCoordinator(t, serverDB, "secret")
	client := newTestDB(t)
	mustConnect(t, client, "work", server.URL, "secret", "#dev")
	done := startTestDaemon(t, client, 250*time.Millisecond)
	if _, err := Send(serverDB, "alice", "#dev", "downloaded by daemon", "", false); err != nil {
		t.Fatal(err)
	}
	daemonEventually(t, func() bool {
		var count int
		err := client.QueryRow("SELECT COUNT(*) FROM messages WHERE target = '#work/dev' AND body = ?", []byte("downloaded by daemon")).Scan(&count)
		return err == nil && count == 1
	})
	if _, _, err := SendSynced(context.Background(), client, "bob", "#work/dev", "uploaded by daemon", "", false, "daemon-send"); err != nil {
		t.Fatal(err)
	}
	daemonEventually(t, func() bool {
		var count int
		err := serverDB.QueryRow("SELECT COUNT(*) FROM messages WHERE body = ?", []byte("uploaded by daemon")).Scan(&count)
		return err == nil && count == 1
	})
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if running, err := StopDaemon(stopCtx, client); err != nil || !running {
		t.Fatalf("stop: %v %v", running, err)
	}
	<-done
	if state, err := ReadDaemonState(context.Background(), client); err != nil || state.Running() {
		t.Fatalf("lease survived stop: %+v %v", state, err)
	}
}

func TestDaemonImportsMessageAfterHighIdlePoll(t *testing.T) {
	serverDB := newTestDB(t)
	server := testCoordinator(t, serverDB, "secret")
	client := newTestDB(t)
	mustConnect(t, client, "work", server.URL, "secret", "#dev")
	if _, err := SyncDue(context.Background(), client, "", 250*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	r, err := getRemote(client, "work")
	if err != nil {
		t.Fatal(err)
	}
	owner, ok, err := claimSync(client, *r, true, time.Now())
	if err != nil || !ok {
		t.Fatalf("claim high-idle sync: ok=%v err=%v", ok, err)
	}
	if _, err := client.Exec("UPDATE sync_schedule SET idle_rounds = 100 WHERE remote_name='work'"); err != nil {
		t.Fatal(err)
	}
	if err := finishSync(client, *r, owner, 250*time.Millisecond, SyncStats{}, nil); err != nil {
		t.Fatal(err)
	}
	var rounds int
	if err := client.QueryRow("SELECT idle_rounds FROM sync_schedule WHERE remote_name='work'").Scan(&rounds); err != nil || rounds < 101 {
		t.Fatalf("high-idle sync did not complete: rounds=%d err=%v", rounds, err)
	}
	startTestDaemon(t, client, 250*time.Millisecond)
	daemonEventually(t, func() bool {
		var current int
		err := client.QueryRow("SELECT idle_rounds FROM sync_schedule WHERE remote_name='work'").Scan(&current)
		return err == nil && current >= 102
	})
	if _, err := Send(serverDB, "alice", "#dev", "arrived after idle poll", "", false); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		var count int
		err := client.QueryRow("SELECT COUNT(*) FROM messages WHERE target='#work/dev' AND body=?", []byte("arrived after idle poll")).Scan(&count)
		if err == nil && count == 1 {
			return
		}
		select {
		case <-ctx.Done():
			t.Fatal("message not imported within five seconds")
		case <-ticker.C:
		}
	}
}

func TestDaemonLeaseAndAutomaticSyncFallback(t *testing.T) {
	serverDB := newTestDB(t)
	server := testCoordinator(t, serverDB, "")
	client := newTestDB(t)
	mustConnect(t, client, "work", server.URL, "", "#dev")
	first, err := acquireDaemon(client, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDaemon(client, time.Second); err == nil {
		t.Fatal("second daemon acquired a live lease")
	} else {
		var existing *DaemonAlreadyRunningError
		if !errors.As(err, &existing) {
			t.Fatal(err)
		}
	}
	if _, err := Send(serverDB, "alice", "#dev", "new", "", false); err != nil {
		t.Fatal(err)
	}
	if stats, err := SyncAvailable(context.Background(), client); err != nil || stats.Pulled != 0 {
		t.Fatalf("command synced while daemon owned the lease: %+v %v", stats, err)
	}
	msg, _, err := SendSynced(context.Background(), client, "bob", "#work/dev", "pending", "", false, "")
	if err != nil || !msg.Pending {
		t.Fatalf("daemon send should be queued locally: %+v %v", msg, err)
	}
	if _, err := client.Exec("UPDATE daemon_state SET heartbeat_at = ?", time.Now().Add(-2*daemonLeaseTTL).UnixMilli()); err != nil {
		t.Fatal(err)
	}
	if stats, err := SyncAvailable(context.Background(), client); err != nil || stats.Pulled != 1 || stats.Pushed != 1 {
		t.Fatalf("stale worker prevented fallback: %+v %v", stats, err)
	}
	second, err := acquireDaemon(client, time.Second)
	if err != nil || second.Owner == first.Owner {
		t.Fatalf("stale lease was not recovered: %+v %v", second, err)
	}
	if _, err := client.Exec("DELETE FROM daemon_state WHERE owner = ?", first.Owner); err != nil {
		t.Fatal(err)
	}
	state, err := ReadDaemonState(context.Background(), client)
	if err != nil || state.Owner != second.Owner {
		t.Fatalf("old worker removed successor's lease: %+v %v", state, err)
	}
}

func TestDaemonStopCancelsRemoteRequest(t *testing.T) {
	serverDB := newTestDB(t)
	handler, err := CoordinatorHandler(serverDB, CoordinatorOptions{Open: true})
	if err != nil {
		t.Fatal(err)
	}
	var blocked atomic.Bool
	entered := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if blocked.Load() {
			_, _ = io.Copy(io.Discard, r.Body) // enable transport cancellation after the POST body
			select {
			case entered <- struct{}{}:
			default:
			}
			<-r.Context().Done()
			return
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := newTestDB(t)
	mustConnect(t, client, "work", server.URL, "", "#dev")
	blocked.Store(true)
	done := startTestDaemon(t, client, time.Second)
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("daemon did not start its request")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if running, err := StopDaemon(ctx, client); err != nil || !running {
		t.Fatalf("offline daemon would not stop: %v %v", running, err)
	}
	<-done
}

func TestDaemonMetadataDoesNotWakeWaiter(t *testing.T) {
	db := newTestDB(t)
	if _, err := Send(db, "alice", "#dev", "old chatter", "", false); err != nil {
		t.Fatal(err)
	}
	if _, err := acquireDaemon(db, time.Second); err != nil {
		t.Fatal(err)
	}
	writer, err := OpenDB()
	if err != nil {
		t.Fatal(err)
	}
	defer writer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		ticker := time.NewTicker(20 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				_, _ = writer.Exec("UPDATE daemon_state SET heartbeat_at = ?", time.Now().UnixMilli())
			}
		}
	}()
	defer func() { cancel(); <-done }()
	var output bytes.Buffer
	st := &State{Name: "bob", Follow: []string{"#dev"}}
	code, err := Wait(context.Background(), &output, db, waitLoader(st), 250*time.Millisecond, 10*time.Millisecond)
	if err != nil || code != 2 || output.Len() != 0 {
		t.Fatalf("metadata woke waiter: code=%d output=%q err=%v", code, &output, err)
	}
}
