package internal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func privateWorkspace(t *testing.T) (*httptest.Server, func(string, []string, bool) (string, string)) {
	t.Helper()
	db := newTestDB(t)
	h, err := CoordinatorHandler(db, CoordinatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	return server, func(person string, channels []string, write bool) (string, string) {
		id, secret, err := CreateInvite(db, person, channels, write, time.Hour, 1)
		if err != nil {
			t.Fatal(err)
		}
		return id, secret
	}
}

func TestMemberApprovalAndAgentPermissions(t *testing.T) {
	serverDB := newTestDB(t)
	h, err := CoordinatorHandler(serverDB, CoordinatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(h)
	t.Cleanup(server.Close)
	if _, err := Send(serverDB, "local", "#dev", "history", "", false); err != nil {
		t.Fatal(err)
	}
	client := newTestDB(t)
	mustConnect(t, client, "work", server.URL, "", "#dev")
	registration, err := RemoteRegistration(client, "work")
	if err != nil || registration.Status != "pending" {
		t.Fatalf("registration: %+v %v", registration, err)
	}
	if err := ApproveMember(serverDB, registration.ApprovalCode, "Alice laptop", []string{"#dev"}, true); err != nil {
		t.Fatal(err)
	}
	mustConnect(t, client, "work", server.URL, "", "#dev")
	mustSync(t, client, "work")
	m, _, err := SendSynced(context.Background(), client, "review-bot", "#work/dev", "approved identity", "", false, "")
	if err != nil || m.Pending {
		t.Fatalf("send: %+v %v", m, err)
	}
	got := messageIn(t, serverDB, "#dev", "approved identity")
	if got.Sender == "review-bot" {
		t.Fatal("registered sender lost its owner namespace")
	}
	remote, err := getRemote(client, "work")
	if err != nil {
		t.Fatal(err)
	}
	root, _ := localCredential(client, remote.ServerID, "")
	remote.Token = root.Secret
	actor, err := agentRemote(context.Background(), client, *remote, "review-bot", "#dev")
	if err != nil {
		t.Fatal(err)
	}
	forged := toWire(NewMessage("someone-else", "#dev", []byte("forged")))
	var ack wireMessage
	if err := remoteRequest(context.Background(), actor, "POST", "/v1/messages", forged, &ack); err == nil {
		t.Fatal("registered agent impersonated another identity")
	}
	if _, err := serverDB.Exec("UPDATE principals SET status='revoked' WHERE id=?", registration.ID); err != nil {
		t.Fatal(err)
	}
	if err := remoteRequest(context.Background(), actor, "GET", "/v1/messages?channel=%23dev", nil, &messagePage{}); err == nil {
		t.Fatal("revoking a device did not revoke its agents")
	}
}

func TestInviteSingleUseAndReadOnly(t *testing.T) {
	server, invite := privateWorkspace(t)
	_, secret := invite("Reader", []string{"#dev"}, false)
	first := newTestDB(t)
	mustConnect(t, first, "work", server.URL, secret, "#dev")
	second := newTestDB(t)
	if err := ConnectRemote(context.Background(), second, "work", server.URL, secret, []string{"#dev"}); err == nil {
		t.Fatal("single-use invitation was redeemed twice")
	}
	m, _, err := SendSynced(context.Background(), first, "reader-bot", "#work/dev", "not allowed", "", false, "")
	if err != nil || !m.Pending {
		t.Fatalf("read-only upload should remain queued: %+v %v", m, err)
	}
}

func TestSharedPollingBudgetAndRetryAfter(t *testing.T) {
	serverDB := newTestDB(t)
	handler, err := CoordinatorHandler(serverDB, CoordinatorOptions{Open: true})
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int64
	var throttle atomic.Bool
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/v1/sync" {
			calls.Add(1)
			if throttle.Load() {
				w.Header().Set("Retry-After", "120")
				apiError(w, 429, "slow down")
				return
			}
		}
		handler.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	client := newTestDB(t)
	mustConnect(t, client, "public", server.URL, "", "#lobby")
	for i := 0; i < 5; i++ {
		if _, err := SyncDue(context.Background(), client, "", time.Second); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("idle callers did not share cached schedule: %d", calls.Load())
	}
	throttle.Store(true)
	if _, err := SyncRemotes(context.Background(), client, "public"); err == nil {
		t.Fatal("expected server throttling")
	}
	before := calls.Load()
	if _, err := SyncRemotes(context.Background(), client, "public"); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != before {
		t.Fatal("explicit sync ignored Retry-After")
	}
	var retry int64
	if err := client.QueryRow("SELECT retry_at FROM sync_schedule WHERE remote_name='public'").Scan(&retry); err != nil || retry < time.Now().Add(110*time.Second).UnixMilli() {
		t.Fatalf("retry deadline: %d %v", retry, err)
	}
	reopened, err := OpenDB()
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if _, err := SyncDue(context.Background(), reopened, "", time.Second); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != before {
		t.Fatal("restart lost server cooldown")
	}
}

func TestBackoffBounds(t *testing.T) {
	if got := backoff(time.Second, 100, maxIdleInterval); got != 2*time.Second {
		t.Fatal(got)
	}
	if got := backoff(time.Second, 100, maxFailureInterval); got != 5*time.Minute {
		t.Fatal(got)
	}
	if got := backoff(time.Millisecond, 0, maxIdleInterval); got != minRemoteInterval {
		t.Fatal(got)
	}
}

func TestFinishSyncCapsHealthyIdleDeadline(t *testing.T) {
	db := newTestDB(t)
	serverDB := newTestDB(t)
	server := testCoordinator(t, serverDB, "secret")
	mustConnect(t, db, "work", server.URL, "secret", "#dev")
	r, err := getRemote(db, "work")
	if err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	owner, ok, err := claimSync(db, *r, false, started)
	if err != nil || !ok {
		t.Fatalf("claim: ok=%v err=%v", ok, err)
	}
	if _, err := db.Exec("UPDATE sync_schedule SET idle_rounds = 100, failures = 7 WHERE remote_name = 'work'"); err != nil {
		t.Fatal(err)
	}
	started = time.Now()
	if err := finishSync(db, *r, owner, time.Second, SyncStats{}, nil); err != nil {
		t.Fatal(err)
	}
	var next int64
	var idle, failures int
	if err := db.QueryRow("SELECT next_at, idle_rounds, failures FROM sync_schedule WHERE remote_name='work'").Scan(&next, &idle, &failures); err != nil {
		t.Fatal(err)
	}
	deadline := time.UnixMilli(next)
	if deadline.Before(started.Add(2*time.Second-time.Millisecond)) || deadline.After(time.Now().Add(2400*time.Millisecond)) {
		t.Fatalf("idle deadline outside positive-jitter cap: %v", deadline.Sub(started))
	}
	if idle != 101 || failures != 0 {
		t.Fatalf("idle/failure counters: idle=%d failures=%d", idle, failures)
	}
	if _, err := db.Exec("UPDATE sync_schedule SET idle_rounds = 100 WHERE remote_name='work'"); err != nil {
		t.Fatal(err)
	}
	owner, ok, err = claimSync(db, *r, true, time.Now())
	if err != nil || !ok {
		t.Fatalf("custom-base claim: ok=%v err=%v", ok, err)
	}
	started = time.Now()
	if err := finishSync(db, *r, owner, 10*time.Second, SyncStats{}, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT next_at FROM sync_schedule WHERE remote_name='work'").Scan(&next); err != nil {
		t.Fatal(err)
	}
	deadline = time.UnixMilli(next)
	if deadline.Before(started.Add(10*time.Second-time.Millisecond)) || deadline.After(time.Now().Add(12*time.Second)) {
		t.Fatalf("custom base interval was not preserved: %v", deadline.Sub(started))
	}
	if _, err := db.Exec("UPDATE sync_schedule SET idle_rounds = 100 WHERE remote_name='work'"); err != nil {
		t.Fatal(err)
	}
	owner, ok, err = claimSync(db, *r, true, time.Now())
	if err != nil || !ok {
		t.Fatalf("traffic claim: ok=%v err=%v", ok, err)
	}
	if err := finishSync(db, *r, owner, time.Second, SyncStats{Pulled: 1}, nil); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow("SELECT idle_rounds FROM sync_schedule WHERE remote_name='work'").Scan(&idle); err != nil || idle != 0 {
		t.Fatalf("traffic did not reset idle rounds: %d %v", idle, err)
	}
}
