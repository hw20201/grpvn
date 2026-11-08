package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const triggerTestSecret = "test-webhook-token"

func registerWebhookFixture(t *testing.T, db *sql.DB, endpoint string) (AgentSession, string) {
	t.Helper()
	seedTestAgent(t, db, "webhook-agent-credential", "trigger-bot", []string{"#team"})
	var principalID string
	if err := db.QueryRow("SELECT id FROM principals WHERE name=?", "trigger-bot").Scan(&principalID); err != nil {
		t.Fatal(err)
	}
	t.Setenv("GRPVN_TEST_WEBHOOK_TOKEN", triggerTestSecret)
	s := AgentSession{
		Agent: "trigger-bot", Runtime: "webhook", RuntimeSession: principalID,
		Mode: "activate", Supervise: true, Channels: []string{"#team"},
		Endpoint: endpoint, CredentialEnv: "GRPVN_TEST_WEBHOOK_TOKEN",
	}
	id, err := RegisterSession(db, s, true, false)
	if err != nil {
		t.Fatal(err)
	}
	s.ID = id
	return s, principalID
}

func sendWebhookFixtureMessage(t *testing.T, db *sql.DB, key string) *Message {
	t.Helper()
	if key == "" {
		m, err := Send(db, "alice", "#team", "private callback payload", "", false)
		if err != nil {
			t.Fatal(err)
		}
		return m
	}
	m, _, err := SendIdempotent(db, "alice", "#team", "private callback payload", "", false, key)
	if err != nil {
		t.Fatal(err)
	}
	return m
}

func claimWebhookFixture(t *testing.T, db *sql.DB, s AgentSession) Activation {
	t.Helper()
	if err := ScanActivationEvents(db); err != nil {
		t.Fatal(err)
	}
	activation, err := ClaimActivation(db, s, 0)
	if err != nil || activation == nil {
		t.Fatalf("claim activation: %+v (%v)", activation, err)
	}
	return *activation
}

func TestMessageTriggersIdleAndCommittedMessageDispatchOnce(t *testing.T) {
	db := newTestDB(t)
	var requests atomic.Int64
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+triggerTestSecret {
			t.Errorf("unexpected webhook request: %s auth=%q", r.Method, r.Header.Get("Authorization"))
		}
		if r.Header.Get("Idempotency-Key") == "" {
			t.Error("missing delivery idempotency key")
		}
		var body messageTriggerPayload
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Errorf("decode webhook body: %v", err)
			return
		}
		if body.DeliveryID == "" || len(body.Events) != 1 || body.Events[0].MessageID == "" {
			t.Errorf("incomplete callback event: %+v", body)
		}
		if r.Header.Get("Idempotency-Key") != body.DeliveryID {
			t.Errorf("idempotency key %q does not match delivery id %q", r.Header.Get("Idempotency-Key"), body.DeliveryID)
		}
		if strings.Contains(string(mustJSON(t, body)), "private callback payload") {
			t.Error("webhook leaked raw message body")
		}
		w.WriteHeader(http.StatusAccepted)
	}))
	defer callback.Close()
	s, _ := registerWebhookFixture(t, db, callback.URL)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := StartMessageTriggers(ctx, db, CoordinatorOptions{}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	// An idle dispatcher must not poll or make a speculative HTTP request.
	time.Sleep(100 * time.Millisecond)
	if got := requests.Load(); got != 0 {
		t.Fatalf("idle dispatcher made %d requests", got)
	}
	msg := sendWebhookFixtureMessage(t, db, "trigger-idempotency")
	deadline := time.Now().Add(4 * time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if got := requests.Load(); got != 1 {
		t.Fatalf("committed message callback count = %d, want 1", got)
	}
	replayed, duplicate, err := SendIdempotent(db, "alice", "#team", "private callback payload", "", false, "trigger-idempotency")
	if err != nil || !duplicate || replayed.ID != msg.ID {
		t.Fatalf("idempotent replay: %+v %t %v", replayed, duplicate, err)
	}
	signalCommittedMessage(db)
	time.Sleep(100 * time.Millisecond)
	if got := requests.Load(); got != 1 {
		t.Fatalf("duplicate signal/replay caused %d callbacks", got)
	}
	_ = s
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestDispatchWebhookRechecksPrincipalAndChannelAccess(t *testing.T) {
	for _, tc := range []struct {
		name  string
		setup func(*testing.T, *sql.DB, string)
	}{
		{"principal revoked", func(t *testing.T, db *sql.DB, id string) {
			if _, err := db.Exec("UPDATE principals SET status='revoked' WHERE id=?", id); err != nil {
				t.Fatal(err)
			}
		}},
		{"channel permission removed", func(t *testing.T, db *sql.DB, id string) {
			if _, err := db.Exec("DELETE FROM permissions WHERE principal_id=? AND channel='#team'", id); err != nil {
				t.Fatal(err)
			}
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			db := newTestDB(t)
			var requests atomic.Int64
			callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				w.WriteHeader(http.StatusNoContent)
			}))
			defer callback.Close()
			s, principalID := registerWebhookFixture(t, db, callback.URL)
			sendWebhookFixtureMessage(t, db, "")
			a := claimWebhookFixture(t, db, s)
			tc.setup(t, db, principalID)
			access, err := newChannelAccess(CoordinatorOptions{})
			if err != nil {
				t.Fatal(err)
			}
			access.db = db
			err = dispatchWebhook(context.Background(), db, access, a)
			if err != nil {
				t.Fatalf("revoked delivery should be suppressed without error: %v", err)
			}
			if requests.Load() != 0 {
				t.Fatal("revoked principal/channel reached callback")
			}
			var status string
			if err := db.QueryRow("SELECT status FROM activation_runs WHERE id=?", a.ID).Scan(&status); err != nil || status != "suppressed" {
				t.Fatalf("delivery status=%q err=%v; want suppressed", status, err)
			}
		})
	}
}

func TestDispatchWebhookRetryAfterIsDurableAndDoesNotRepeatAmbiguousRun(t *testing.T) {
	db := newTestDB(t)
	var requests atomic.Int64
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.Header().Set("Retry-After", "120")
		w.WriteHeader(http.StatusTooManyRequests)
	}))
	defer callback.Close()
	s, _ := registerWebhookFixture(t, db, callback.URL)
	sendWebhookFixtureMessage(t, db, "")
	a := claimWebhookFixture(t, db, s)
	access, err := newChannelAccess(CoordinatorOptions{})
	if err != nil {
		t.Fatal(err)
	}
	access.db = db
	before := time.Now()
	if err := dispatchWebhook(context.Background(), db, access, a); err == nil {
		t.Fatal("429 should be returned as a delivery error")
	} else if err := recordDispatchFailure(db, a.ID, err); err != nil {
		t.Fatal(err)
	}
	sendWebhookFixtureMessage(t, db, "")
	if err := ScanActivationEvents(db); err != nil {
		t.Fatal(err)
	}
	var nextAt int64
	var failures int
	if err := db.QueryRow("SELECT next_at,failures FROM webhook_cooldowns WHERE session_id=?", s.ID).Scan(&nextAt, &failures); err != nil || failures != 1 || nextAt < before.Add(119*time.Second).UnixMilli() {
		t.Fatalf("persisted cooldown: next=%d failures=%d error=%v", nextAt, failures, err)
	}
	wake, err := nextWebhookWake(db, s)
	if err != nil {
		t.Fatal(err)
	}
	if wake.Before(before.Add(119 * time.Second)) {
		t.Fatalf("wake deadline %s did not preserve Retry-After", wake)
	}
	if requests.Load() != 1 {
		t.Fatalf("requests=%d, want one ambiguous attempt", requests.Load())
	}
	if err := dispatchWebhook(context.Background(), db, access, a); err != nil {
		t.Fatal(err)
	}
	if requests.Load() != 1 {
		t.Fatal("ambiguous run was automatically retried")
	}
}

func TestMessageTriggersStartupRecoveryAndFastStop(t *testing.T) {
	db := newTestDB(t)
	var requests atomic.Int64
	callback := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusAccepted)
	}))
	defer callback.Close()
	s, _ := registerWebhookFixture(t, db, callback.URL)
	sendWebhookFixtureMessage(t, db, "")
	if err := ScanActivationEvents(db); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE activation_events SET created_at=? WHERE session_id=?", time.Now().Add(-3*time.Second).UnixMilli(), s.ID); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stop, err := StartMessageTriggers(ctx, db, CoordinatorOptions{}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	defer stop()
	deadline := time.Now().Add(4 * time.Second)
	for requests.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if requests.Load() != 1 {
		t.Fatalf("startup reconciliation requests=%d", requests.Load())
	}
	started := time.Now()
	cancel()
	stop()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stop took %s with no active callback", elapsed)
	}
}

func TestValidWebhookEndpointRejectsUnsafeTargets(t *testing.T) {
	for _, endpoint := range []string{"", "file:///tmp/x", "http://example.com/hook", "http://localhost/hook", "https://user:pass@example.com/hook", "https://example.com/hook?x=1", "https://example.com/hook#frag"} {
		if err := validWebhookEndpoint(endpoint); err == nil {
			t.Errorf("accepted unsafe endpoint %q", endpoint)
		}
	}
	for _, endpoint := range []string{"https://hooks.example.com/events", "http://127.0.0.1:8080/hook", "http://[::1]:8080/hook"} {
		if err := validWebhookEndpoint(endpoint); err != nil {
			t.Errorf("rejected valid endpoint %q: %v", endpoint, err)
		}
	}
}

func TestCommittedMessageDoorbellsDoNotSignalRollbackOrReplay(t *testing.T) {
	db := newTestDB(t)
	seedTestAgent(t, db, "sender-token", "sender", []string{"#team"})
	p, err := findPrincipal(db, "name=?", "sender")
	if err != nil {
		t.Fatal(err)
	}
	ch, done := addMessageListener(db)
	defer done()
	assertSignal := func(want bool) {
		t.Helper()
		select {
		case <-ch:
			if !want {
				t.Fatal("rollback/replay rang the message doorbell")
			}
		default:
			if want {
				t.Fatal("committed message did not ring the doorbell")
			}
		}
	}
	tx, err := beginWrite(db)
	if err != nil {
		t.Fatal(err)
	}
	if err := NewMessage("sender", "#team", []byte("rolled back")).save(tx); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	assertSignal(false)
	if _, _, err := sendRegistered(db, p, "#team", "registered MCP message", "mcp-key"); err != nil {
		t.Fatal(err)
	}
	assertSignal(true)
	if _, duplicate, err := sendRegistered(db, p, "#team", "registered MCP message", "mcp-key"); err != nil || !duplicate {
		t.Fatalf("MCP replay: %t %v", duplicate, err)
	}
	assertSignal(false)
	wire := toWire(NewMessage("sender", "#team", []byte("remote upload")))
	if _, err := acceptAttributedMessage(db, wire, p); err != nil {
		t.Fatal(err)
	}
	assertSignal(true)
	if _, err := acceptAttributedMessage(db, wire, p); err != nil {
		t.Fatal(err)
	}
	assertSignal(false)
}

func TestWebhookExcludesOtherChannelsUnapprovedAndAutomatedSenders(t *testing.T) {
	for _, scenario := range []string{"other channel", "unapproved sender", "automated reply", "notification only", "self"} {
		t.Run(scenario, func(t *testing.T) {
			db := newTestDB(t)
			s, _ := registerWebhookFixture(t, db, "https://receiver.example/hook")
			switch scenario {
			case "other channel":
				if _, err := Send(db, "alice", "#other", "not subscribed", "", false); err != nil {
					t.Fatal(err)
				}
			case "unapproved sender":
				seedTestAgent(t, db, "unapproved-token", "unapproved", []string{"#team"})
				p, err := findPrincipal(db, "name=?", "unapproved")
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := sendRegistered(db, p, "#team", "not approved for inference", ""); err != nil {
					t.Fatal(err)
				}
			case "automated reply":
				if _, _, err := SendIdempotent(db, "alice", "#team", "automatic acknowledgement", "", false, "activation:test"); err != nil {
					t.Fatal(err)
				}
			case "notification only":
				if _, err := db.Exec("UPDATE agent_sessions SET mode='notify' WHERE id=?", s.ID); err != nil {
					t.Fatal(err)
				}
				if _, err := Send(db, "alice", "@trigger-bot", "a notification, not permission to infer", "", false); err != nil {
					t.Fatal(err)
				}
			case "self":
				if _, err := Send(db, "trigger-bot", "#team", "self", "", false); err != nil {
					t.Fatal(err)
				}
			}
			if err := ScanActivationEvents(db); err != nil {
				t.Fatal(err)
			}
			if a, err := ClaimActivation(db, s, 0); err != nil || a != nil {
				t.Fatalf("unexpected automatic activation: %+v %v", a, err)
			}
			if wake, err := nextWebhookWake(db, s); err != nil || !wake.IsZero() {
				t.Fatalf("excluded traffic armed a work timer: %v %v", wake, err)
			}
		})
	}
}

func TestMessageTriggersStopCancelsPendingDebounce(t *testing.T) {
	db := newTestDB(t)
	registerWebhookFixture(t, db, "https://receiver.example/hook")
	stop, err := StartMessageTriggers(context.Background(), db, CoordinatorOptions{}, io.Discard)
	if err != nil {
		t.Fatal(err)
	}
	sendWebhookFixtureMessage(t, db, "")
	started := time.Now()
	stop()
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("stop waited for debounce: %s", elapsed)
	}
	messageHub.Lock()
	remaining := len(messageHub.listeners[db])
	messageHub.Unlock()
	if remaining != 0 {
		t.Fatalf("%d listeners leaked after stop", remaining)
	}
}
