package internal

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"net"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"
)

// Only committed writes on the coordinator's DB handle ring this doorbell.
// Direct SQL and separate-process writes are reconciled at startup, not polled.
var messageHub = struct {
	sync.Mutex
	listeners map[*sql.DB]map[chan struct{}]struct{}
}{listeners: map[*sql.DB]map[chan struct{}]struct{}{}}

func signalCommittedMessage(db *sql.DB) {
	messageHub.Lock()
	defer messageHub.Unlock()
	for ch := range messageHub.listeners[db] {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

func addMessageListener(db *sql.DB) (chan struct{}, func()) {
	ch := make(chan struct{}, 1)
	messageHub.Lock()
	if messageHub.listeners[db] == nil {
		messageHub.listeners[db] = map[chan struct{}]struct{}{}
	}
	messageHub.listeners[db][ch] = struct{}{}
	messageHub.Unlock()
	return ch, func() {
		messageHub.Lock()
		delete(messageHub.listeners[db], ch)
		if len(messageHub.listeners[db]) == 0 {
			delete(messageHub.listeners, db)
		}
		messageHub.Unlock()
	}
}

type messageTriggerPayload struct {
	Version    int               `json:"version"`
	DeliveryID string            `json:"delivery_id"`
	Agent      string            `json:"agent"`
	SessionID  string            `json:"session_id"`
	Thread     string            `json:"thread"`
	Events     []ActivationEvent `json:"events"`
	Input      string            `json:"input"`
}

// StartMessageTriggers has no idle timer or HTTP polling. Timers exist only
// for durable pending work (debounce, execution cooldown, or error recovery).
// The caller must stop it before closing db. Receiver acceptance is not read.
func StartMessageTriggers(ctx context.Context, db *sql.DB, opts CoordinatorOptions, output io.Writer) (func(), error) {
	access, err := newChannelAccess(opts)
	if err != nil {
		return nil, err
	}
	access.db = db
	ch, unregister := addMessageListener(db)
	if err := ScanActivationEvents(db); err != nil {
		unregister()
		return nil, err
	}
	child, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	var outputMu sync.Mutex
	logError := func(err error) {
		if output != nil && child.Err() == nil {
			outputMu.Lock()
			fmt.Fprintln(output, "message triggers:", err)
			outputMu.Unlock()
		}
	}
	go func() {
		defer close(done)
		defer unregister()
		var workers sync.WaitGroup
		defer workers.Wait()
		slots := make(chan struct{}, 4)
		failures := 0
		for child.Err() == nil {
			// Drain paginated scans without waiting for another message signal.
			var workErr error
			for page := 0; page < 20; page++ {
				if workErr = ScanActivationEvents(db); workErr != nil {
					break
				}
				var behind bool
				workErr = db.QueryRow(`SELECT EXISTS(SELECT 1 FROM agent_sessions s JOIN activation_owners o ON o.session_id=s.id WHERE s.runtime='webhook' AND EXISTS(SELECT 1 FROM messages m WHERE m.seq>s.scan_position))`).Scan(&behind)
				if workErr != nil || !behind {
					break
				}
				if page == 19 {
					signalCommittedMessage(db)
				}
			}
			var next time.Time
			if workErr == nil {
				workErr = expireWebhookClaims(db)
			}
			var sessions []AgentSession
			if workErr == nil {
				sessions, workErr = Sessions(db, true)
			}
			if workErr == nil {
				for _, s := range sessions {
					if s.Runtime != "webhook" || s.Mode != "activate" || s.Status == "paused" || !s.Supervise {
						continue
					}
					if len(slots) == cap(slots) {
						// A worker completion will ring the doorbell; do not spin
						// on an overdue pending timestamp while all slots are busy.
						next = time.Time{}
						break
					}
					due, err := nextWebhookWake(db, s)
					if err != nil {
						workErr = err
						break
					}
					if !due.IsZero() && !due.After(time.Now()) {
						a, err := ClaimActivation(db, s, 2*time.Second)
						if err != nil {
							workErr = err
							break
						}
						if a != nil {
							slots <- struct{}{}
							workers.Add(1)
							go func(a Activation) {
								defer workers.Done()
								defer func() { <-slots; signalCommittedMessage(db) }()
								if err := dispatchWebhook(child, db, access, a); err != nil {
									_ = recordDispatchFailure(db, a.ID, err)
									logError(err)
								}
							}(*a)
						}
						due, workErr = nextWebhookWake(db, s)
						if workErr != nil {
							break
						}
					}
					if !due.IsZero() && (next.IsZero() || due.Before(next)) {
						next = due
					}
				}
			}
			if workErr != nil {
				logError(workErr)
				failures++
				next = time.Now().Add(backoff(time.Second, failures, maxFailureInterval))
			} else {
				failures = 0
			}
			var timer *time.Timer
			var wake <-chan time.Time
			if !next.IsZero() {
				timer = time.NewTimer(max(time.Millisecond, time.Until(next)))
				wake = timer.C
			}
			select {
			case <-child.Done():
			case <-ch:
			case <-wake:
			}
			if timer != nil {
				timer.Stop()
			}
		}
	}()
	var once sync.Once
	return func() { once.Do(func() { cancel(); <-done }) }, nil
}

// This mirrors ClaimDelivery's scheduling constraints, without polling them.
// A worker rings the doorbell on completion; a lost claim expires conservatively.
func nextWebhookWake(db *sql.DB, s AgentSession) (time.Time, error) {
	var active sql.NullInt64
	if err := db.QueryRow(`SELECT MIN(updated_at) FROM activation_runs WHERE session_id=? AND status IN ('queued','dispatching','running')`, s.ID).Scan(&active); err != nil {
		return time.Time{}, err
	}
	if active.Valid {
		return time.UnixMilli(active.Int64).Add(2 * time.Minute), nil
	}
	var first, last int64
	err := db.QueryRow(`SELECT MIN(created_at),MAX(created_at) FROM activation_events WHERE session_id=? AND status='pending' AND action='activate' AND consumed_at IS NULL GROUP BY thread_root ORDER BY MIN(created_at) LIMIT 1`, s.ID).Scan(&first, &last)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, nil
	}
	if err != nil {
		return time.Time{}, err
	}
	due := min(last+2000, first+15000)
	var lastRun int64
	if err := db.QueryRow(`SELECT COALESCE(MAX(r.created_at),0) FROM activation_runs r WHERE r.session_id=? AND EXISTS(SELECT 1 FROM activation_run_events re JOIN activation_events e ON e.id=re.event_id WHERE re.run_id=r.id AND e.action='activate')`, s.ID).Scan(&lastRun); err != nil {
		return time.Time{}, err
	}
	due = max(due, lastRun+10000)
	var cooldown int64
	err = db.QueryRow("SELECT next_at FROM webhook_cooldowns WHERE session_id=?", s.ID).Scan(&cooldown)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, err
	}
	return time.UnixMilli(max(due, cooldown)), nil
}

func expireWebhookClaims(db *sql.DB) error {
	tx, err := beginWrite(db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`UPDATE activation_runs SET status='uncertain',detail='webhook dispatcher stopped before acknowledgement' WHERE session_id IN(SELECT id FROM agent_sessions WHERE runtime='webhook') AND status IN ('queued','dispatching','running') AND updated_at<=?`, time.Now().Add(-2*time.Minute).UnixMilli()); err != nil {
		return err
	}
	if _, err := tx.Exec(`UPDATE activation_events SET status='uncertain',detail='webhook dispatcher stopped before acknowledgement' WHERE status IN ('queued','dispatching','running') AND consumed_at IS NULL AND id IN(SELECT re.event_id FROM activation_run_events re JOIN activation_runs r ON r.id=re.run_id WHERE r.status='uncertain' AND r.detail='webhook dispatcher stopped before acknowledgement')`); err != nil {
		return err
	}
	return tx.Commit()
}

func validWebhookEndpoint(endpoint string) error {
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" || u.User != nil || u.RawQuery != "" || u.Fragment != "" || u.Opaque != "" || (u.Scheme != "https" && !(u.Scheme == "http" && isLoopbackHost(u.Hostname()))) {
		return fmt.Errorf("webhook endpoint must be HTTPS (literal loopback HTTP is allowed), without URL credentials, query or fragment")
	}
	return nil
}

func dispatchWebhook(ctx context.Context, db *sql.DB, access *channelAccess, a Activation) error {
	var status string
	if err := db.QueryRow("SELECT status FROM activation_runs WHERE id=? AND session_id=?", a.ID, a.Session.ID).Scan(&status); err != nil {
		return err
	}
	if status != "dispatching" {
		return nil
	}
	// Refresh policy and endpoint just before dispatch, not merely at scan time.
	sessions, err := Sessions(db, false)
	if err != nil {
		return err
	}
	var current *AgentSession
	for i := range sessions {
		if sessions[i].ID == a.Session.ID {
			current = &sessions[i]
			break
		}
	}
	suppress := func(detail string) error { return FinishActivation(db, a.ID, "suppressed", detail, "", "") }
	if current == nil || current.Runtime != "webhook" || current.Mode != "activate" || current.Status == "paused" || !current.Supervise {
		return suppress("webhook execution policy changed")
	}
	s := *current
	var owner bool
	if err := db.QueryRow("SELECT EXISTS(SELECT 1 FROM activation_owners WHERE session_id=?)", s.ID).Scan(&owner); err != nil {
		return err
	}
	if !owner {
		return suppress("webhook delivery owner changed")
	}
	p, err := findPrincipal(db, "id=?", s.RuntimeSession)
	if err != nil || p.Status != "active" || p.Kind != "agent" {
		return suppress("webhook principal is unavailable")
	}
	if err := cachePrincipal(db, p); err != nil || !principalOwnsAddress(p, s.Agent) {
		return suppress("webhook principal or owner registration changed")
	}
	// Re-read each receipt and permission: an event may have been consumed,
	// a channel grant removed, or the subscription changed while queued.
	var unread []ActivationEvent
	for _, e := range a.Events {
		var pending bool
		if err := db.QueryRow(`SELECT EXISTS(SELECT 1 FROM activation_events e JOIN activation_run_events re ON re.event_id=e.id WHERE e.id=? AND e.session_id=? AND re.run_id=? AND e.consumed_at IS NULL AND e.action='activate' AND e.status='dispatching')`, e.ID, s.ID, a.ID).Scan(&pending); err != nil {
			return err
		}
		if !pending {
			continue
		}
		subscribed := e.Target == "@"+s.Agent
		for _, channel := range s.Channels {
			subscribed = subscribed || (strings.HasPrefix(channel, "#") && e.Target == channel)
		}
		if !subscribed || !rpcAllowed(db, access, p, e.Target, false) {
			return suppress("webhook principal no longer has subscription or read access")
		}
		unread = append(unread, e)
	}
	if len(unread) == 0 {
		return suppress("addressed messages already consumed")
	}
	if err := validWebhookEndpoint(s.Endpoint); err != nil {
		return err
	}
	secret := os.Getenv(s.CredentialEnv)
	if s.CredentialEnv == "" || secret == "" {
		return fmt.Errorf("webhook credential environment variable is unset")
	}
	a.Session, a.Events = s, unread
	a.Prompt = activationPrompt(a)
	body, err := json.Marshal(messageTriggerPayload{Version: 1, DeliveryID: a.ID, Agent: s.Agent, SessionID: s.ID, Thread: a.Thread, Events: a.Events, Input: a.Prompt})
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, "POST", s.Endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+secret)
	req.Header.Set("Idempotency-Key", a.ID)
	req.Header.Set("X-Grpvn-Delivery-ID", a.ID)
	resp, err := adapterHTTP.Do(req)
	if err == nil {
		defer resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode >= 300 {
			err = &remoteHTTPError{Status: resp.StatusCode, Message: "webhook request failed", RetryAt: retryAfter(resp.Header.Get("Retry-After"), time.Now())}
		}
	}
	if err != nil {
		if cooldownErr := recordWebhookCooldown(db, s.ID, err); cooldownErr != nil {
			return fmt.Errorf("webhook request failed; could not persist cooldown: %w", cooldownErr)
		}
		return err
	}
	if _, err := db.Exec("DELETE FROM webhook_cooldowns WHERE session_id=?", s.ID); err != nil {
		return err
	}
	return FinishActivation(db, a.ID, "accepted", "webhook transport accepted; agent execution and reading are not confirmed", "", "")
}

// Retry-After delays future messages; a request with a lost acknowledgement is
// never automatically re-POSTed. Only a receiver's own receipt proves execution.
func recordWebhookCooldown(db *sql.DB, session string, failure error) error {
	tx, err := beginWrite(db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var failures int
	err = tx.QueryRow("SELECT failures FROM webhook_cooldowns WHERE session_id=?", session).Scan(&failures)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	failures++
	delay := backoff(time.Second, failures, maxFailureInterval)
	next := time.Now().Add(time.Duration(float64(delay) * (1 + rand.Float64()*0.2)))
	var remoteErr *remoteHTTPError
	if errors.As(failure, &remoteErr) && remoteErr.RetryAt.After(next) {
		next = remoteErr.RetryAt
	}
	if _, err := tx.Exec(`INSERT INTO webhook_cooldowns(session_id,next_at,failures) VALUES(?,?,?) ON CONFLICT(session_id) DO UPDATE SET next_at=MAX(webhook_cooldowns.next_at,excluded.next_at),failures=excluded.failures`, session, next.UnixMilli(), failures); err != nil {
		return err
	}
	return tx.Commit()
}

func isLoopbackHost(host string) bool {
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}
