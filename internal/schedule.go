package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strconv"
	"time"
)

const minRemoteInterval = 250 * time.Millisecond

// Frequent healthy checks reduce notification lag, at the cost of more idle traffic.
const maxIdleInterval = 2 * time.Second
const maxFailureInterval = 5 * time.Minute
const syncLeaseTTL = 30 * time.Second
const maxSyncDuration = 20 * time.Second

type remoteHTTPError struct {
	Status  int
	Message string
	RetryAt time.Time
}

func (e *remoteHTTPError) Error() string {
	return fmt.Sprintf("coordinator returned HTTP %d: %s", e.Status, e.Message)
}

func retryAfter(value string, now time.Time) time.Time {
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds > 0 {
		if seconds > 365*24*60*60 {
			seconds = 365 * 24 * 60 * 60
		}
		return now.Add(time.Duration(seconds) * time.Second)
	}
	if date, err := http.ParseTime(value); err == nil && date.After(now) {
		return date
	}
	return time.Time{}
}

func backoff(base time.Duration, rounds int, ceiling time.Duration) time.Duration {
	if base < minRemoteInterval {
		base = minRemoteInterval
	}
	for rounds > 0 && base < ceiling {
		if base > ceiling/2 {
			return ceiling
		}
		base *= 2
		rounds--
	}
	if base > ceiling {
		return ceiling
	}
	return base
}

// A single lease is shared by aliases of the same coordinator and by all
// processes using this database. Network work is never done in a transaction.
func claimSync(db *sql.DB, r Remote, force bool, now time.Time) (string, bool, error) {
	tx, err := beginWrite(db)
	if err != nil {
		return "", false, err
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT OR IGNORE INTO sync_schedule (remote_name) VALUES (?)", r.Name); err != nil {
		return "", false, err
	}
	var next, retry int64
	if err := tx.QueryRow("SELECT next_at, retry_at FROM sync_schedule WHERE remote_name = ?", r.Name).Scan(&next, &retry); err != nil {
		return "", false, err
	}
	if retry > now.UnixMilli() || (!force && next > now.UnixMilli()) {
		return "", false, nil
	}
	if _, err := tx.Exec("INSERT OR IGNORE INTO sync_peers (server_id) VALUES (?)", r.ServerID); err != nil {
		return "", false, err
	}
	owner := NewMessage("", "", nil).ID
	result, err := tx.Exec("UPDATE sync_peers SET owner = ?, lease_until = ? WHERE server_id = ? AND lease_until <= ?", owner, now.Add(syncLeaseTTL).UnixMilli(), r.ServerID, now.UnixMilli())
	if err != nil {
		return "", false, err
	}
	n, err := result.RowsAffected()
	if err != nil || n == 0 {
		return "", false, err
	}
	return owner, true, tx.Commit()
}

func finishSync(db *sql.DB, r Remote, owner string, base time.Duration, stats SyncStats, failure error) error {
	tx, err := beginWrite(db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var current string
	if err := tx.QueryRow("SELECT owner FROM sync_peers WHERE server_id = ?", r.ServerID).Scan(&current); err != nil {
		return err
	}
	if current != owner {
		return fmt.Errorf("sync lease lost")
	}
	var idle, failures int
	if err := tx.QueryRow("SELECT idle_rounds, failures FROM sync_schedule WHERE remote_name = ?", r.Name).Scan(&idle, &failures); err != nil {
		return err
	}
	now := time.Now()
	retry := int64(0)
	delay := base
	message := ""
	if failure != nil {
		message = failure.Error()
		if len(message) > 1000 {
			message = message[:1000]
		}
		failures++
		delay = backoff(time.Second, failures, maxFailureInterval)
		var remoteError *remoteHTTPError
		if errors.As(failure, &remoteError) {
			if remoteError.Status == http.StatusUnauthorized || remoteError.Status == http.StatusForbidden {
				if delay < 30*time.Second {
					delay = 30 * time.Second
				}
			}
			if remoteError.RetryAt.After(now) {
				retry = remoteError.RetryAt.UnixMilli()
			}
		}
	} else if stats.Pulled+stats.Pushed == 0 {
		idle++
		failures = 0
		ceiling := max(base, maxIdleInterval)
		delay = backoff(base, idle, ceiling)
	} else {
		idle, failures = 0, 0
	}
	// Positive jitter keeps the configured floor and server retry deadlines.
	delay = time.Duration(float64(delay) * (1 + rand.Float64()*0.2))
	if delay < minRemoteInterval {
		delay = minRemoteInterval
	}
	next := now.Add(delay).UnixMilli()
	if retry > next {
		next = retry
	}
	if failure != nil {
		retry = next
	}
	if _, err := tx.Exec("UPDATE sync_schedule SET next_at = ?, retry_at = ?, idle_rounds = ?, failures = ? WHERE remote_name = ?", next, retry, idle, failures, r.Name); err != nil {
		return err
	}
	if failure == nil {
		_, err = tx.Exec("UPDATE remotes SET last_sync = ?, last_error = '' WHERE name = ?", now.UnixMilli(), r.Name)
	} else {
		_, err = tx.Exec("UPDATE remotes SET last_error = ? WHERE name = ?", message, r.Name)
	}
	if err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE sync_peers SET owner = '', lease_until = 0 WHERE server_id = ? AND owner = ?", r.ServerID, owner); err != nil {
		return err
	}
	return tx.Commit()
}

// At most four HTTP requests/second per coordinator/database, including
// aliases, explicit syncs and offline-outbox catch-up. Cooldowns and leases
// survive process restarts. No sleeping holds the SQLite writer lock.
func remoteRequestSlot(ctx context.Context, r Remote) error {
	if r.DB == nil || r.ServerID == "" {
		return nil
	}
	for {
		tx, err := beginWrite(r.DB)
		if err != nil {
			return err
		}
		if _, err := tx.Exec("INSERT OR IGNORE INTO sync_peers (server_id) VALUES (?)", r.ServerID); err != nil {
			tx.Rollback()
			return err
		}
		var next int64
		if err := tx.QueryRow("SELECT next_request_at FROM sync_peers WHERE server_id = ?", r.ServerID).Scan(&next); err != nil {
			tx.Rollback()
			return err
		}
		now := time.Now()
		if next <= now.UnixMilli() {
			_, err = tx.Exec("UPDATE sync_peers SET next_request_at = ? WHERE server_id = ?", now.Add(minRemoteInterval).UnixMilli(), r.ServerID)
			if err != nil {
				tx.Rollback()
				return err
			}
			return tx.Commit()
		}
		tx.Rollback()
		timer := time.NewTimer(time.Until(time.UnixMilli(next)))
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}

func wakeRemote(db dbtx, name string) error {
	_, err := db.Exec(`INSERT INTO sync_schedule (remote_name, next_at) VALUES (?, 0)
		ON CONFLICT(remote_name) DO UPDATE SET next_at = 0, idle_rounds = 0 WHERE sync_schedule.retry_at <= ?`, name, time.Now().UnixMilli())
	return err
}

// SyncDue is the automatic path. Explicit SyncRemotes skips idle delays,
// but both paths honor error backoff, Retry-After and per-peer leases.
func SyncDue(ctx context.Context, db *sql.DB, name string, base time.Duration) (SyncStats, error) {
	return syncScheduled(ctx, db, name, base, false)
}
