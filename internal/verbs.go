package internal

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"regexp"
	"strings"
	"time"
)

// targetsFor returns the channels this agent reads from (followed channels
// plus its own DM inbox), in the order the caller passed them.
func targetsFor(name string, follow []string) []string {
	out := append([]string{}, follow...)
	out = append(out, "@"+name)
	return out
}

// Check counts unread per followed target. Cursors are per-target seq
// positions in the cursors table, so a follow added today surfaces every
// message in that channel (absent cursor reads as 0 = everything unread).
func Check(w io.Writer, db *sql.DB, st *State) (int, error) {
	return checkUnread(w, db, st, false)
}

// CheckActionable counts only unread that needs this agent: DMs, messages
// that mention its name, and replies to its posts. Channel chatter on a
// followed target is omitted — that's visible via Check, not a to-do.
func CheckActionable(w io.Writer, db *sql.DB, st *State) (int, error) {
	return checkUnread(w, db, st, true)
}

func checkUnread(w io.Writer, db *sql.DB, st *State, actionableOnly bool) (int, error) {
	cursors, err := loadCursors(db, st.Name)
	if err != nil {
		return 0, err
	}
	counts := []string{}
	total := 0
	inbox := "@" + st.Name
	for _, target := range targetsFor(st.Name, st.Follow) {
		n, err := countUnread(db, st.Name, target, cursors[target], actionableOnly && target != inbox)
		if err != nil {
			return 0, err
		}
		if n == 0 {
			continue
		}
		label := target
		if target == inbox {
			label = "@me"
		}
		counts = append(counts, fmt.Sprintf("%d %s", n, label))
		total += n
	}
	if total == 0 {
		return 2, nil
	}
	fmt.Fprintln(w, strings.Join(counts, " "))
	return 0, nil
}

func countUnread(db *sql.DB, name, target string, cursor int64, actionableOnly bool) (int, error) {
	var n int
	var err error
	if !actionableOnly {
		err = db.QueryRow(
			"SELECT COUNT(*) FROM messages WHERE target = ? AND seq > ? AND sender != ?",
			target, cursor, name,
		).Scan(&n)
	} else {
		err = db.QueryRow(
			`SELECT COUNT(*) FROM messages m
			 WHERE m.target = ? AND m.seq > ? AND m.sender != ?
			   AND (instr(m.body, ?) > 0
			     OR EXISTS (SELECT 1 FROM messages p WHERE p.id = m.parent_id AND p.sender = ?))`,
			target, cursor, name, name, name,
		).Scan(&n)
	}
	return n, err
}

// scanMessage reads one row into a Message. withSeq matches queries that
// select seq as the leading column.
func scanMessage(rows *sql.Rows, withSeq bool) (*Message, error) {
	var m Message
	var pID, corr sql.NullString
	var err error
	if withSeq {
		err = rows.Scan(&m.Seq, &m.ID, &m.Sender, &m.Target, &m.Body, &m.ChainRoot, &m.ChainDepth, &pID, &corr, &m.CreatedAt)
	} else {
		err = rows.Scan(&m.ID, &m.Sender, &m.Target, &m.Body, &m.ChainRoot, &m.ChainDepth, &pID, &corr, &m.CreatedAt)
	}
	if err != nil {
		return nil, err
	}
	if pID.Valid {
		m.ParentID = &pID.String
	}
	if corr.Valid {
		m.Correlation = &corr.String
	}
	return &m, nil
}

// AutoFollow subscribes the sender to a channel it just posted into.
// Posting says "I care about this conversation"; without the follow, the
// replies land where the sender never looks — the field failure was four
// posts into a channel and three answers the sender reported as "no
// reply". DMs and already-followed channels are no-ops. The send has
// already committed when this runs, so save failures are returned for the
// caller to warn about, never to fail the verb.
func AutoFollow(db *sql.DB, st *State, statePath, target string) (bool, error) {
	if !strings.HasPrefix(target, "#") {
		return false, nil
	}
	for _, f := range st.Follow {
		if f == target {
			return false, nil
		}
	}
	st.Follow = append(st.Follow, target)
	if err := st.Save(statePath); err != nil {
		return false, err
	}
	// Start reading the channel from now — the sender wants the replies to
	// its post, not the channel's history dumped into unread. Best-effort:
	// a failed fast-forward means a noisy first read, not a broken follow.
	_ = fastForwardTarget(db, st.Name, target)
	return true, nil
}

// resolveReadTargets maps optional r/p arguments onto the agent's inbox.
// Empty only means every followed channel plus @me. "@me" is the DM inbox.
// A target this agent does not follow is an error — unread is not defined
// for a channel with no cursor of ours, and guessing would dump history.
func resolveReadTargets(st *State, only []string) ([]string, error) {
	all := targetsFor(st.Name, st.Follow)
	if len(only) == 0 {
		return all, nil
	}
	allowed := make(map[string]bool, len(all))
	for _, t := range all {
		allowed[t] = true
	}
	out := make([]string, 0, len(only))
	seen := map[string]bool{}
	for _, raw := range only {
		if raw == "" {
			continue
		}
		t := raw
		if t == "@me" {
			t = "@" + st.Name
		}
		if !strings.HasPrefix(t, "#") && !strings.HasPrefix(t, "@") {
			return nil, fmt.Errorf("invalid read target %q: pass a #channel or @me", raw)
		}
		if !allowed[t] {
			return nil, fmt.Errorf("not following %s", raw)
		}
		if seen[t] {
			continue
		}
		seen[t] = true
		out = append(out, t)
	}
	if len(out) == 0 {
		return all, nil
	}
	return out, nil
}

// Read prints unread across followed targets in commit (seq) order.
// only, if non-empty, restricts the read to those targets (`#dev`, `@me`);
// empty means every followed channel plus the DM inbox. Cursors are
// per-target rows in the cursors table: after a successful render each
// target's cursor advances to the last seq rendered FOR THAT TARGET,
// guarded to be monotonic. Delivery is at-least-once: two reads by the
// same agent racing each other may both print a message, but the
// commit-ordered seq guarantees neither can skip one.
func Read(w io.Writer, db *sql.DB, st *State, limit int, advance bool, ts bool, full bool, human bool, color string, only ...string) (int, error) {
	cursors, err := loadCursors(db, st.Name)
	if err != nil {
		return 0, err
	}
	targets, err := resolveReadTargets(st, only)
	if err != nil {
		return 0, err
	}

	var where strings.Builder
	args := []interface{}{}
	for i, t := range targets {
		if i > 0 {
			where.WriteString(" OR ")
		}
		where.WriteString("(target = ? AND seq > ?)")
		args = append(args, t, cursors[t])
	}
	q := "SELECT seq, id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE (" + where.String() + ") AND sender != ? ORDER BY seq ASC"
	args = append(args, st.Name)
	if limit > 0 {
		q += " LIMIT ?"
		args = append(args, limit)
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		return 0, err
	}
	defer rows.Close()

	newCursors := map[string]int64{}
	var msgs []*Message
	for rows.Next() {
		m, err := scanMessage(rows, true)
		if err != nil {
			return 0, err
		}
		newCursors[m.Target] = m.Seq
		msgs = append(msgs, m)
	}
	if err := rows.Err(); err != nil {
		return 0, err
	}
	if len(msgs) == 0 {
		return 2, nil
	}
	RenderBatch(w, db, msgs, st.Name, st.DefaultChannel, ts, full, human, color)
	if advance {
		for target, pos := range newCursors {
			if err := advanceCursor(db, st.Name, target, pos); err != nil {
				return 0, err
			}
		}
	}
	return 0, nil
}

// MaxBodyBytes caps a single message body. Readers pay for every byte in
// tokens on every read, so a runaway body is everyone's problem; 64 KiB is
// generous for coordination traffic and small enough to keep `r` sane.
const MaxBodyBytes = 64 * 1024

// Send resolves the target, threads under a parent when the target is a
// ULID prefix, and saves. The returned message carries the assigned ID
// (the correlation ID when isAsk). The body arrives literal — stdin
// expansion of "-" happens in the CLI layer only: when this runs inside
// `grpvn serve`, stdin is the MCP JSON-RPC transport and reading it here
// would hang the server.
func Send(db *sql.DB, sender string, targetArg string, bodyArg string, defaultChannel string, isAsk bool) (*Message, error) {
	m, _, err := SendIdempotent(db, sender, targetArg, bodyArg, defaultChannel, isAsk, "")
	return m, err
}

// MaxIdempotencyKeyBytes caps the retry key. Keys exist to make a lost
// transport response safe to retry, not to store payloads.
const MaxIdempotencyKeyBytes = 256

// SendIdempotent is Send plus an optional retry key. A second call from the
// same sender with the same non-empty key returns the original message and
// does not insert another row. Empty key is a plain send. The lookup and
// insert share a transaction so two retries racing each other cannot both
// commit.
func SendIdempotent(db *sql.DB, sender string, targetArg string, bodyArg string, defaultChannel string, isAsk bool, key string) (*Message, bool, error) {
	if len(key) > MaxIdempotencyKeyBytes {
		return nil, false, fmt.Errorf("idempotency key too long: %d bytes (max %d)", len(key), MaxIdempotencyKeyBytes)
	}
	tx, err := beginWrite(db)
	if err != nil {
		return nil, false, err
	}
	defer tx.Rollback()
	var existing string
	err = sql.ErrNoRows
	if key != "" {
		err = tx.QueryRow("SELECT message_id FROM idempotency WHERE agent_name = ? AND key = ?", sender, key).Scan(&existing)
	}
	switch {
	case err == nil:
		if err := tx.Commit(); err != nil {
			return nil, false, err
		}
		m, err := findMessageByID(db, existing)
		return m, true, err
	case err != sql.ErrNoRows:
		return nil, false, err
	}
	m, err := sendOnce(tx, sender, targetArg, bodyArg, defaultChannel, isAsk)
	if err != nil {
		return nil, false, err
	}
	if key != "" {
		if _, err := tx.Exec(
			"INSERT INTO idempotency (agent_name, key, message_id, created_at) VALUES (?, ?, ?, ?)",
			sender, key, m.ID, time.Now().UnixMilli(),
		); err != nil {
			return nil, false, err
		}
	}
	if strings.HasPrefix(key, "activation:") {
		if _, err := tx.Exec("INSERT OR IGNORE INTO automated_messages(message_id,activation_id,hops) VALUES(?,?,1)", m.ID, strings.TrimPrefix(key, "activation:")); err != nil {
			return nil, false, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, false, err
	}
	signalCommittedMessage(db)
	return m, false, nil
}

func sendOnce(db dbtx, sender string, targetArg string, bodyArg string, defaultChannel string, isAsk bool) (*Message, error) {
	if len(bodyArg) > MaxBodyBytes {
		return nil, fmt.Errorf("body too large: %d bytes (max %d)", len(bodyArg), MaxBodyBytes)
	}
	target, parent, err := ResolveTarget(db, targetArg, defaultChannel)
	if err != nil {
		return nil, err
	}
	if channelArchived(db, target) {
		return nil, fmt.Errorf("channel is archived; reopen it before posting")
	}
	if !localSenderAllowed(db, sender, target) {
		return nil, fmt.Errorf("this local agent is removed or lacks channel write access")
	}
	if parent != nil && strings.HasPrefix(parent.Target, "@") {
		sameSender, err := samePrincipalAddress(db, parent.Sender, sender)
		if err != nil {
			return nil, err
		}
		if !sameSender {
			target = "@" + parent.Sender
		}
	}
	target, err = canonicalDMTarget(db, target)
	if err != nil {
		return nil, err
	}
	m := NewMessage(sender, target, []byte(bodyArg))
	if parent != nil {
		if parent.ChainDepth+1 > 8 {
			return nil, fmt.Errorf("chain depth limit reached (8)")
		}
		m.ChainRoot = parent.ChainRoot
		m.ChainDepth = parent.ChainDepth + 1
		m.ParentID = &parent.ID
	}
	if isAsk {
		m.Correlation = &m.ID
	}
	if err := m.save(db); err != nil {
		return nil, err
	}
	if err := markAutomaticReply(db, m); err != nil {
		return nil, err
	}
	if err := queueRemoteMessage(db, m); err != nil {
		return nil, err
	}
	return m, nil
}

// FormatSendAck is the stdout/MCP body for a successful send: the id a
// caller can retry against, and the target it landed on. "replayed" means
// the idempotency key hit an existing row — the store did not grow.
func FormatSendAck(m *Message, replayed bool) string {
	suffix := ""
	if m.Pending {
		suffix = " queued"
	}
	if replayed {
		return fmt.Sprintf("%s %s replayed%s", m.ID, m.Target, suffix)
	}
	return fmt.Sprintf("%s %s%s", m.ID, m.Target, suffix)
}

// Gc prunes messages (and their marks) older than the cutoff. Retention is
// an operator decision, so this is CLI-only — it is deliberately NOT
// exposed over MCP, keeping the agent-facing surface append-only. Cursor
// positions survive pruning: seq is AUTOINCREMENT and never reused, so a
// position simply has fewer rows behind it. Pruned thread parents render
// as unresolvable prefixes, which is the documented trade.
func Gc(w io.Writer, db *sql.DB, olderThan time.Duration, vacuum bool) error {
	if olderThan <= 0 {
		return fmt.Errorf("--older-than must be positive")
	}
	cutoff := time.Now().Add(-olderThan).UnixMilli()
	tx, err := beginWrite(db)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	eligible := `SELECT id FROM messages WHERE created_at < ? AND NOT EXISTS
		(SELECT 1 FROM remote_messages rm WHERE rm.local_id = messages.id AND rm.remote_seq IS NULL)`
	if _, err := tx.Exec(
		"DELETE FROM idempotency WHERE message_id IN ("+eligible+")", cutoff); err != nil {
		return fmt.Errorf("prune idempotency: %w", err)
	}
	resMarks, err := tx.Exec(
		"DELETE FROM marks WHERE message_id IN ("+eligible+")", cutoff)
	if err != nil {
		return fmt.Errorf("prune marks: %w", err)
	}
	resMsgs, err := tx.Exec("DELETE FROM messages WHERE id IN ("+eligible+")", cutoff)
	if err != nil {
		return fmt.Errorf("prune messages: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	nMsgs, _ := resMsgs.RowsAffected()
	nMarks, _ := resMarks.RowsAffected()
	fmt.Fprintf(w, "pruned %d messages, %d marks\n", nMsgs, nMarks)
	if vacuum {
		if _, err := db.Exec("VACUUM"); err != nil {
			return fmt.Errorf("vacuum: %w", err)
		}
	}
	return nil
}

func Grep(w io.Writer, db *sql.DB, name string, follow []string, pattern string, scope string, limit int, defaultChannel string, ts bool, full bool, human bool, color string) error {
	var v []string
	if scope != "" {
		// A search scope is a target, and every target starts with # or @.
		// Anything else can only ever match zero rows, which reads exactly
		// like "nothing found" — say so instead.
		if !strings.HasPrefix(scope, "#") && !strings.HasPrefix(scope, "@") {
			return fmt.Errorf("invalid search scope %q: pass a #channel or @user, or leave it empty to search followed channels and @me", scope)
		}
		v = []string{scope}
	} else {
		v = append([]string{}, follow...)
		v = append(v, "@"+name)
	}
	p := make([]string, len(v))
	args := make([]interface{}, len(v))
	for i, x := range v {
		p[i] = "?"
		args[i] = x
	}
	rows, err := db.Query("SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE target IN ("+strings.Join(p, ", ")+") ORDER BY id DESC", args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	re, err := regexp.Compile(pattern)
	if err != nil {
		return fmt.Errorf("invalid pattern: %w", err)
	}
	var msgs []*Message
	for rows.Next() {
		m, err := scanMessage(rows, false)
		if err != nil {
			return err
		}
		if !re.Match(m.Body) {
			continue
		}
		msgs = append(msgs, m)
		if limit > 0 && len(msgs) >= limit {
			break
		}
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// Breaking out at the limit leaves rows open, and open rows hold the
	// pool's only connection (OpenDB caps it at one). RenderBatch queries
	// the store for the ID prefix length, so it would wait for a second
	// connection that cannot exist — a hang, not an error. Release the
	// connection before rendering; the deferred Close covers the paths that
	// return early and is a no-op once this one has run.
	rows.Close()
	RenderBatch(w, db, msgs, name, defaultChannel, ts, full, human, color)
	return nil
}

func Log(w io.Writer, db *sql.DB, name string, arg string, limit int, defaultChannel string, ts bool, full bool, human bool, color string) error {
	var q string
	var args []interface{}
	if strings.HasPrefix(arg, "#") || strings.HasPrefix(arg, "@") {
		q = "SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE target = ? ORDER BY id ASC"
		args = []interface{}{arg}
	} else {
		parent, err := FindMessageByPrefix(db, arg)
		if err != nil {
			return err
		}
		q = "SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE chain_root = ? ORDER BY id ASC"
		args = []interface{}{parent.ChainRoot}
	}
	if limit > 0 {
		if strings.HasPrefix(arg, "#") || strings.HasPrefix(arg, "@") {
			q = "SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM (SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE target = ? ORDER BY id DESC LIMIT ?) ORDER BY id ASC"
			args = append(args, limit)
		} else {
			q = "SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM (SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages WHERE chain_root = ? ORDER BY id DESC LIMIT ?) ORDER BY id ASC"
			args = append(args, limit)
		}
	}
	rows, err := db.Query(q, args...)
	if err != nil {
		return err
	}
	defer rows.Close()
	var msgs []*Message
	for rows.Next() {
		m, err := scanMessage(rows, false)
		if err != nil {
			return err
		}
		msgs = append(msgs, m)
	}
	RenderBatch(w, db, msgs, name, defaultChannel, ts, full, human, color)
	return nil
}

// Channels lists every channel that exists in the store — not just the ones
// this agent follows — with its message count, how long ago it last saw
// traffic, and whether the agent is following it. Without this there is no
// supported answer to "what channels are there?", and the only way to find
// out is to open ~/.grpvn/grpvn.db behind the CLI's back.
func Channels(w io.Writer, db *sql.DB, follow []string) error {
	rows, err := db.Query("SELECT target, COUNT(*), MAX(created_at) FROM messages WHERE target LIKE '#%' GROUP BY target ORDER BY MAX(created_at) DESC")
	if err != nil {
		return err
	}
	defer rows.Close()
	followed := make(map[string]bool, len(follow))
	for _, f := range follow {
		followed[f] = true
	}
	seen := map[string]bool{}
	for rows.Next() {
		var target string
		var count int
		var last int64
		if err := rows.Scan(&target, &count, &last); err != nil {
			return err
		}
		seen[target] = true
		suffix := ""
		if followed[target] {
			suffix = " following"
		}
		fmt.Fprintf(w, "%s %d %s%s\n", target, count, RelativeTime(last), suffix)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	// A followed channel with no messages yet still exists as far as this
	// agent is concerned; leaving it out would make the list disagree with
	// `grpvn follow`.
	for _, f := range follow {
		if !seen[f] {
			fmt.Fprintf(w, "%s 0 - following\n", f)
		}
	}
	return nil
}

func Mark(w io.Writer, db *sql.DB, name string, msgArg string, delete bool, defaultChannel string, ts bool, full bool, human bool, color string) error {
	if msgArg == "" && !delete {
		rows, err := db.Query("SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at FROM messages JOIN marks ON messages.id = marks.message_id WHERE marks.agent_name = ? ORDER BY id ASC", name)
		if err != nil {
			return err
		}
		defer rows.Close()
		var msgs []*Message
		for rows.Next() {
			m, err := scanMessage(rows, false)
			if err != nil {
				return err
			}
			msgs = append(msgs, m)
		}
		RenderBatch(w, db, msgs, name, defaultChannel, ts, full, human, color)
		return nil
	}
	m, err := FindMessageByPrefix(db, msgArg)
	if err != nil {
		return err
	}
	if delete {
		_, err := db.Exec("DELETE FROM marks WHERE agent_name = ? AND message_id = ?", name, m.ID)
		return err
	}
	_, err = db.Exec("INSERT INTO marks (agent_name, message_id, marked_at) VALUES (?, ?, ?) ON CONFLICT(agent_name,message_id) DO UPDATE SET marked_at=excluded.marked_at", name, m.ID, time.Now().UnixMilli())
	return err
}

func ID(w io.Writer, name string) {
	cwd, _ := os.Getwd()
	fmt.Fprintf(w, "%s@%s\n", name, cwd)
}

func Init(path string, as string, force bool) (string, error) {
	if _, err := os.Stat(path); err == nil && !force {
		return "", fmt.Errorf("state file already exists")
	}
	name := as
	if name == "" {
		var err error
		name, err = GenerateIdentity()
		if err != nil {
			return "", err
		}
	}
	s := &State{Name: name, Follow: []string{}}
	if err := s.Save(path); err != nil {
		return "", err
	}
	return name, nil
}
