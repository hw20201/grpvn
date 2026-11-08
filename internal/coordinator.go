package internal

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/oklog/ulid/v2"
)

const syncProtocol = 1
const syncPageSize = 100

type serverInfo struct {
	Protocol int    `json:"protocol"`
	ServerID string `json:"server_id"`
	Web      bool   `json:"web,omitempty"`
}

// wireMessage uses the coordinator's IDs and commit sequence. Each client
// maps these IDs to its own log, independently of every other remote.
type wireMessage struct {
	Seq           int64   `json:"seq"`
	ID            string  `json:"id"`
	Sender        string  `json:"sender"`
	Target        string  `json:"target"`
	Body          string  `json:"body"`
	ChainRoot     string  `json:"chain_root"`
	ChainDepth    int     `json:"chain_depth"`
	ParentID      *string `json:"parent_id,omitempty"`
	Correlation   *string `json:"correlation,omitempty"`
	CreatedAt     int64   `json:"created_at"`
	PrincipalID   string  `json:"principal_id,omitempty"`
	OwnerID       string  `json:"owner_id,omitempty"`
	OwnerName     string  `json:"owner_name,omitempty"`
	OwnerApproved bool    `json:"owner_approved,omitempty"`
	Automated     bool    `json:"automated,omitempty"`
}

type messagePage struct {
	Messages []wireMessage `json:"messages"`
	Cursor   int64         `json:"cursor"`
}

func toWire(m *Message) wireMessage {
	return wireMessage{Seq: m.Seq, ID: m.ID, Sender: m.Sender, Target: m.Target, Body: string(m.Body), ChainRoot: m.ChainRoot, ChainDepth: m.ChainDepth, ParentID: m.ParentID, Correlation: m.Correlation, CreatedAt: m.CreatedAt}
}

func (m wireMessage) message() *Message {
	return &Message{Seq: m.Seq, ID: m.ID, Sender: m.Sender, Target: m.Target, Body: []byte(m.Body), ChainRoot: m.ChainRoot,
		ChainDepth: m.ChainDepth, ParentID: m.ParentID, Correlation: m.Correlation, CreatedAt: m.CreatedAt}
}

func validMessageID(id string) bool {
	_, err := ulid.ParseStrict(id)
	return err == nil && id == strings.ToUpper(id)
}

func validateWire(m wireMessage) error {
	if !validMessageID(m.ID) || !validMessageID(m.ChainRoot) || (m.ParentID != nil && !validMessageID(*m.ParentID)) {
		return fmt.Errorf("invalid message or thread ID")
	}
	if m.Sender == "" || len(m.Sender) > 200 || strings.ContainsAny(m.Sender, "\r\n\t\x00 ") {
		return fmt.Errorf("invalid sender")
	}
	if err := validateChannel(m.Target); err != nil {
		return err
	}
	if len(m.Body) > MaxBodyBytes || !utf8.ValidString(m.Body) {
		return fmt.Errorf("body must be UTF-8 and at most %d bytes", MaxBodyBytes)
	}
	if m.CreatedAt <= 0 || m.ChainDepth < 0 || m.ChainDepth > 8 {
		return fmt.Errorf("invalid creation time or chain depth")
	}
	if m.ParentID == nil {
		if m.ChainDepth != 0 || m.ChainRoot != m.ID {
			return fmt.Errorf("root message must reference itself at depth 0")
		}
	} else if m.ChainDepth == 0 || *m.ParentID == m.ID || m.ChainRoot == m.ID {
		return fmt.Errorf("invalid reply chain")
	}
	if m.Correlation != nil && *m.Correlation != m.ID {
		return fmt.Errorf("question correlation must be its own message ID")
	}
	return nil
}

// CoordinatorHandler exposes only native channel messages: DMs and caches
// of other coordinators are never re-exported. Private channels need the
// approved membership; open channels allow anonymous reads and automatic
// registration. Remote posts are bound to registered agent identities.
func CoordinatorHandler(db *sql.DB, options CoordinatorOptions) (http.Handler, error) {
	access, err := newChannelAccess(options)
	if err != nil {
		return nil, err
	}
	access.db = db
	id := NewMessage("", "", nil).ID
	if _, err := db.Exec("INSERT OR IGNORE INTO server_meta (key, value) VALUES ('id', ?)", id); err != nil {
		return nil, err
	}
	if err := db.QueryRow("SELECT value FROM server_meta WHERE key = 'id'").Scan(&id); err != nil {
		return nil, err
	}
	mux := http.NewServeMux()
	mountFiles(mux, db, access, options.PublicURL, options.LocalUI)
	mountAgentGuide(mux, options.PublicURL)
	if options.Web || options.LocalUI {
		mountWeb(mux, db, access, options.PublicURL, options.LocalUI)
	} else {
		mux.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
			http.Redirect(w, r, "/llm.txt", http.StatusTemporaryRedirect)
		})
	}
	if options.PublicURL != "" {
		if err := mountOAuth(mux, db, options.PublicURL, access); err != nil {
			return nil, err
		}
	} else {
		mountBrowserInvitations(mux, db, "")
	}
	mux.Handle("/mcp", remoteMCPHandler(db, access, options.PublicURL))
	mux.HandleFunc("POST /v1/register", enrollmentHandler(db, access, false))
	mux.HandleFunc("POST /v1/agents", enrollmentHandler(db, access, true))
	mux.HandleFunc("POST /v1/sync", batchHandler(db, access, id))
	serverMetadata := func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, serverInfo{Protocol: syncProtocol, ServerID: id, Web: options.Web})
	}
	mux.HandleFunc("GET /v1/server", serverMetadata)
	// Compatibility with coordinators and clients released before v0.12.
	mux.HandleFunc("GET /v1/info", serverMetadata)
	mux.HandleFunc("GET /v1/channels", func(w http.ResponseWriter, r *http.Request) {
		channels, err := channelCandidates(db, access)
		if err != nil {
			apiError(w, 500, "cannot list channels")
			return
		}
		out := []string{}
		for _, channel := range channels {
			if access.canAccess(r, channel) {
				out = append(out, channel)
			}
		}
		writeJSON(w, http.StatusOK, map[string]any{"channels": out})
	})
	mux.HandleFunc("GET /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		channel := r.URL.Query().Get("channel")
		if err := validateChannel(channel); err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
		after := int64(0)
		var err error
		if raw := r.URL.Query().Get("after"); raw != "" {
			after, err = strconv.ParseInt(raw, 10, 64)
		}
		if err != nil || after < 0 {
			apiError(w, http.StatusBadRequest, "after must be a non-negative sequence")
			return
		}
		if prospectiveChannel(db, access, requestPrincipal(r), channel) {
			writeJSON(w, http.StatusOK, messagePage{Messages: []wireMessage{}, Cursor: after})
			return
		}
		if !access.authorize(w, r, channel) {
			return
		}
		rows, err := db.QueryContext(r.Context(), `SELECT seq, id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at
			FROM messages m WHERE target = ? AND seq > ?
			AND NOT EXISTS (SELECT 1 FROM remote_messages rm WHERE rm.local_id = m.id) ORDER BY seq LIMIT ?`, channel, after, syncPageSize)
		if err != nil {
			apiError(w, http.StatusInternalServerError, "cannot read log")
			return
		}
		defer rows.Close()
		page := messagePage{Messages: []wireMessage{}, Cursor: after}
		for rows.Next() {
			m, err := scanMessage(rows, true)
			if err != nil {
				apiError(w, http.StatusInternalServerError, "cannot read message")
				return
			}
			page.Messages = append(page.Messages, toWire(m))
			page.Cursor = m.Seq
		}
		if err := rows.Err(); err != nil {
			apiError(w, http.StatusInternalServerError, "cannot read log")
			return
		}
		for i := range page.Messages {
			page.Messages[i] = attributedWire(db, page.Messages[i].message())
		}
		writeJSON(w, http.StatusOK, page)
	})
	mux.HandleFunc("POST /v1/messages", func(w http.ResponseWriter, r *http.Request) {
		r.Body = http.MaxBytesReader(w, r.Body, 6*MaxBodyBytes+4096) // JSON may escape each byte as \uXXXX.
		dec := json.NewDecoder(r.Body)
		dec.DisallowUnknownFields()
		var msg wireMessage
		if err := dec.Decode(&msg); err != nil {
			apiError(w, http.StatusBadRequest, "invalid message JSON or request too large")
			return
		}
		if err := dec.Decode(new(any)); err != io.EOF {
			apiError(w, http.StatusBadRequest, "expected one JSON message")
			return
		}
		if err := validateWire(msg); err != nil {
			apiError(w, http.StatusBadRequest, err.Error())
			return
		}
		principal := requestPrincipal(r)
		if principal == nil {
			apiError(w, http.StatusUnauthorized, "a registered agent identity is required to post")
			return
		}
		if !principalOwnsLocalName(principal, msg.Sender) {
			apiError(w, 403, "sender must match the registered agent identity")
			return
		}
		if !access.authorize(w, r, msg.Target) {
			return
		}
		m, err := acceptAttributedMessage(db, msg, principal)
		if err != nil {
			apiError(w, http.StatusConflict, err.Error())
			return
		}
		writeJSON(w, http.StatusOK, attributedWire(db, m))
	})
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if pinned := r.Header.Get("X-Grpvn-Server"); pinned != "" && pinned != id {
			apiError(w, 409, "coordinator identity changed")
			return
		}
		if humanAPIPath(r.URL.Path) || strings.HasPrefix(r.URL.Path, "/assets/") || r.URL.Path == "/favicon.svg" || publicGuidePath(r.URL.Path) || r.URL.Path == "/join" || r.URL.Path == "/v1/server" || r.URL.Path == "/v1/info" || r.URL.Path == "/v1/register" || r.URL.Path == "/mcp" || strings.HasPrefix(r.URL.Path, "/oauth/") || strings.HasPrefix(r.URL.Path, "/.well-known/") {
			mux.ServeHTTP(w, r)
			return
		}
		principal, err := authenticateMember(db, r)
		if err != nil {
			w.Header().Set("WWW-Authenticate", "Bearer")
			apiError(w, 401, err.Error())
			return
		}
		if principal != nil {
			r = r.WithContext(context.WithValue(r.Context(), principalContextKey{}, principal))
		}
		if !access.hasAnonymousAccess() && !access.authenticated(r) {
			authenticationRequired(w)
			return
		}
		mux.ServeHTTP(w, r)
	}), nil
}

func sameWire(a, b wireMessage) bool {
	// seq belongs to the server and is not part of the immutable payload.
	a.Seq, b.Seq = 0, 0
	a.PrincipalID, a.OwnerID, a.OwnerName = "", "", ""
	b.PrincipalID, b.OwnerID, b.OwnerName = "", "", ""
	a.OwnerApproved, b.OwnerApproved = false, false
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return string(aa) == string(bb)
}

func acceptRemoteMessage(db *sql.DB, wire wireMessage) (*Message, error) {
	return acceptAttributedMessage(db, wire, nil)
}

func acceptAttributedMessage(db *sql.DB, wire wireMessage, principal *Principal) (*Message, error) {
	if channelArchived(db, wire.Target) {
		return nil, fmt.Errorf("channel is archived")
	}
	tx, err := beginWrite(db)
	if err != nil {
		return nil, fmt.Errorf("cannot write log")
	}
	defer tx.Rollback()
	// Hosted channels cannot alias a remote namespace, including one not
	// joined yet. This also prevents accidentally bridging coordinators.
	if name, _, ok := splitRemoteTarget(wire.Target); ok {
		var remote bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM remotes WHERE name = ?)", name).Scan(&remote); err != nil {
			return nil, fmt.Errorf("cannot inspect channel")
		}
		if remote {
			return nil, fmt.Errorf("cannot publish into a remote namespace")
		}
	}
	existing, err := findMessageByID(tx, wire.ID)
	if err == nil {
		if principal != nil {
			var origin string
			if err := tx.QueryRow("SELECT principal_id FROM message_principals WHERE message_id = ?", wire.ID).Scan(&origin); err != nil || origin != principal.ID {
				return nil, fmt.Errorf("message belongs to a different registered sender")
			}
		}
		var cached bool
		if err := tx.QueryRow("SELECT EXISTS(SELECT 1 FROM remote_messages WHERE local_id = ?)", wire.ID).Scan(&cached); err != nil {
			return nil, fmt.Errorf("cannot inspect message")
		}
		previous := attributedWire(tx, existing)
		if principal != nil {
			// Registered ownership is stable across a rename; a lost upload
			// acknowledgement must still replay under its original sender name.
			previous.Sender = wire.Sender
		}
		if cached || !sameWire(previous, wire) {
			return nil, fmt.Errorf("message ID already belongs to another payload")
		}
		if err := tx.QueryRow("SELECT seq FROM messages WHERE id = ?", wire.ID).Scan(&existing.Seq); err != nil {
			return nil, fmt.Errorf("cannot read sequence")
		}
		return existing, tx.Commit()
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("cannot inspect message")
	}
	if wire.ParentID != nil {
		parent, err := findMessageByID(tx, *wire.ParentID)
		if err != nil {
			return nil, fmt.Errorf("reply parent is unavailable on coordinator")
		}
		if parent.Target != wire.Target || parent.ChainRoot != wire.ChainRoot || parent.ChainDepth+1 != wire.ChainDepth {
			return nil, fmt.Errorf("reply must keep its parent's channel, root, and depth")
		}
	}
	m := wire.message()
	if principal != nil {
		owner := principal
		if principal.ParentID != "" {
			parent, err := findPrincipal(tx, "id=?", principal.ParentID)
			if err != nil || parent.Status != "active" {
				return nil, fmt.Errorf("owner registration is inactive")
			}
			owner = parent
		}
		m.Sender = registeredSender(principal.Name, owner.Name, owner.ID)
		if principal.Kind == "human" && principal.ParentID == "" {
			m.Sender = principal.Name
		}
	}
	if err := m.save(tx); err != nil {
		return nil, fmt.Errorf("cannot save message")
	}
	if wire.Automated {
		if _, err := tx.Exec("INSERT INTO automated_messages(message_id,activation_id,hops) VALUES(?,?,1)", m.ID, "remote:"+m.ID); err != nil {
			return nil, err
		}
	}
	if principal != nil {
		if _, err := tx.Exec("INSERT INTO message_principals (message_id,principal_id) VALUES (?,?)", m.ID, principal.ID); err != nil {
			return nil, err
		}
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	signalCommittedMessage(db)
	return m, nil
}

func registeredSender(name, owner, id string) string {
	if len(id) > 8 {
		id = id[:8]
	}
	return name + "@" + strings.Join(strings.Fields(owner), "_") + "~" + id
}

func attributedWire(db dbtx, m *Message) wireMessage {
	w := toWire(m)
	_ = db.QueryRow("SELECT EXISTS(SELECT 1 FROM automated_messages WHERE message_id=?)", m.ID).Scan(&w.Automated)
	var name string
	err := db.QueryRow(`SELECT p.id,p.name,COALESCE(owner.id,p.id),COALESCE(owner.name,p.name)
		FROM message_principals mp JOIN principals p ON p.id=mp.principal_id LEFT JOIN principals owner ON owner.id=p.parent_id WHERE mp.message_id=?`, m.ID).Scan(&w.PrincipalID, &name, &w.OwnerID, &w.OwnerName)
	if err == nil {
		w.Sender = name
		_ = db.QueryRow("SELECT approved FROM principal_approvals WHERE principal_id=?", w.OwnerID).Scan(&w.OwnerApproved)
	}
	return w
}

func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(value)
}

func apiError(w http.ResponseWriter, code int, message string) {
	writeJSON(w, code, map[string]string{"error": message})
}
