package internal

// PRAGMAs here are also set in OpenDB's DSN — they're restated on the
// migration connection because modernc.org/sqlite doesn't always have the
// DSN pragmas in effect before the first non-PRAGMA statement of the
// connection runs, and without them the very first migration on a shared DB
// can race a sibling process to SQLITE_BUSY.
//
// Schema v2: messages carry an explicit `seq INTEGER PRIMARY KEY
// AUTOINCREMENT`. seq is assigned under SQLite's single-writer lock at
// commit time, so commit order IS seq order — unlike ULIDs, which are
// minted before the insert commits and can therefore appear "in the past"
// relative to a cursor that already advanced. Cursors live in the database
// (per agent, per target) instead of state.json, so reading is one store's
// problem rather than a load-modify-save race across files. AUTOINCREMENT
// (plus the explicit INTEGER PRIMARY KEY) also pins seq across VACUUM and
// guarantees it never regresses, which `gc` relies on.
const Schema = `
PRAGMA journal_mode = WAL;
PRAGMA synchronous = NORMAL;
PRAGMA foreign_keys = ON;
PRAGMA busy_timeout = 5000;
CREATE TABLE IF NOT EXISTS messages (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    id           TEXT NOT NULL UNIQUE,
    sender       TEXT NOT NULL,
    target       TEXT NOT NULL,
    body         BLOB NOT NULL,
    chain_root   TEXT NOT NULL,
    chain_depth  INTEGER NOT NULL DEFAULT 0,
    parent_id    TEXT,
    correlation  TEXT,
    created_at   INTEGER NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_messages_target      ON messages(target);
CREATE INDEX IF NOT EXISTS idx_messages_chain_root  ON messages(chain_root);
CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation);
CREATE INDEX IF NOT EXISTS idx_messages_parent      ON messages(parent_id);

CREATE TABLE IF NOT EXISTS marks (
    agent_name   TEXT NOT NULL,
    message_id   TEXT NOT NULL,
    marked_at    INTEGER NOT NULL,
    PRIMARY KEY (agent_name, message_id),
    FOREIGN KEY (message_id) REFERENCES messages(id)
);

CREATE TABLE IF NOT EXISTS cursors (
    agent_name   TEXT NOT NULL,
    target       TEXT NOT NULL,
    position     INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (agent_name, target)
);

CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER PRIMARY KEY,
    applied_at INTEGER NOT NULL
);

-- Retry key for Send: a second post with the same (agent, key) returns the
-- original message instead of inserting a duplicate. Added in place; CREATE
-- TABLE IF NOT EXISTS is applied on every open so existing v2 stores pick
-- it up without a rebuild.
CREATE TABLE IF NOT EXISTS idempotency (
    agent_name  TEXT NOT NULL,
    key         TEXT NOT NULL,
    message_id  TEXT NOT NULL,
    created_at  INTEGER NOT NULL,
    PRIMARY KEY (agent_name, key),
    FOREIGN KEY (message_id) REFERENCES messages(id)
);

-- Remote configuration, channel replication positions, and the durable
-- outbox live alongside the log. An absent remote_seq means not yet sent.
CREATE TABLE IF NOT EXISTS remotes (
    name         TEXT PRIMARY KEY,
    url          TEXT NOT NULL,
    token        TEXT NOT NULL DEFAULT '',
    server_id    TEXT NOT NULL,
    last_sync    INTEGER NOT NULL DEFAULT 0,
    last_error   TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS remote_channels (
    remote_name  TEXT NOT NULL REFERENCES remotes(name),
    channel      TEXT NOT NULL,
    target       TEXT NOT NULL UNIQUE,
    position     INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (remote_name, channel)
);
CREATE TABLE IF NOT EXISTS remote_messages (
    remote_name  TEXT NOT NULL REFERENCES remotes(name),
    remote_id    TEXT NOT NULL,
    local_id     TEXT NOT NULL UNIQUE,
    remote_seq   INTEGER,
    PRIMARY KEY (remote_name, remote_id)
);
CREATE TABLE IF NOT EXISTS server_meta (
    key          TEXT PRIMARY KEY,
    value        TEXT NOT NULL
);

-- One renewable worker lease per database, shared by every local identity.
-- Stop requests are owner-scoped; no PID signalling or PID reuse risk.
CREATE TABLE IF NOT EXISTS daemon_state (
    singleton    INTEGER PRIMARY KEY CHECK (singleton = 1),
    owner        TEXT NOT NULL,
    pid          INTEGER NOT NULL,
    started_at   INTEGER NOT NULL,
    heartbeat_at INTEGER NOT NULL,
    interval_ms  INTEGER NOT NULL,
    stop_requested INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sync_schedule (
    remote_name TEXT PRIMARY KEY REFERENCES remotes(name),
    next_at INTEGER NOT NULL DEFAULT 0,
    retry_at INTEGER NOT NULL DEFAULT 0,
    idle_rounds INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS sync_peers (
    server_id TEXT PRIMARY KEY,
    owner TEXT NOT NULL DEFAULT '',
    lease_until INTEGER NOT NULL DEFAULT 0,
    next_request_at INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS principals (
    id TEXT PRIMARY KEY,
    credential_hash TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    parent_id TEXT,
    status TEXT NOT NULL,
    approval_code TEXT NOT NULL UNIQUE,
    requested TEXT NOT NULL DEFAULT '[]',
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS permissions (
    principal_id TEXT NOT NULL REFERENCES principals(id),
    channel TEXT NOT NULL,
    can_read INTEGER NOT NULL DEFAULT 1,
    can_write INTEGER NOT NULL DEFAULT 0,
    PRIMARY KEY (principal_id, channel)
);
-- Names may change; old addresses remain bound to the same identity.
CREATE TABLE IF NOT EXISTS principal_aliases (
    address TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id)
);
CREATE INDEX IF NOT EXISTS idx_principal_aliases_owner ON principal_aliases(principal_id);
CREATE TABLE IF NOT EXISTS workspace_channels (
    channel TEXT PRIMARY KEY,
    is_open INTEGER NOT NULL DEFAULT 0,
    owner_id TEXT NOT NULL REFERENCES principals(id),
    creator_id TEXT NOT NULL REFERENCES principals(id),
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS invites (
    id TEXT PRIMARY KEY,
    secret_hash TEXT NOT NULL UNIQUE,
    person TEXT NOT NULL,
    channels TEXT NOT NULL,
    can_write INTEGER NOT NULL,
    expires_at INTEGER NOT NULL,
    uses_left INTEGER NOT NULL,
    revoked INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS invite_issuers (
    invite_id TEXT PRIMARY KEY REFERENCES invites(id),
    principal_id TEXT NOT NULL REFERENCES principals(id)
);
CREATE TABLE IF NOT EXISTS local_credentials (
    server_id TEXT NOT NULL,
    agent_name TEXT NOT NULL,
    credential TEXT NOT NULL,
    principal_id TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'new',
    approval_code TEXT NOT NULL DEFAULT '',
    PRIMARY KEY (server_id, agent_name)
);
CREATE TABLE IF NOT EXISTS message_principals (
    message_id TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id)
);
CREATE TABLE IF NOT EXISTS agent_sessions (
    id TEXT PRIMARY KEY,
    agent_name TEXT NOT NULL,
    runtime TEXT NOT NULL,
    runtime_session TEXT NOT NULL,
    project TEXT NOT NULL DEFAULT '',
    state_path TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'idle',
    mode TEXT NOT NULL DEFAULT 'notify',
    channels TEXT NOT NULL DEFAULT '[]',
    endpoint TEXT NOT NULL DEFAULT '',
    credential_env TEXT NOT NULL DEFAULT '',
    model TEXT NOT NULL DEFAULT '',
    supervise INTEGER NOT NULL DEFAULT 0,
    activate_mentions INTEGER NOT NULL DEFAULT 0,
    allow_senders TEXT NOT NULL DEFAULT '[]',
    scan_position INTEGER NOT NULL DEFAULT 0,
    heartbeat_at INTEGER NOT NULL,
    UNIQUE (runtime,runtime_session)
);
CREATE TABLE IF NOT EXISTS activation_owners (
    agent_name TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES agent_sessions(id)
);
CREATE TABLE IF NOT EXISTS activation_events (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    message_id TEXT NOT NULL,
    message_seq INTEGER NOT NULL,
    target TEXT NOT NULL,
    thread_root TEXT NOT NULL,
    action TEXT NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at INTEGER NOT NULL,
    accepted_at INTEGER,
    consumed_at INTEGER,
    detail TEXT NOT NULL DEFAULT '',
    UNIQUE (session_id,message_id)
);
CREATE TABLE IF NOT EXISTS activation_runs (
    id TEXT PRIMARY KEY,
    session_id TEXT NOT NULL REFERENCES agent_sessions(id),
    thread_root TEXT NOT NULL,
    status TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    provider_id TEXT NOT NULL DEFAULT '',
    conversation_url TEXT NOT NULL DEFAULT '',
    detail TEXT NOT NULL DEFAULT ''
);
CREATE TABLE IF NOT EXISTS activation_run_events (
    run_id TEXT NOT NULL REFERENCES activation_runs(id),
    event_id TEXT NOT NULL UNIQUE REFERENCES activation_events(id),
    PRIMARY KEY (run_id,event_id)
);
CREATE TABLE IF NOT EXISTS agent_conversations (
    session_id TEXT NOT NULL,
    thread_root TEXT NOT NULL,
    response_id TEXT NOT NULL DEFAULT '',
    PRIMARY KEY(session_id,thread_root)
);
CREATE TABLE IF NOT EXISTS automated_messages (
    message_id TEXT PRIMARY KEY,
    activation_id TEXT NOT NULL,
    hops INTEGER NOT NULL DEFAULT 1
);
CREATE TABLE IF NOT EXISTS principal_approvals (
    principal_id TEXT PRIMARY KEY, approved INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS remote_attributions (
    local_id TEXT PRIMARY KEY, principal_id TEXT NOT NULL, owner_id TEXT NOT NULL, approved INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS oauth_clients (
    id TEXT PRIMARY KEY, name TEXT NOT NULL, redirects TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_requests (
    id TEXT PRIMARY KEY, client_id TEXT NOT NULL, principal_id TEXT NOT NULL,
    ticket_hash TEXT NOT NULL, code_hash TEXT NOT NULL UNIQUE, redirect_uri TEXT NOT NULL,
    state TEXT NOT NULL, challenge TEXT NOT NULL, scope TEXT NOT NULL, resource TEXT NOT NULL,
    expires_at INTEGER NOT NULL, consumed INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS oauth_refresh (
    secret_hash TEXT PRIMARY KEY, principal_id TEXT NOT NULL, client_id TEXT NOT NULL,
    resource TEXT NOT NULL, scope TEXT NOT NULL, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_access (
    credential_hash TEXT PRIMARY KEY, scope TEXT NOT NULL, resource TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS oauth_credentials (
    credential_hash TEXT PRIMARY KEY,
    principal_id TEXT NOT NULL REFERENCES principals(id)
);
CREATE TABLE IF NOT EXISTS oauth_profiles (
    browser_hash TEXT NOT NULL,
    app_key TEXT NOT NULL,
    principal_id TEXT NOT NULL REFERENCES principals(id),
    PRIMARY KEY(browser_hash,app_key)
);
CREATE TABLE IF NOT EXISTS oauth_request_approvals (
    request_id TEXT PRIMARY KEY REFERENCES oauth_requests(id),
    approval_code TEXT NOT NULL UNIQUE,
    approved INTEGER NOT NULL DEFAULT 0
);
-- Keep already-issued connections working when a profile reconnects.
INSERT OR IGNORE INTO oauth_credentials(credential_hash,principal_id)
 SELECT p.credential_hash,p.id FROM principals p JOIN oauth_access a ON a.credential_hash=p.credential_hash;
CREATE TABLE IF NOT EXISTS credential_expiry (
    credential_hash TEXT PRIMARY KEY, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS provider_poll (
    run_id TEXT PRIMARY KEY, next_at INTEGER NOT NULL DEFAULT 0, failures INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS workspace_signins (
 state_hash TEXT PRIMARY KEY, local_principal_id TEXT NOT NULL REFERENCES principals(id),
 base_url TEXT NOT NULL, server_id TEXT NOT NULL, alias TEXT NOT NULL, client_id TEXT NOT NULL,
 callback TEXT NOT NULL, verifier TEXT NOT NULL, status TEXT NOT NULL, detail TEXT NOT NULL DEFAULT '', expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS channel_archives (
 channel TEXT PRIMARY KEY, principal_id TEXT NOT NULL, created_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS local_participants (
 name TEXT PRIMARY KEY, enabled INTEGER NOT NULL DEFAULT 1, restricted INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS local_channel_grants (
 name TEXT NOT NULL, channel TEXT NOT NULL, can_write INTEGER NOT NULL DEFAULT 1,
 PRIMARY KEY(name,channel)
);
CREATE TABLE IF NOT EXISTS channel_subscriptions (
 principal_id TEXT NOT NULL, channel TEXT NOT NULL, joined INTEGER NOT NULL,
 PRIMARY KEY(principal_id,channel)
);
CREATE TABLE IF NOT EXISTS conversation_preferences (
 principal_id TEXT NOT NULL, target TEXT NOT NULL, muted INTEGER NOT NULL DEFAULT 0,
 held_position INTEGER, mute_dirty INTEGER NOT NULL DEFAULT 0,
 PRIMARY KEY(principal_id,target)
);
CREATE TABLE IF NOT EXISTS chat_files (
 id TEXT NOT NULL, target TEXT NOT NULL, uploader TEXT NOT NULL, name TEXT NOT NULL,
 media_type TEXT NOT NULL, data BLOB NOT NULL, created_at INTEGER NOT NULL,
 PRIMARY KEY(id,target)
);
CREATE TABLE IF NOT EXISTS remote_file_queue (
 remote_name TEXT NOT NULL, file_id TEXT NOT NULL, target TEXT NOT NULL,
 PRIMARY KEY(remote_name,file_id,target)
);
CREATE TABLE IF NOT EXISTS chat_metadata (key TEXT PRIMARY KEY,value INTEGER NOT NULL);
CREATE TABLE IF NOT EXISTS remote_channel_details (
 remote_name TEXT NOT NULL, channel TEXT NOT NULL, can_read INTEGER NOT NULL,
 can_write INTEGER NOT NULL, is_open INTEGER NOT NULL, can_manage INTEGER NOT NULL,
 PRIMARY KEY(remote_name,channel)
);
CREATE TABLE IF NOT EXISTS remote_human_sessions (
 server_id TEXT PRIMARY KEY, local_principal_id TEXT NOT NULL REFERENCES principals(id),
 principal_id TEXT NOT NULL, name TEXT NOT NULL, client_id TEXT NOT NULL, refresh_token TEXT NOT NULL, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS web_accounts (
 principal_id TEXT PRIMARY KEY REFERENCES principals(id), login TEXT NOT NULL UNIQUE,
 salt TEXT NOT NULL, password_hash TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS web_sessions (
 secret_hash TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES principals(id), expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS web_tickets (
 secret_hash TEXT PRIMARY KEY, principal_id TEXT NOT NULL REFERENCES principals(id), expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS bridge_leases (
    session_id TEXT PRIMARY KEY REFERENCES agent_sessions(id),
    owner TEXT NOT NULL, expires_at INTEGER NOT NULL
);
CREATE TABLE IF NOT EXISTS webhook_cooldowns (
    session_id TEXT PRIMARY KEY REFERENCES agent_sessions(id),
    next_at INTEGER NOT NULL DEFAULT 0,
    failures INTEGER NOT NULL DEFAULT 0
);
`

// migrateV1toV2 rebuilds the messages table to add the seq column and is
// executed inside a transaction by Migrate. ORDER BY id preserves the v1
// notion of message order (ULIDs) as the initial seq assignment, so
// migrated history reads back in the same sequence it always did.
const migrateV1toV2 = `
CREATE TABLE messages_v2 (
    seq          INTEGER PRIMARY KEY AUTOINCREMENT,
    id           TEXT NOT NULL UNIQUE,
    sender       TEXT NOT NULL,
    target       TEXT NOT NULL,
    body         BLOB NOT NULL,
    chain_root   TEXT NOT NULL,
    chain_depth  INTEGER NOT NULL DEFAULT 0,
    parent_id    TEXT,
    correlation  TEXT,
    created_at   INTEGER NOT NULL
);
INSERT INTO messages_v2 (id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at)
    SELECT id, sender, target, body, chain_root, chain_depth, parent_id, correlation, created_at
    FROM messages ORDER BY id ASC;
DROP TABLE messages;
ALTER TABLE messages_v2 RENAME TO messages;
CREATE INDEX IF NOT EXISTS idx_messages_target      ON messages(target);
CREATE INDEX IF NOT EXISTS idx_messages_chain_root  ON messages(chain_root);
CREATE INDEX IF NOT EXISTS idx_messages_correlation ON messages(correlation);
CREATE INDEX IF NOT EXISTS idx_messages_parent      ON messages(parent_id);
DROP INDEX IF EXISTS idx_messages_target_id;
CREATE TABLE IF NOT EXISTS cursors (
    agent_name   TEXT NOT NULL,
    target       TEXT NOT NULL,
    position     INTEGER NOT NULL,
    updated_at   INTEGER NOT NULL,
    PRIMARY KEY (agent_name, target)
);`
