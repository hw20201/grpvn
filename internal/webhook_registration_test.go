package internal

import (
	"database/sql"
	"testing"
)

func webhookRegistrationFixture(t *testing.T) (*sql.DB, AgentSession, string) {
	t.Helper()
	db := newTestDB(t)
	seedTestAgent(t, db, "webhook-registration-secret", "registered-agent", []string{"#subscribed"})
	var principalID string
	if err := db.QueryRow("SELECT id FROM principals WHERE name=?", "registered-agent").Scan(&principalID); err != nil {
		t.Fatal(err)
	}
	s := AgentSession{Agent: "local-as-name", Runtime: "webhook", RuntimeSession: principalID, Mode: "activate", Supervise: true, Channels: []string{"#subscribed"}, Endpoint: "https://hooks.example.test/notify", CredentialEnv: "WEBHOOK_TOKEN"}
	return db, s, principalID
}

func TestRegisterWebhookBindsExplicitPrincipalID(t *testing.T) {
	db, s, principalID := webhookRegistrationFixture(t)
	if _, err := RegisterSession(db, s, true, false); err != nil {
		t.Fatal(err)
	}
	var gotAgent, gotThread string
	if err := db.QueryRow("SELECT agent_name,runtime_session FROM agent_sessions WHERE runtime='webhook'").Scan(&gotAgent, &gotThread); err != nil {
		t.Fatal(err)
	}
	if gotThread != principalID {
		t.Fatalf("thread bound to %q, want requested principal %q", gotThread, principalID)
	}
	if gotAgent != "registered-agent" {
		t.Fatalf("agent_name = %q, want actual principal address", gotAgent)
	}
}

func TestRegisterWebhookDoesNotFallbackFromInvalidPrincipalIDToName(t *testing.T) {
	db, s, _ := webhookRegistrationFixture(t)
	s.RuntimeSession = "not-a-principal-id"
	s.Agent = "registered-agent"
	if _, err := RegisterSession(db, s, true, false); err == nil {
		t.Fatal("invalid --thread unexpectedly fell back to principal name")
	}
}

func TestRegisterWebhookRejectsInvalidEndpoint(t *testing.T) {
	db, s, _ := webhookRegistrationFixture(t)
	s.Endpoint = "http://example.test/hook"
	if _, err := RegisterSession(db, s, true, false); err == nil {
		t.Fatal("expected non-loopback HTTP endpoint rejection")
	}
}

func TestRegisterWebhookRequiresSuperviseAndValidCredentialEnv(t *testing.T) {
	db, s, _ := webhookRegistrationFixture(t)
	s.Supervise = false
	if _, err := RegisterSession(db, s, true, false); err == nil {
		t.Fatal("expected --supervise requirement")
	}
	s.Supervise = true
	s.CredentialEnv = "bad-name"
	if _, err := RegisterSession(db, s, true, false); err == nil {
		t.Fatal("expected invalid credential environment variable rejection")
	}
}

func TestRegisterWebhookRejectsRevokedParent(t *testing.T) {
	db, s, principalID := webhookRegistrationFixture(t)
	if _, err := db.Exec("INSERT INTO principals(id,credential_hash,name,kind,status,approval_code,created_at) VALUES('revoked-parent','revoked-hash','revoked-owner','human','revoked','revoked-code',1)"); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("UPDATE principals SET parent_id='revoked-parent' WHERE id=?", principalID); err != nil {
		t.Fatal(err)
	}
	if _, err := RegisterSession(db, s, true, false); err == nil {
		t.Fatal("expected revoked owner principal rejection")
	}
}
