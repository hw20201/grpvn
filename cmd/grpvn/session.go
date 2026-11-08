package main

import (
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/frane/grpvn/internal"
	"github.com/spf13/cobra"
)

var sessionConfig internal.AgentSession
var sessionOwn, sessionReplay, watchOnce bool
var sessionAckStatus, watchSession string
var sessionCmd = &cobra.Command{Use: "session", Aliases: []string{"ss"}, Short: "Register agent runtimes, choose delivery owners and inspect receipts"}
var sessionRegisterCmd = &cobra.Command{Use: "register", Aliases: []string{"reg"}, Short: "Bind this grpvn identity to a runtime session", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
	name, st, err := bootstrap()
	if err != nil {
		return err
	}
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	s := sessionConfig
	s.Agent = name
	s.Project = internal.ProjectRoot()
	if len(s.Channels) == 0 {
		s.StatePath, err = filepath.Abs(internal.ResolveStatePath(statePathFlag))
		if err != nil {
			return err
		}
		s.Channels = st.Follow
	}
	id, err := internal.RegisterSession(db, s, sessionOwn, sessionReplay)
	if err != nil {
		return err
	}
	fmt.Println(id)
	return nil
}}
var sessionListCmd = &cobra.Command{Use: "list", Aliases: []string{"ls"}, Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	sessions, err := internal.Sessions(db, false)
	if err != nil {
		return err
	}
	owners, err := internal.Sessions(db, true)
	if err != nil {
		return err
	}
	owned := map[string]bool{}
	for _, s := range owners {
		owned[s.ID] = true
	}
	for _, s := range sessions {
		fmt.Printf("%s %s %s session=%s mode=%s status=%s owner=%t\n", s.ID, s.Agent, s.Runtime, s.RuntimeSession, s.Mode, s.Status, owned[s.ID])
	}
	return nil
}}
var sessionOwnerCmd = &cobra.Command{Use: "own <registry-id>", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := db.Exec("INSERT INTO activation_owners(agent_name,session_id) SELECT agent_name,id FROM agent_sessions WHERE id=? ON CONFLICT(agent_name) DO UPDATE SET session_id=excluded.session_id", args[0])
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("unknown registry ID")
	}
	fmt.Println("delivery owner selected")
	return nil
}}
var sessionModeCmd = &cobra.Command{Use: "mode <registry-id> <passive|notify|activate>", Short: "Change automatic activation policy", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
	if args[1] != "passive" && args[1] != "notify" && args[1] != "activate" {
		return fmt.Errorf("mode must be passive, notify or activate")
	}
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	result, err := db.Exec("UPDATE agent_sessions SET mode=? WHERE id=?", args[1], args[0])
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("unknown registry ID")
	}
	fmt.Println(args[1])
	return nil
}}
var sessionReceiptsCmd = &cobra.Command{Use: "receipts", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	rows, err := db.Query("SELECT e.id,e.session_id,e.message_id,e.action,e.status,COALESCE(e.consumed_at,0),e.detail FROM activation_events e ORDER BY e.created_at DESC LIMIT 100")
	if err != nil {
		return err
	}
	defer rows.Close()
	for rows.Next() {
		var id, session, message, action, status, detail string
		var consumed int64
		if err := rows.Scan(&id, &session, &message, &action, &status, &consumed, &detail); err != nil {
			return err
		}
		fmt.Printf("%s session=%s message=%s action=%s status=%s consumed=%t %s\n", id, session, message, action, status, consumed > 0, detail)
	}
	return rows.Err()
}}
var sessionStatusCmd = &cobra.Command{Use: "status <registry-id> <idle|busy|offline>", Short: "Report native runtime lifecycle status", Args: cobra.ExactArgs(2), RunE: func(cmd *cobra.Command, args []string) error {
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	return internal.TouchSession(db, args[0], args[1])
}}
var sessionAckCmd = &cobra.Command{Use: "ack <delivery-id>", Short: "Acknowledge a native delivery without marking chat read", Args: cobra.ExactArgs(1), RunE: func(cmd *cobra.Command, args []string) error {
	if sessionAckStatus != "accepted" && sessionAckStatus != "failed" && sessionAckStatus != "uncertain" {
		return fmt.Errorf("ack status must be accepted, failed or uncertain")
	}
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	return internal.FinishActivation(db, args[0], sessionAckStatus, "native runtime bridge acknowledgement", "", "")
}}
var watchCmd = &cobra.Command{Use: "watch", Aliases: []string{"wa"}, Short: "Watch committed messages and deliver configured runtime activations", Args: cobra.NoArgs, RunE: func(cmd *cobra.Command, args []string) error {
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	ctx, stop := signal.NotifyContext(cmd.Context(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if watchSession != "" {
		return internal.RunSessionBridge(ctx, db, watchSession, true, func(a internal.Activation) error { return json.NewEncoder(os.Stdout).Encode(a) })
	}
	if !watchOnce {
		internal.RunActivationWatcher(ctx, db, os.Stderr)
		return nil
	}
	ticker := time.NewTicker(250 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := internal.ProcessActivations(ctx, db, os.Stderr); err != nil {
			return err
		}
		if watchOnce {
			return nil
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}}

func init() {
	f := sessionRegisterCmd.Flags()
	f.StringVar(&sessionConfig.Runtime, "runtime", "", "codex, claude-channel, opencode, pi, grok, workspace-agent, openai or webhook")
	f.StringVar(&sessionConfig.RuntimeSession, "thread", "", "native session UUID, API worker name, published agtch_ trigger ID, or webhook agent principal ID")
	f.StringVar(&sessionConfig.Mode, "mode", "notify", "passive, notify or activate")
	f.StringSliceVar(&sessionConfig.Channels, "channel", nil, "channel subscriptions (default current follows)")
	f.StringVar(&sessionConfig.Endpoint, "endpoint", "", "existing runtime server or API base URL")
	f.StringVar(&sessionConfig.CredentialEnv, "credential-env", "", "environment variable holding provider credentials")
	f.StringVar(&sessionConfig.Model, "model", "", "explicit model for an OpenAI API agent")
	f.BoolVar(&sessionConfig.Supervise, "supervise", false, "permit proactive API execution or native session resume")
	f.BoolVar(&sessionConfig.ActivateMentions, "activate-mentions", false, "activate on mentions and replies as well as DMs/addressed questions")
	f.StringSliceVar(&sessionConfig.AllowSenders, "allow-sender", nil, "remote sender identities permitted to activate inference")
	f.BoolVar(&sessionOwn, "owner", false, "make this session the delivery owner for the identity")
	f.BoolVar(&sessionReplay, "replay", false, "scan existing history as well as future messages")
	f.BoolVar(&sessionConfig.KeepPolicy, "keep-policy", false, "native re-registration preserves the human's existing activation policy")
	sessionAckCmd.Flags().StringVar(&sessionAckStatus, "status", "accepted", "accepted, failed or uncertain")
	watchCmd.Flags().StringVar(&watchSession, "session", "", "emit JSON deliveries for this native bridge session; acknowledge with ss ack")
	watchCmd.Flags().BoolVar(&watchOnce, "once", false, "scan/dispatch once")
	watchCmd.MarkFlagsMutuallyExclusive("session", "once")
	sessionCmd.AddCommand(sessionRegisterCmd, sessionListCmd, sessionOwnerCmd, sessionModeCmd, sessionReceiptsCmd, sessionStatusCmd, sessionAckCmd)
	sessionCmd.RunE = sessionListCmd.RunE
	rootCmd.AddCommand(sessionCmd, watchCmd)
}
