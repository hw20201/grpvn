package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/spf13/cobra"

	"github.com/frane/grpvn/internal"
)

var listenFlag, serveTokenFlag string
var serveChannelsFlag []string
var serveOpenFlag, servePrivateFlag bool
var serveOpenChannelsFlag, servePrivateChannelsFlag []string
var servePublicURLFlag string
var serveClaudeChannel bool
var serveMessageTriggers bool
var serveWeb bool
var serveSession, serveActivationMode string

var serveCmd = &cobra.Command{
	Use:     "serve",
	Aliases: []string{"sv"},
	Short:   "Run the MCP server, or an HTTP coordinator with --listen",
	Args:    cobra.NoArgs,
	PreRunE: func(cmd *cobra.Command, args []string) error {
		if serveClaudeChannel && listenFlag != "" {
			return fmt.Errorf("Claude Channels use stdio; omit --listen")
		}
		if serveActivationMode != "passive" && serveActivationMode != "notify" && serveActivationMode != "activate" {
			return fmt.Errorf("--activation must be passive, notify or activate")
		}
		if listenFlag == "" && (cmd.Flags().Changed("message-triggers") || cmd.Flags().Changed("web") || cmd.Flags().Changed("open") || cmd.Flags().Changed("private") || cmd.Flags().Changed("open-channel") || cmd.Flags().Changed("private-channel") || cmd.Flags().Changed("public-url") || cmd.Flags().Changed("channel") || cmd.Flags().Changed("token")) {
			return fmt.Errorf("HTTP coordinator options require --listen")
		}
		return nil
	},
	Run: func(cmd *cobra.Command, args []string) {
		if listenFlag != "" {
			if err := serveCoordinator(cmd.Context()); err != nil {
				fmt.Fprintln(os.Stderr, "error:", err)
				os.Exit(1)
			}
			return
		}
		err := internal.ServeMCP("grpvn", version, bootstrap, internal.MCPOptions{ClaudeChannel: serveClaudeChannel, RuntimeSession: serveSession, ActivationMode: serveActivationMode})
		// EOF / cancellation is the host closing the pipe on shutdown.
		if err != nil && !errors.Is(err, io.EOF) && !errors.Is(err, context.Canceled) {
			fmt.Fprintln(os.Stderr, "error:", err)
			os.Exit(1)
		}
	},
}

func serveCoordinator(ctx context.Context) error {
	token := serveTokenFlag
	if token == "" {
		token = os.Getenv("GRPVN_TOKEN")
	}
	if token != "" {
		return fmt.Errorf("shared server tokens are no longer supported; use grpvn invite and approve to grant membership")
	}
	_, _, err := net.SplitHostPort(listenFlag)
	if err != nil {
		return fmt.Errorf("--listen must be host:port: %w", err)
	}
	db, err := internal.OpenDB()
	if err != nil {
		return err
	}
	defer db.Close()
	options := internal.CoordinatorOptions{
		PublicURL: servePublicURLFlag,
		Web:       serveWeb,
		Channels:  serveChannelsFlag, Open: serveOpenFlag,
		OpenChannels: serveOpenChannelsFlag, PrivateChannels: servePrivateChannelsFlag,
	}
	handler, err := internal.CoordinatorHandler(db, options)
	if err != nil {
		return err
	}
	listener, err := net.Listen("tcp", listenFlag)
	if err != nil {
		return err
	}
	defer listener.Close()
	fmt.Fprintf(os.Stderr, "grpvn coordinator listening on %s\n", listener.Addr())
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()
	if serveMessageTriggers {
		triggerStop, e := internal.StartMessageTriggers(ctx, db, options, os.Stderr)
		if e != nil {
			return e
		}
		defer triggerStop()
	}
	srv := &http.Server{Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	done := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdown); err != nil {
				_ = srv.Close()
			}
		case <-done:
		}
	}()
	serveErr := srv.Serve(listener)
	close(done)
	<-shutdownDone
	if err := serveErr; err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}
	return nil
}

func init() {
	serveCmd.Flags().BoolVar(&serveMessageTriggers, "message-triggers", false, "dispatch opted-in webhook activations on committed messages")
	serveCmd.Flags().BoolVar(&serveWeb, "web", false, "serve the human web app alongside the coordinator (requires --listen)")
	serveCmd.Flags().BoolVar(&serveClaudeChannel, "claude-channel", false, "expose native Claude Channel notifications (requires host opt-in)")
	serveCmd.Flags().StringVar(&serveSession, "session", "", "stable native session ID for the Claude Channel bridge")
	serveCmd.Flags().StringVar(&serveActivationMode, "activation", "notify", "initial channel activation policy: passive, notify or activate")
	serveCmd.Flags().StringVarP(&listenFlag, "listen", "l", "", "HTTP coordinator address, e.g. 127.0.0.1:7331 (omit for stdio MCP)")
	serveCmd.Flags().StringVarP(&serveTokenFlag, "token", "k", "", "unsupported legacy shared token; use invite/approve")
	serveCmd.Flags().StringSliceVar(&serveChannelsFlag, "channel", nil, "serve only these channels (repeat or comma-separate; default all local channels)")
	serveCmd.Flags().BoolVarP(&serveOpenFlag, "open", "o", false, "public reads and automatic agent registration by default")
	serveCmd.Flags().BoolVarP(&servePrivateFlag, "private", "p", false, "require approved channel membership (the default)")
	serveCmd.Flags().StringSliceVar(&serveOpenChannelsFlag, "open-channel", nil, "public reads and automatic agent registration on these channels")
	serveCmd.Flags().StringSliceVar(&servePrivateChannelsFlag, "private-channel", nil, "require approved membership on these channels")
	serveCmd.Flags().StringVar(&servePublicURLFlag, "public-url", "", "canonical HTTPS base URL for ChatGPT/MCP OAuth discovery (loopback HTTP allowed)")
	serveCmd.MarkFlagsMutuallyExclusive("open", "private")
	rootCmd.AddCommand(serveCmd)
}
