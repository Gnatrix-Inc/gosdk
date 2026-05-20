// Command example dials a gnatrixquery server with the gnatrix Go SDK,
// prints the session minted by WELCOME, sends one PING, and exits.
//
// Usage:
//
//	./example -addr host:port -token gnx_... -tenant my-tenant
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/gnatrix/gnatrix-gosdk"
)

func main() {
	addr := flag.String("addr", "", "gnatrixquery server address as host:port (required)")
	token := flag.String("token", "", "raw gnx_... API token (required)")
	tenant := flag.String("tenant", "", "tenant slug (required)")
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout for dial + handshake + ping")
	flag.Parse()

	if *addr == "" || *token == "" || *tenant == "" {
		fmt.Fprintln(os.Stderr, "missing required flag: -addr, -token, and -tenant are all mandatory")
		flag.Usage()
		os.Exit(2)
	}

	ctx, cancel := context.WithTimeout(context.Background(), *timeout)
	defer cancel()

	client, err := gnatrix.Dial(ctx, gnatrix.Config{
		Addr:       *addr,
		Token:      *token,
		TenantSlug: *tenant,
	})
	if err != nil {
		var authErr *gnatrix.AuthError
		if errors.As(err, &authErr) {
			log.Fatalf("auth failed (code %d): %s", authErr.Code, authErr.Message)
		}
		log.Fatalf("dial: %v", err)
	}
	defer client.Close()

	sess := client.Session()
	fmt.Printf("connected to %s\n", *addr)
	fmt.Printf("  session_id:  %d\n", sess.SessionID)
	fmt.Printf("  user_id:     %s\n", formatUUID(sess.UserID))
	fmt.Printf("  tenant_id:   %s\n", formatUUID(sess.TenantID))
	fmt.Printf("  permissions: %v\n", sess.Permissions)
	fmt.Printf("  expires_at:  %s\n", sess.ExpiresAt.Format(time.RFC3339))

	rtt, err := client.Ping(ctx)
	if err != nil {
		log.Fatalf("ping: %v", err)
	}
	fmt.Printf("ping rtt: %v\n", rtt)
}

func formatUUID(b [16]byte) string {
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}
