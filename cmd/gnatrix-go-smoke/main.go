// Command gnatrix-go-smoke is a non-interactive smoke check for CI.
// It dials a gnatrixquery server, sends three PINGs, closes the session,
// and exits 0 on success or non-zero on any failure.
//
// Usage:
//
//	gnatrix-go-smoke -addr host:port -token gnx_... -tenant my-tenant
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
	timeout := flag.Duration("timeout", 10*time.Second, "overall timeout for dial + handshake + 3 pings")
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

	for i := 1; i <= 3; i++ {
		rtt, err := client.Ping(ctx)
		if err != nil {
			log.Fatalf("ping %d/3: %v", i, err)
		}
		fmt.Printf("ping %d/3: %v\n", i, rtt)
	}

	fmt.Println("OK")
}
