package gnatrix_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	gnatrix "github.com/Gnatrix-Inc/gosdk"
)

// Example_query shows the canonical end-to-end query flow:
//
//  1. Dial a gnatrixquery server (handshake + session).
//  2. Defer Close on the *Client so the connection is torn down on exit.
//  3. Issue a query with QueryOptions (index, limit, optional time range).
//  4. Defer Close on the *QueryStream so the in-flight slot is freed
//     even if the loop exits early via `break` or `return`.
//  5. Iterate Next() until any non-nil error. io.EOF is the clean
//     terminal; *QueryError and *QueryRejectError are typed failures
//     recoverable via errors.As.
//  6. Read totals via Result() after drain. Valid for io.EOF and
//     *QueryError; nil for *QueryRejectError (no QUERY_END was emitted).
//
// The example is package-level (Example_query rather than
// ExampleClient_Query) because it spans the full lifecycle from Dial
// to teardown, not just a single method.
func Example_query() {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// 1. Dial. CACertPath is optional; the SDK falls back to system
	//    roots when omitted. Token, TenantSlug, and Addr are required.
	client, err := gnatrix.Dial(ctx, gnatrix.Config{
		Addr:       "gnatrixquery.example.com:7777",
		Token:      "gnx_...",
		TenantSlug: "acme",
	})
	if err != nil {
		// Dial-time auth failures (server ERROR codes 2001..2010)
		// surface as *gnatrix.AuthError.
		var authErr *gnatrix.AuthError
		if errors.As(err, &authErr) {
			log.Fatalf("auth failed: code=%d %s", authErr.Code, authErr.Message)
		}
		log.Fatal(err)
	}
	defer client.Close()

	// 2. Issue the query. TimeRange is optional; nil means the
	//    server's default window. ProgressIntervalMs=0 means the
	//    server default (250 ms — progress frames are decoded but
	//    not surfaced to the caller in Slice 1).
	stream, err := client.Query(ctx, "search error | top 10 host", gnatrix.QueryOptions{
		IndexName: "logs-2026",
		Limit:     500,
		TimeRange: &gnatrix.TimeRange{
			EarliestNs: time.Now().Add(-1 * time.Hour).UnixNano(),
			LatestNs:   time.Now().UnixNano(),
		},
	})
	if err != nil {
		// Returned synchronously by Query: ErrQueryInFlight (another
		// query active on this *Client) or a wrapped session/write
		// error. Server-side rejections do NOT surface here — they
		// arrive via Next.
		log.Fatalf("query: %v", err)
	}
	// Always pair with `defer stream.Close()` — see the QueryStream
	// lifecycle contract. If the loop below exits early (break,
	// return, panic), Close releases the one-in-flight slot via a
	// background drainer; otherwise it is a no-op.
	defer stream.Close()

	// 3. Drain rows. Each Next blocks until a row arrives or the
	//    stream reaches a terminal condition. The terminal types are
	//    documented on QueryStream.Next.
	var processed int
	for {
		row, err := stream.Next(ctx)
		if errors.Is(err, io.EOF) {
			break // clean QUERY_END status=0
		}
		if err != nil {
			// Typed failures, recoverable via errors.As:
			var rej *gnatrix.QueryRejectError
			if errors.As(err, &rej) {
				log.Printf("server rejected query: code=%d %s", rej.Code, rej.Message)
				// Session is still alive — caller may issue another Query.
				return
			}
			var qerr *gnatrix.QueryError
			if errors.As(err, &qerr) {
				// Status != 0: 1=cancelled, 2=engine_error,
				// 3=permission_denied, 4=quota, 5=memory_limit,
				// 6=timeout, 7=storage_unavailable.
				log.Printf("query failed: status=%d code=%d %s",
					qerr.Status, qerr.EngineCode, qerr.Message)
				// Result() still has the partial totals — useful for
				// observability even on failure.
				break
			}
			// ctx cancellation, ErrStreamClosed, or session-fatal.
			log.Fatalf("stream error: %v", err)
		}
		processed++
		_ = row["_time"] // use the row fields
	}

	// 4. Read totals from the terminating QUERY_END. Valid after any
	//    drain to a terminal (io.EOF or *QueryError). Returns nil
	//    after a *QueryRejectError.
	if res := stream.Result(); res != nil {
		fmt.Printf("query %d: %d rows returned, %d events scanned, %dms elapsed\n",
			res.QueryID, res.RowsReturned, res.EventsScanned, res.ElapsedMs)
		if res.NextCursor != "" {
			// More rows are available — issue another Query with
			// QueryOptions{Cursor: res.NextCursor} to resume.
			fmt.Printf("more rows available; resume with cursor=%q\n", res.NextCursor)
		}
	}
}
