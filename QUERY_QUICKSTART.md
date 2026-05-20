# Query Quickstart

Cómo correr tu primera query con `gnatrix-gosdk`. Copy-paste-friendly,
sin teoría. Para referencia densa por método, ver el [README](./README.md)
o `go doc github.com/Gnatrix-Inc/gosdk`.

## 30 segundos: minimal

```go
package main

import (
    "context"
    "errors"
    "fmt"
    "io"
    "log"

    gnatrix "github.com/Gnatrix-Inc/gosdk"
)

func main() {
    ctx := context.Background()

    client, err := gnatrix.Dial(ctx, gnatrix.Config{
        Addr:       "localhost:7777",
        Token:      "gnx_...",
        TenantSlug: "acme",
    })
    if err != nil {
        log.Fatal(err)
    }
    defer client.Close()

    stream, err := client.Query(ctx, "search error", gnatrix.QueryOptions{
        Limit: 100,
    })
    if err != nil {
        log.Fatal(err)
    }
    defer stream.Close() // ← obligatorio, ver "Reglas críticas" abajo

    for {
        row, err := stream.Next(ctx)
        if errors.Is(err, io.EOF) {
            break
        }
        if err != nil {
            log.Fatal(err)
        }
        fmt.Println(row)
    }

    res := stream.Result()
    fmt.Printf("%d rows, %d events scanned, %dms\n",
        res.RowsReturned, res.EventsScanned, res.ElapsedMs)
}
```

Eso es todo. Si tu server tiene data, las rows fluyen; sino verás un
error tipado (siguiente sección).

## Manejo de errores

`Next` retorna tres clases de error tipadas + dos sentinels:

```go
for {
    row, err := stream.Next(ctx)

    if errors.Is(err, io.EOF) {
        break // QUERY_END con status=0 — éxito limpio
    }

    var qerr *gnatrix.QueryError
    if errors.As(err, &qerr) {
        // QUERY_END status != 0 (post-execution)
        //   1 cancelled       2 engine_error      3 permission_denied
        //   4 quota           5 memory_limit      6 timeout
        //   7 storage_unavailable
        // EngineCode se parsea del prefix "NNNN: msg" cuando status=2
        log.Printf("query failed: status=%d code=%d %s",
            qerr.Status, qerr.EngineCode, qerr.Message)
        break // stream.Result() tiene totales válidos
    }

    var rej *gnatrix.QueryRejectError
    if errors.As(err, &rej) {
        // ERROR pre-execution (códigos 2011..2019):
        //   2011 syntax       2012 unsupported    2013 invalid_time_range
        //   2014 unknown_field 2015 mem_limit     2016 timeout
        //   2017 storage      2018 too_large      2019 too_many_in_flight
        // La sesión queda viva — podés disparar otra Query inmediatamente
        log.Printf("rejected: code=%d %s", rej.Code, rej.Message)
        break // stream.Result() es nil (no hubo QUERY_END)
    }

    if errors.Is(err, gnatrix.ErrStreamClosed) {
        break // alguien llamó stream.Close()
    }

    if err != nil {
        log.Fatal(err) // ctx canceled, session-fatal, etc.
    }

    process(row)
}
```

Errores que `Query` retorna directamente (síncronos, sin tocar el wire):

| Error | Causa |
|---|---|
| `gnatrix.ErrQueryInFlight` | Otra query está activa en este `*Client` |
| `gnatrix: query: <wrapped>` | Read loop ya murió (sesión cerrada) |
| `gnatrix: query write: <wrapped>` | El write del frame al socket falló |

## Reglas críticas

**1. Siempre `defer stream.Close()` después de un `Query()` exitoso.**

```go
stream, err := client.Query(ctx, "...", opts)
if err != nil { return err }
defer stream.Close() // ← AQUI, antes de cualquier return condicional
```

Si abandonás la iteración (break / return / panic) sin Close ni
drain-a-terminal, el slot one-in-flight queda colgado. Los siguientes
`Query()` sobre el mismo `*Client` retornan `ErrQueryInFlight` hasta
que llames `Client.Close()` y hagas un `Dial` nuevo.

**2. No mezcles `Next` desde múltiples goroutines en el mismo stream.**

Single-consumer. Si necesitás fan-out, corré un solo Next-consumer y
publicá las rows en un canal propio.

**3. `Result()` panica si lo llamás antes de que `Next` termine.**

Es programmer error. Llamá `Result()` solo después de un terminal
(io.EOF o cualquier `err != nil` de Next).

## TimeRange (ventana temporal)

```go
import "time"

stream, err := client.Query(ctx, "search error", gnatrix.QueryOptions{
    TimeRange: &gnatrix.TimeRange{
        EarliestNs: time.Now().Add(-1 * time.Hour).UnixNano(),
        LatestNs:   time.Now().UnixNano(),
    },
})
```

- Half-open `[EarliestNs, LatestNs)` en nanosegundos desde Unix epoch.
- Ambos campos o ninguno — el server rechaza `latest < earliest` con
  `*QueryRejectError{Code: 2013}`.
- El SDK **no resuelve tiempos relativos** ("-15m", "now") — usá
  `time.Now()` / `time.Add()` y `.UnixNano()` para construir los `int64`.
- Si omitís `TimeRange` (queda `nil`), el server aplica su default
  (15 min ending now, configurable por tenant).

## Paginación

```go
// Primera página
stream, _ := client.Query(ctx, "search error", gnatrix.QueryOptions{Limit: 1000})
// ... drain rows ...
res := stream.Result()

// Página siguiente, si hay
if res.NextCursor != "" {
    stream2, _ := client.Query(ctx, "search error", gnatrix.QueryOptions{
        Limit:  1000,
        Cursor: res.NextCursor,
    })
    // ... drain ...
}
```

`NextCursor` es opaco — pasalo verbatim. Cuando viene vacío, no hay
más rows.

## Colectar todo en memoria

El SDK es streaming-only — no expone `QueryAll`. Si necesitás todo en
una slice, hacelo vos:

```go
rows := []gnatrix.Row{}
for {
    row, err := stream.Next(ctx)
    if errors.Is(err, io.EOF) { break }
    if err != nil { return nil, err }
    rows = append(rows, row)
}
```

**Atención**: sin `Limit`, podés recibir millones de rows. Pasá `Limit`
en `QueryOptions` o asegurate de que el caller pueda soportar el
volumen — la SDK no impone caps por diseño.

## Concurrencia

- **Pings concurrentes con un query activo**: OK. El `*Client` serializa
  writes internamente.
- **Dos `Query()` simultáneos en el mismo `*Client`**: el segundo
  retorna `ErrQueryInFlight` sin tocar la red. Si querés N queries en
  paralelo, hacé N `Dial()` (un `*Client` por query concurrente).
- **`Next` desde múltiples goroutines en el mismo stream**: NO — ver
  "Reglas críticas" arriba.

## Más

- README del SDK: [README.md](./README.md) — referencia API completa.
- godoc: `go doc github.com/Gnatrix-Inc/gosdk` — todos los tipos y métodos.
- Example runnable: `example_test.go` en este mismo repo (`Example_query`).
- Endpoint REST para probar contra el server real:
  [gnatrix-tests/gosdk](../gnatrix-tests/gosdk) — sample API que expone
  `POST /api/v1/gnatrix/query`.
