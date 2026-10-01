# correlated-telemetrygen

Synthetic OTLP traces and logs in which **every log record belongs to a span of the same request**, so a
backend can jump from a log line to its trace and from a span to its logs. Built for the demo load in
[clickhouse-observability-stack](https://github.com/benhastings/clickhouse-observability-stack), where stock
[`telemetrygen`](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/cmd/telemetrygen)
can't do this: its `logs` command takes one fixed `--trace-id`, and its `traces` command picks random IDs.

## What it sends

Each interaction is one `POST /api/v1/checkout` on the `checkout` service, which calls `payments`:

```text
checkout  SERVER  POST /api/v1/checkout          log: checkout started
checkout  CLIENT    GET cart                     (redis, cache.hit)
checkout  CLIENT    SELECT shop.inventory ...    (postgresql, 2–5 queries)
checkout  CLIENT    POST  -> payments
payments  SERVER      POST /api/v1/charge        log: authorizing card
payments  CLIENT        POST  -> gateway         log: payment gateway timed out (on error)
                                                 log: charge completed / charge failed   (wide event)
                                                 log: checkout completed / accepted with payment pending (wide event)
```

- A fraction of charges (`--error-ratio`, default 25%) time out at the gateway after ~900 ms. The payments
  server span and the calls into and out of it are marked as errors. Checkout accepts the order with payment
  pending (HTTP 202), so its server span succeeds.
- Every log is emitted inside its span's context, so OTLP carries the span's `TraceId` and `SpanId`. They are
  also written as `trace_id` and `span_id` log attributes, for backends that link on attributes.
- Each service ends its request with one canonical **wide event** log (`main=true`) carrying the same
  attributes as its server span: `http.request.method`, `http.route`, `http.response.status_code`,
  `duration_ms`, `user.id`, `user.team.id`, `db.query_count`, `db.duration_ms`, `cache.hit`, `outcome` and,
  on failure, `error.type`.
- Every resource has `service.name`, `service.namespace`, `service.version`, `service.team`,
  `service.build.git_hash` and `deployment.environment.name`, plus anything in `OTEL_RESOURCE_ATTRIBUTES`
  (for example `k8s.pod.name` or `cloud.region`).

Timestamps are simulated from the start of each interaction, so the process never sleeps inside a request.
Span ends can be up to about a second in the future.

## Usage

```bash
docker run --rm -e OTEL_EXPORTER_OTLP_ENDPOINT=http://host.docker.internal:4317 \
  ghcr.io/brandonapol/correlated-telemetrygen:v0.1.0 --rate 2 --error-ratio 0.25
```

| Flag | Default | |
|---|---|---|
| `--rate` | `2` | checkout requests per second |
| `--error-ratio` | `0.25` | fraction of payments that time out |
| `--environment` | `local` | `deployment.environment.name` |
| `--namespace` | `shop` | `service.namespace` |
| `--duration` | `0` | how long to run; `0` runs until SIGTERM |
| `--seed` | `0` | random seed; `0` uses the clock |

The exporters read the standard `OTEL_EXPORTER_OTLP_*` variables. They send OTLP gRPC to `localhost:4317`
by default, and an `http://` endpoint means no TLS.

## Development

```bash
go test ./...      # asserts every log sits inside a span of the same trace and service
docker build .
```

Pushing a `v*` tag publishes a multi-arch (amd64, arm64) image to `ghcr.io/brandonapol/correlated-telemetrygen`.
