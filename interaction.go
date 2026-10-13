package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/trace"
)

// instance is one replica of a simulated service: the tracer and logger share a resource, so
// its spans and logs carry the same service.name, service.instance.id, k8s.pod.name and so on.
type instance struct {
	tracer trace.Tracer
	logger otellog.Logger
}

// simulator emits one checkout interaction at a time. Time is simulated: spans and logs get
// explicit timestamps laid out from the start of the interaction, so nothing sleeps.
type simulator struct {
	checkout   []instance
	payments   []instance
	errorRatio float64
	rng        *rand.Rand
}

var tenants = []string{"enterprise_corp_a", "enterprise_corp_b", "smb_north", "smb_south", "free_tier"}

// clock hands out monotonically increasing timestamps within one interaction.
type clock struct{ now time.Time }

func (c *clock) advance(d time.Duration) time.Time {
	c.now = c.now.Add(d)
	return c.now
}

// pick chooses the replica that serves a request, as a load balancer would.
func (s *simulator) pick(fleet []instance) instance {
	return fleet[s.rng.IntN(len(fleet))]
}

func (s *simulator) between(lo, hi time.Duration) time.Duration {
	return lo + time.Duration(s.rng.Int64N(int64(hi-lo)+1))
}

// run simulates POST /api/v1/checkout, which reads the cart from cache, runs a few database
// queries and calls payments. A payment gateway timeout fails the payments request, and
// checkout accepts the order with payment pending, so its own request still succeeds.
func (s *simulator) run(ctx context.Context, start time.Time) {
	c := &clock{now: start}
	checkout := s.pick(s.checkout)
	userID := fmt.Sprintf("usr_%05d", s.rng.IntN(500))
	tenant := tenants[s.rng.IntN(len(tenants))]

	ctx, root := checkout.tracer.Start(ctx, "POST /api/v1/checkout",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start),
		trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("http.route", "/api/v1/checkout"),
			attribute.String("user.id", userID),
			attribute.String("user.team.id", tenant),
		))
	items := 1 + s.rng.IntN(5)
	s.log(ctx, checkout, c.advance(time.Millisecond), otellog.SeverityInfo, "checkout started",
		attribute.String("user.id", userID), attribute.Int("cart.items", items))

	cacheHit := s.rng.Float64() < 0.8
	_, cache := checkout.tracer.Start(ctx, "GET cart", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("db.system.name", "redis"),
			attribute.String("db.operation.name", "GET"),
			attribute.Bool("cache.hit", cacheHit),
		))
	cache.End(trace.WithTimestamp(c.advance(s.between(time.Millisecond, 3*time.Millisecond))))

	queries := 2 + s.rng.IntN(3)
	if !cacheHit {
		queries++
	}
	var dbTime time.Duration
	for i := range queries {
		op, table := "SELECT", "shop.inventory"
		if i == queries-1 {
			op, table = "INSERT", "shop.orders"
		}
		d := s.between(2*time.Millisecond, 12*time.Millisecond)
		dbTime += d
		_, q := checkout.tracer.Start(ctx, op+" "+table, trace.WithSpanKind(trace.SpanKindClient),
			trace.WithTimestamp(c.now), trace.WithAttributes(
				attribute.String("db.system.name", "postgresql"),
				attribute.String("db.namespace", "shop"),
				attribute.String("db.operation.name", op),
				attribute.String("db.collection.name", table),
			))
		q.End(trace.WithTimestamp(c.advance(d)))
	}

	paid, errorType := s.charge(ctx, checkout, c, userID, tenant, items)

	outcome, status, severity, body := "success", 200, otellog.SeverityInfo, "checkout completed"
	if !paid {
		outcome, status, severity, body = "payment_pending", 202, otellog.SeverityWarn, "checkout accepted with payment pending"
	}
	end := c.advance(s.between(time.Millisecond, 4*time.Millisecond))
	wide := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/checkout"),
		attribute.Int("http.response.status_code", status),
		attribute.Float64("duration_ms", float64(end.Sub(start).Microseconds())/1000),
		attribute.String("user.id", userID),
		attribute.String("user.team.id", tenant),
		attribute.Int("db.query_count", queries),
		attribute.Float64("db.duration_ms", float64(dbTime.Microseconds())/1000),
		attribute.Bool("cache.hit", cacheHit),
		attribute.String("outcome", outcome),
	}
	if errorType != "" {
		wide = append(wide, attribute.String("error.type", errorType))
	}
	root.SetAttributes(wide...)
	root.SetStatus(codes.Ok, "")
	s.log(ctx, checkout, end, severity, body, append(wide, attribute.Bool("main", true))...)
	root.End(trace.WithTimestamp(end))
}

// charge is checkout's call to payments: a client span in checkout and, under it, the
// payments server span with its own gateway call and logs. It reports whether the charge
// succeeded and, if not, the error type.
func (s *simulator) charge(ctx context.Context, checkout instance, c *clock, userID, tenant string, items int) (bool, string) {
	fail := s.rng.Float64() < s.errorRatio
	payments := s.pick(s.payments)
	ctx, call := checkout.tracer.Start(ctx, "POST", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", "payments"),
			attribute.Int("server.port", 8080),
			attribute.String("url.full", "http://payments:8080/api/v1/charge"),
		))
	start := c.advance(time.Millisecond)

	pctx, srv := payments.tracer.Start(ctx, "POST /api/v1/charge",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start),
		trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("http.route", "/api/v1/charge"),
			attribute.String("user.id", userID),
			attribute.String("user.team.id", tenant),
		))
	amount := float64(items) * (5 + float64(s.rng.IntN(9500))/100)
	s.log(pctx, payments, c.advance(time.Millisecond), otellog.SeverityInfo, "authorizing card",
		attribute.Float64("payment.amount", amount), attribute.String("payment.currency", "USD"))

	_, gw := payments.tracer.Start(pctx, "POST", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", "gateway.example.com"),
			attribute.Int("server.port", 443),
		))
	status, outcome, errorType := 200, "success", ""
	if fail {
		gwEnd := c.advance(s.between(800*time.Millisecond, 950*time.Millisecond))
		gw.SetAttributes(attribute.String("error.type", "GatewayTimeout"))
		gw.SetStatus(codes.Error, "gateway timeout")
		gw.End(trace.WithTimestamp(gwEnd))
		status, outcome, errorType = 504, "upstream_error", "GatewayTimeout"
		s.log(pctx, payments, c.advance(time.Millisecond), otellog.SeverityError, "payment gateway timed out",
			attribute.String("error.type", errorType), attribute.Float64("payment.amount", amount))
	} else {
		gw.End(trace.WithTimestamp(c.advance(s.between(30*time.Millisecond, 110*time.Millisecond))))
	}

	end := c.advance(s.between(time.Millisecond, 3*time.Millisecond))
	wide := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/charge"),
		attribute.Int("http.response.status_code", status),
		attribute.Float64("duration_ms", float64(end.Sub(start).Microseconds())/1000),
		attribute.String("user.id", userID),
		attribute.String("user.team.id", tenant),
		attribute.Int("db.query_count", 0),
		attribute.Bool("cache.hit", false),
		attribute.String("outcome", outcome),
	}
	severity, body := otellog.SeverityInfo, "charge completed"
	if fail {
		wide = append(wide, attribute.String("error.type", errorType))
		srv.SetStatus(codes.Error, "gateway timeout")
		severity, body = otellog.SeverityError, "charge failed"
	}
	srv.SetAttributes(wide...)
	s.log(pctx, payments, end, severity, body, append(wide, attribute.Bool("main", true))...)
	srv.End(trace.WithTimestamp(end))

	callEnd := c.advance(time.Millisecond)
	call.SetAttributes(attribute.Int("http.response.status_code", status))
	if fail {
		call.SetAttributes(attribute.String("error.type", errorType))
		call.SetStatus(codes.Error, "payments returned 504")
	}
	call.End(trace.WithTimestamp(callEnd))
	return !fail, errorType
}

// log emits a record inside the span in ctx. The SDK copies the span's trace and span IDs onto
// the record; they are repeated as trace_id and span_id attributes so a log backend can link
// to the trace from the attributes alone.
func (s *simulator) log(ctx context.Context, svc instance, ts time.Time, sev otellog.Severity, body string, attrs ...attribute.KeyValue) {
	sc := trace.SpanContextFromContext(ctx)
	var r otellog.Record
	r.SetTimestamp(ts)
	r.SetObservedTimestamp(ts)
	r.SetSeverity(sev)
	r.SetSeverityText(severityText(sev))
	r.SetBody(attribute.StringValue(body))
	r.AddAttributes(attrs...)
	r.AddAttributes(
		attribute.String("trace_id", sc.TraceID().String()),
		attribute.String("span_id", sc.SpanID().String()),
	)
	svc.logger.Emit(ctx, r)
}

func severityText(sev otellog.Severity) string {
	switch {
	case sev >= otellog.SeverityError:
		return "ERROR"
	case sev >= otellog.SeverityWarn:
		return "WARN"
	default:
		return "INFO"
	}
}
