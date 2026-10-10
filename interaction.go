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

// instance is one replica of a simulated service, or for the web front end one browser
// profile: the tracer and logger share a resource, so its spans and logs carry the same
// service.name, service.instance.id, k8s.pod.name and so on.
type instance struct {
	tracer trace.Tracer
	logger otellog.Logger
	podIP  string // empty for a browser
}

// simulator emits one checkout interaction at a time. Time is simulated: spans and logs get
// explicit timestamps laid out from the start of the interaction, so nothing sleeps.
type simulator struct {
	simConfig
	web      []instance // one per browser profile
	checkout []instance
	payments []instance
	rng      *rand.Rand
	sessions map[string]*session
}

type simConfig struct {
	namespace  string  // the Kubernetes namespace the services run in
	errorRatio float64 // fraction of charges that time out at the payment gateway
}

func newSimulator(web, checkout, payments []instance, cfg simConfig, rng *rand.Rand) *simulator {
	return &simulator{simConfig: cfg, web: web, checkout: checkout, payments: payments, rng: rng, sessions: map[string]*session{}}
}

var tenants = []string{"enterprise_corp_a", "enterprise_corp_b", "smb_north", "smb_south", "free_tier"}

const users = 500

// session is a shopper's browser session. The browser sends session.id to checkout, and
// checkout to payments, in W3C Baggage, so every wide event in a request carries it.
type session struct {
	id, previousID string
	browser        int    // index into browserProfiles and simulator.web
	ip             string // the shopper's public address
}

// request is one checkout by one shopper.
type request struct {
	userID, tenant string
	session        *session
	items          int
}

// newRequest picks a shopper. A user belongs to one tenant and keeps their browser across
// sessions; one checkout in twenty starts a new session.
func (s *simulator) newRequest() request {
	n := s.rng.IntN(users)
	userID := fmt.Sprintf("usr_%05d", n)
	sess, ok := s.sessions[userID]
	switch {
	case !ok:
		sess = &session{id: uuidV4(s.rng), browser: s.rng.IntN(len(s.web)), ip: publicIP(s.rng)}
		s.sessions[userID] = sess
	case s.rng.Float64() < 0.05:
		sess.previousID, sess.id = sess.id, uuidV4(s.rng)
	}
	return request{userID: userID, tenant: tenants[n%len(tenants)], session: sess, items: 1 + s.rng.IntN(5)}
}

// publicIP returns an address from the IPv4 ranges reserved for documentation (RFC 5737).
func publicIP(rng *rand.Rand) string {
	nets := []string{"192.0.2", "198.51.100", "203.0.113"}
	return fmt.Sprintf("%s.%d", nets[rng.IntN(len(nets))], 1+rng.IntN(254))
}

// loadBalancerIPs are the addresses of the load balancer nodes in front of checkout, one per zone.
var loadBalancerIPs = []string{"10.0.4.17", "10.0.37.201", "10.0.70.88"}

// sessionAttrs are the session attributes the browser records.
func (r request) sessionAttrs() []attribute.KeyValue {
	attrs := []attribute.KeyValue{attribute.String("session.id", r.session.id)}
	if r.session.previousID != "" {
		attrs = append(attrs, attribute.String("session.previous_id", r.session.previousID))
	}
	return attrs
}

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

func millis(d time.Duration) float64 {
	return float64(d.Microseconds()) / 1000
}

// checkoutResult is what checkout tells the browser.
type checkoutResult struct {
	status             int
	outcome, errorType string
}

// run simulates one checkout as the shopper sees it: clicking "place order" in the browser
// sends POST /api/v1/checkout to the checkout service, and the page shows the result.
func (s *simulator) run(ctx context.Context, start time.Time) {
	c := &clock{now: start}
	req := s.newRequest()
	web := s.web[req.session.browser]

	ctx, click := web.tracer.Start(ctx, "click", trace.WithSpanKind(trace.SpanKindInternal),
		trace.WithTimestamp(start), trace.WithAttributes(req.sessionAttrs()...))
	s.log(ctx, web, c.advance(time.Millisecond), otellog.SeverityInfo, "checkout submitted",
		append(req.sessionAttrs(), attribute.String("user.id", req.userID), attribute.Int("cart.items", req.items))...)

	fctx, fetch := web.tracer.Start(ctx, "POST", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(append(req.sessionAttrs(),
			attribute.String("http.request.method", "POST"),
			attribute.String("url.full", "https://shop.example.com/api/v1/checkout"),
			attribute.String("server.address", "shop.example.com"),
			attribute.Int("server.port", 443),
		)...))
	latency := s.between(10*time.Millisecond, 60*time.Millisecond)
	c.advance(latency)
	res := s.handleCheckout(fctx, c, req)
	fetch.SetAttributes(attribute.Int("http.response.status_code", res.status))
	if res.status >= 400 {
		fetch.SetAttributes(attribute.String("error.type", fmt.Sprint(res.status)))
		fetch.SetStatus(codes.Error, "")
	}
	fetch.End(trace.WithTimestamp(c.advance(latency)))

	severity, body := otellog.SeverityInfo, "order confirmed"
	if res.outcome == "payment_pending" {
		severity, body = otellog.SeverityWarn, "order placed with payment pending"
	}
	end := c.advance(s.between(5*time.Millisecond, 20*time.Millisecond))
	wide := append(req.sessionAttrs(),
		attribute.String("http.request.method", "POST"),
		attribute.String("url.full", "https://shop.example.com/api/v1/checkout"),
		attribute.Int("http.response.status_code", res.status),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.String("user.id", req.userID),
		attribute.String("user.team.id", req.tenant),
		attribute.String("outcome", res.outcome),
	)
	if res.errorType != "" {
		wide = append(wide, attribute.String("error.type", res.errorType))
	}
	s.log(ctx, web, end, severity, body, append(wide, attribute.Bool("main", true))...)
	click.End(trace.WithTimestamp(end))
}

// handleCheckout simulates checkout serving POST /api/v1/checkout: it reads the cart from
// cache, runs a few database queries and calls payments. A payment gateway timeout fails the
// payments request, and checkout accepts the order with payment pending, so its own request
// still succeeds.
func (s *simulator) handleCheckout(ctx context.Context, c *clock, req request) checkoutResult {
	checkout := s.pick(s.checkout)
	start := c.now
	// The load balancer terminates TLS and forwards over HTTP/1.1. The shopper's address and the
	// original scheme come from X-Forwarded-For and X-Forwarded-Proto.
	requestAttrs := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/checkout"),
		attribute.String("url.path", "/api/v1/checkout"),
		attribute.String("url.scheme", "https"),
		attribute.String("server.address", "shop.example.com"),
		attribute.Int("server.port", 443),
		attribute.String("client.address", req.session.ip),
		attribute.String("network.peer.address", loadBalancerIPs[s.rng.IntN(len(loadBalancerIPs))]),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("user_agent.original", browserProfiles[req.session.browser].userAgent),
		attribute.Int("http.request.body.size", 120+90*req.items),
		attribute.String("user.id", req.userID),
		attribute.String("user.team.id", req.tenant),
		attribute.String("session.id", req.session.id),
	}
	ctx, root := checkout.tracer.Start(ctx, "POST /api/v1/checkout",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start), trace.WithAttributes(requestAttrs...))
	s.log(ctx, checkout, c.advance(time.Millisecond), otellog.SeverityInfo, "checkout started",
		attribute.String("user.id", req.userID), attribute.Int("cart.items", req.items))

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

	paid, errorType := s.charge(ctx, checkout, c, req)

	outcome, status, severity, body := "success", 200, otellog.SeverityInfo, "checkout completed"
	if !paid {
		outcome, status, severity, body = "payment_pending", 202, otellog.SeverityWarn, "checkout accepted with payment pending"
	}
	end := c.advance(s.between(time.Millisecond, 4*time.Millisecond))
	wide := append(requestAttrs,
		attribute.Int("http.response.status_code", status),
		attribute.Int("http.response.body.size", 280+s.rng.IntN(40)),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.Int("db.query_count", queries),
		attribute.Float64("db.duration_ms", millis(dbTime)),
		attribute.Bool("cache.hit", cacheHit),
		attribute.String("outcome", outcome),
	)
	if errorType != "" {
		wide = append(wide, attribute.String("error.type", errorType))
	}
	// Server span status stays unset below 500: a 202 is a success for checkout.
	root.SetAttributes(wide...)
	s.log(ctx, checkout, end, severity, body, append(wide, attribute.Bool("main", true))...)
	root.End(trace.WithTimestamp(end))
	return checkoutResult{status: status, outcome: outcome, errorType: errorType}
}

// paymentProvider is an external payment API that payments charges cards through.
type paymentProvider struct {
	name, host, path string
}

var paymentProviders = []paymentProvider{
	{name: "stripe", host: "api.stripe.com", path: "/v1/charges"},
	{name: "adyen", host: "checkout-live.adyen.com", path: "/v71/payments"},
	{name: "braintree", host: "payments.braintree-api.com", path: "/graphql"},
}

// charge is checkout's call to payments: a client span in checkout and, under it, the
// payments server span with its own gateway call and logs. It reports whether the charge
// succeeded and, if not, the error type.
func (s *simulator) charge(ctx context.Context, checkout instance, c *clock, req request) (bool, string) {
	fail := s.rng.Float64() < s.errorRatio
	payments := s.pick(s.payments)
	ctx, call := checkout.tracer.Start(ctx, "POST /api/v1/charge", trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", "payments"),
			attribute.Int("server.port", 8080),
			attribute.String("url.full", "http://payments:8080/api/v1/charge"),
			attribute.String("url.template", "/api/v1/charge"),
			attribute.String("network.peer.address", payments.podIP),
			attribute.Int("network.peer.port", 8080),
			attribute.String("network.protocol.version", "1.1"),
			attribute.String("service.peer.name", "payments"),
			attribute.String("service.peer.namespace", s.namespace),
		))
	start := c.advance(time.Millisecond)

	requestAttrs := []attribute.KeyValue{
		attribute.String("http.request.method", "POST"),
		attribute.String("http.route", "/api/v1/charge"),
		attribute.String("url.path", "/api/v1/charge"),
		attribute.String("url.scheme", "http"),
		attribute.String("server.address", "payments"),
		attribute.Int("server.port", 8080),
		attribute.String("client.address", checkout.podIP),
		attribute.String("network.peer.address", checkout.podIP),
		attribute.String("network.protocol.version", "1.1"),
		attribute.String("user_agent.original", "Go-http-client/1.1"),
		attribute.Int("http.request.body.size", 150+s.rng.IntN(30)),
		attribute.String("user.id", req.userID),
		attribute.String("user.team.id", req.tenant),
		attribute.String("session.id", req.session.id),
	}
	pctx, srv := payments.tracer.Start(ctx, "POST /api/v1/charge",
		trace.WithSpanKind(trace.SpanKindServer), trace.WithTimestamp(start), trace.WithAttributes(requestAttrs...))
	amount := float64(req.items) * (5 + float64(s.rng.IntN(9500))/100)
	s.log(pctx, payments, c.advance(time.Millisecond), otellog.SeverityInfo, "authorizing card",
		attribute.Float64("payment.amount", amount), attribute.String("payment.currency", "USD"))

	provider := paymentProviders[s.rng.IntN(len(paymentProviders))]
	_, gw := payments.tracer.Start(pctx, "POST "+provider.path, trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(c.now), trace.WithAttributes(
			attribute.String("http.request.method", "POST"),
			attribute.String("server.address", provider.host),
			attribute.Int("server.port", 443),
			attribute.String("url.full", "https://"+provider.host+provider.path),
			attribute.String("url.template", provider.path),
			attribute.String("network.protocol.version", "2"),
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
		gw.SetAttributes(attribute.Int("http.response.status_code", 200))
		gw.End(trace.WithTimestamp(c.advance(s.between(30*time.Millisecond, 110*time.Millisecond))))
	}

	end := c.advance(s.between(time.Millisecond, 3*time.Millisecond))
	wide := append(requestAttrs,
		attribute.Int("http.response.status_code", status),
		attribute.Int("http.response.body.size", 90+s.rng.IntN(20)),
		attribute.Float64("duration_ms", millis(end.Sub(start))),
		attribute.Int("db.query_count", 0),
		attribute.Bool("cache.hit", false),
		attribute.String("outcome", outcome),
	)
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
