package main

import (
	"context"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
)

// logRecorder keeps every emitted log record in memory.
type logRecorder struct {
	mu      sync.Mutex
	records []sdklog.Record
}

func (r *logRecorder) Enabled(context.Context, sdklog.EnabledParameters) bool { return true }
func (r *logRecorder) Shutdown(context.Context) error                         { return nil }
func (r *logRecorder) ForceFlush(context.Context) error                       { return nil }
func (r *logRecorder) OnEmit(_ context.Context, rec *sdklog.Record) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec.Clone())
	return nil
}

var testDeployment = deployment{environment: "test", namespace: "shop", region: "us-east-1"}

func recordingFleet(t *testing.T, replicas []replica, spans *tracetest.SpanRecorder, logs *logRecorder) []instance {
	t.Helper()
	var fleet []instance
	for _, r := range replicas {
		res := resource.NewSchemaless(r.resource...)
		tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithSpanProcessor(spans))
		lp := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(logs))
		fleet = append(fleet, instance{tracer: tp.Tracer(scope), logger: lp.Logger(scope), podIP: r.podIP})
	}
	return fleet
}

func simulate(t *testing.T, n int, errorRatio float64) ([]sdktrace.ReadOnlySpan, []sdklog.Record) {
	t.Helper()
	spans := tracetest.NewSpanRecorder()
	logs := &logRecorder{}
	rng := rand.New(rand.NewPCG(1, 2))
	sim := newSimulator(
		recordingFleet(t, browserResources(testDeployment), spans, logs),
		recordingFleet(t, replicaResources(checkoutIdentity, testDeployment, rng), spans, logs),
		recordingFleet(t, replicaResources(paymentsIdentity, testDeployment, rng), spans, logs),
		simConfig{namespace: testDeployment.namespace, errorRatio: errorRatio}, rng,
	)
	start := time.Unix(1_700_000_000, 0)
	for i := range n {
		sim.run(context.Background(), start.Add(time.Duration(i)*time.Second))
	}
	return spans.Ended(), logs.records
}

func serviceOf(res *resource.Resource) string {
	return resourceAttr(res, "service.name")
}

func resourceAttr(res *resource.Resource, key attribute.Key) string {
	v, _ := res.Set().Value(key)
	return v.Emit()
}

func attr(rec sdklog.Record, key string) (attribute.Value, bool) {
	var found attribute.Value
	ok := false
	rec.WalkAttributes(func(kv attribute.KeyValue) bool {
		if string(kv.Key) == key {
			found, ok = kv.Value, true
			return false
		}
		return true
	})
	return found, ok
}

func TestEveryLogBelongsToASpanOfTheSameServiceAndTrace(t *testing.T) {
	spans, logs := simulate(t, 50, 0.25)
	if len(logs) == 0 {
		t.Fatal("no logs emitted")
	}
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	for _, rec := range logs {
		span, ok := byID[rec.SpanID()]
		if !ok {
			t.Fatalf("log %q has span ID %s, which no span has", rec.Body().AsString(), rec.SpanID())
		}
		if span.SpanContext().TraceID() != rec.TraceID() {
			t.Fatalf("log %q is in trace %s but its span is in %s", rec.Body().AsString(), rec.TraceID(), span.SpanContext().TraceID())
		}
		if serviceOf(span.Resource()) != serviceOf(rec.Resource()) {
			t.Fatalf("log %q from %s sits in a %s span", rec.Body().AsString(), serviceOf(rec.Resource()), serviceOf(span.Resource()))
		}
		if v, _ := attr(rec, "trace_id"); v.AsString() != rec.TraceID().String() {
			t.Fatalf("trace_id attribute %q does not match the record's trace ID %s", v.AsString(), rec.TraceID())
		}
		if v, _ := attr(rec, "span_id"); v.AsString() != rec.SpanID().String() {
			t.Fatalf("span_id attribute %q does not match the record's span ID %s", v.AsString(), rec.SpanID())
		}
		if ts := rec.Timestamp(); ts.Before(span.StartTime()) || ts.After(span.EndTime()) {
			t.Fatalf("log %q at %s falls outside its span [%s, %s]", rec.Body().AsString(), ts, span.StartTime(), span.EndTime())
		}
	}
}

func TestPaymentsServerSpanIsAChildOfCheckoutsClientCall(t *testing.T) {
	spans, _ := simulate(t, 10, 0.25)
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	found := 0
	for _, s := range spans {
		if serviceOf(s.Resource()) != "payments" || s.SpanKind() != trace.SpanKindServer {
			continue
		}
		found++
		parent, ok := byID[s.Parent().SpanID()]
		if !ok || serviceOf(parent.Resource()) != "checkout" || parent.SpanKind() != trace.SpanKindClient {
			t.Fatalf("payments server span's parent is not a checkout client span")
		}
	}
	if found != 10 {
		t.Fatalf("got %d payments server spans, want 10", found)
	}
}

func isWideEvent(rec sdklog.Record) bool {
	v, ok := attr(rec, "main")
	return ok && v.AsBool()
}

func TestEachServiceEmitsOneWideEventPerRequest(t *testing.T) {
	const n = 20
	_, logs := simulate(t, n, 0.25)
	perService := map[string]int{}
	for _, rec := range logs {
		if !isWideEvent(rec) {
			continue
		}
		svc := serviceOf(rec.Resource())
		perService[svc]++
		keys := []string{"http.response.status_code", "duration_ms", "user.id", "user.team.id", "session.id", "outcome"}
		if svc != "web" {
			keys = append(keys, "http.route", "db.query_count", "cache.hit")
		}
		for _, key := range keys {
			if _, ok := attr(rec, key); !ok {
				t.Fatalf("%s wide event is missing %s", svc, key)
			}
		}
	}
	if perService["web"] != n || perService["checkout"] != n || perService["payments"] != n {
		t.Fatalf("wide events per service = %v, want %d each", perService, n)
	}
}

func TestCheckoutServerSpanIsAChildOfTheBrowsersFetch(t *testing.T) {
	spans, _ := simulate(t, 10, 0.25)
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	found := 0
	for _, s := range spans {
		if serviceOf(s.Resource()) != "checkout" || s.SpanKind() != trace.SpanKindServer {
			continue
		}
		found++
		parent, ok := byID[s.Parent().SpanID()]
		if !ok || serviceOf(parent.Resource()) != "web" || parent.SpanKind() != trace.SpanKindClient {
			t.Fatalf("checkout server span's parent is not a web client span")
		}
		if parent.StartTime().After(s.StartTime()) || parent.EndTime().Before(s.EndTime()) {
			t.Fatalf("the browser's fetch [%s, %s] does not contain the checkout server span [%s, %s]",
				parent.StartTime(), parent.EndTime(), s.StartTime(), s.EndTime())
		}
	}
	if found != 10 {
		t.Fatalf("got %d checkout server spans, want 10", found)
	}
}

func TestWideEventsOfOneRequestShareTheSessionAndUser(t *testing.T) {
	_, logs := simulate(t, 200, 0.25)
	type who struct{ session, user, team string }
	perTrace := map[trace.TraceID]who{}
	sessionsPerUser := map[string]map[string]bool{}
	for _, rec := range logs {
		if !isWideEvent(rec) {
			continue
		}
		sess, _ := attr(rec, "session.id")
		user, _ := attr(rec, "user.id")
		team, _ := attr(rec, "user.team.id")
		w := who{sess.AsString(), user.AsString(), team.AsString()}
		if prev, ok := perTrace[rec.TraceID()]; ok && prev != w {
			t.Fatalf("wide events of one request disagree: %+v and %+v", prev, w)
		}
		perTrace[rec.TraceID()] = w
		if sessionsPerUser[w.user] == nil {
			sessionsPerUser[w.user] = map[string]bool{}
		}
		sessionsPerUser[w.user][w.session] = true
	}
	teams := map[string]string{}
	for _, w := range perTrace {
		if prev, ok := teams[w.user]; ok && prev != w.team {
			t.Fatalf("user %s belongs to teams %s and %s", w.user, prev, w.team)
		}
		teams[w.user] = w.team
	}
	if len(sessionsPerUser) == len(perTrace) {
		t.Fatal("no user checked out twice, so sessions were never reused")
	}
}

func TestBrowserResourcesDescribeTheBrowserNotTheServer(t *testing.T) {
	for _, r := range browserResources(testDeployment) {
		res := resource.NewSchemaless(r.resource...)
		if got := resourceAttr(res, "telemetry.sdk.language"); got != "webjs" {
			t.Fatalf("web telemetry.sdk.language = %q, want webjs", got)
		}
		for _, key := range []attribute.Key{"service.instance.id", "cloud.region", "k8s.pod.name"} {
			if v := resourceAttr(res, key); v != "" {
				t.Fatalf("browser resource has %s=%s", key, v)
			}
		}
		ua := resourceAttr(res, "user_agent.original")
		hasBrands := resourceAttr(res, "browser.brands") != ""
		if chromium := strings.Contains(ua, "Chrome/"); chromium != hasBrands {
			t.Fatalf("browser.brands should be set exactly for Chromium browsers, got brands=%v for %s", hasBrands, ua)
		}
	}
}

func TestPaymentErrorsStayOutOfCheckoutsServerSpan(t *testing.T) {
	spans, _ := simulate(t, 400, 0.25)
	var payments, paymentErrors int
	for _, s := range spans {
		if s.SpanKind() != trace.SpanKindServer {
			continue
		}
		switch serviceOf(s.Resource()) {
		case "checkout":
			if s.Status().Code != codes.Unset {
				t.Fatalf("a checkout server span has status %s, want unset", s.Status().Code)
			}
		case "payments":
			payments++
			if s.Status().Code == codes.Error {
				paymentErrors++
			}
		}
	}
	if ratio := float64(paymentErrors) / float64(payments); ratio < 0.18 || ratio > 0.32 {
		t.Fatalf("payments error ratio %.2f, want about 0.25", ratio)
	}
}

func TestReplicasAreDistinctPodsSpreadAcrossZones(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	for _, id := range []serviceIdentity{checkoutIdentity, paymentsIdentity} {
		replicas := replicaResources(id, testDeployment, rng)
		if len(replicas) != id.replicas {
			t.Fatalf("%s has %d replicas, want %d", id.name, len(replicas), id.replicas)
		}
		seen := map[string]map[string]bool{}
		for _, r := range replicas {
			res := resource.NewSchemaless(r.resource...)
			for _, key := range []attribute.Key{
				"service.instance.id", "service.criticality", "cloud.provider", "cloud.platform", "cloud.account.id",
				"cloud.region", "cloud.availability_zone", "k8s.cluster.name", "k8s.namespace.name", "k8s.deployment.name",
				"k8s.replicaset.name", "k8s.pod.name", "k8s.pod.uid", "k8s.node.name", "container.name",
				"container.image.name", "container.image.tags", "host.arch", "process.runtime.name", "process.runtime.version",
			} {
				v := resourceAttr(res, key)
				if v == "" {
					t.Fatalf("%s replica is missing %s", id.name, key)
				}
				if seen[string(key)] == nil {
					seen[string(key)] = map[string]bool{}
				}
				seen[string(key)][v] = true
			}
		}
		for _, key := range []string{"service.instance.id", "k8s.pod.name", "k8s.pod.uid", "cloud.availability_zone"} {
			if len(seen[key]) != id.replicas {
				t.Fatalf("%s replicas share a %s: %v", id.name, key, seen[key])
			}
		}
		if len(seen["k8s.replicaset.name"]) != 1 {
			t.Fatalf("%s replicas belong to different replica sets: %v", id.name, seen["k8s.replicaset.name"])
		}
	}
}

func TestRequestsSpreadAcrossReplicasAndStayOnOne(t *testing.T) {
	spans, logs := simulate(t, 100, 0.25)
	perService := map[string]map[string]bool{}
	perTrace := map[string]string{}
	note := func(res *resource.Resource, traceID trace.TraceID) {
		svc, inst := serviceOf(res), resourceAttr(res, "service.instance.id")
		if perService[svc] == nil {
			perService[svc] = map[string]bool{}
		}
		perService[svc][inst] = true
		key := traceID.String() + "/" + svc
		if prev, ok := perTrace[key]; ok && prev != inst {
			t.Fatalf("one %s request was served by two instances, %s and %s", svc, prev, inst)
		}
		perTrace[key] = inst
	}
	for _, s := range spans {
		note(s.Resource(), s.SpanContext().TraceID())
	}
	for _, rec := range logs {
		note(rec.Resource(), rec.TraceID())
	}
	for svc, replicas := range map[string]int{"checkout": checkoutIdentity.replicas, "payments": paymentsIdentity.replicas} {
		if len(perService[svc]) != replicas {
			t.Fatalf("%s requests were served by %d instances, want %d", svc, len(perService[svc]), replicas)
		}
	}
}

func spanAttr(s sdktrace.ReadOnlySpan, key attribute.Key) (attribute.Value, bool) {
	for _, kv := range s.Attributes() {
		if kv.Key == key {
			return kv.Value, true
		}
	}
	return attribute.Value{}, false
}

func TestHTTPSpansHaveTheAttributesSemconvRequires(t *testing.T) {
	spans, _ := simulate(t, 100, 0.25)
	clients := 0
	for _, s := range spans {
		if _, ok := spanAttr(s, "peer.service"); ok {
			t.Fatalf("span %q uses the deprecated peer.service", s.Name())
		}
		if _, ok := spanAttr(s, "http.request.method"); !ok {
			continue
		}
		var required []attribute.Key
		switch s.SpanKind() {
		case trace.SpanKindServer:
			required = []attribute.Key{"url.path", "url.scheme", "http.response.status_code"}
		case trace.SpanKindClient:
			clients++
			required = []attribute.Key{"server.address", "server.port", "url.full"}
		default:
			t.Fatalf("%s span %q has http.request.method", s.SpanKind(), s.Name())
		}
		for _, key := range required {
			if _, ok := spanAttr(s, key); !ok {
				t.Fatalf("%s %s span %q is missing %s", serviceOf(s.Resource()), s.SpanKind(), s.Name(), key)
			}
		}
	}
	if clients != 300 {
		t.Fatalf("got %d HTTP client spans, want 300 (browser, checkout and payments, one each per request)", clients)
	}
}

func TestPeerAddressesIdentifyTheReplicaOnTheOtherEnd(t *testing.T) {
	spans, _ := simulate(t, 100, 0.25)
	byID := map[trace.SpanID]sdktrace.ReadOnlySpan{}
	for _, s := range spans {
		byID[s.SpanContext().SpanID()] = s
	}
	// Each address must belong to exactly one pod, and each pod must have exactly one address.
	addrToPod, podToAddr := map[string]string{}, map[string]string{}
	pair := func(addr attribute.Value, pod string) {
		a := addr.AsString()
		if a == "" {
			t.Fatalf("no address recorded for pod %s", pod)
		}
		if prev, ok := addrToPod[a]; ok && prev != pod {
			t.Fatalf("address %s belongs to pods %s and %s", a, prev, pod)
		}
		if prev, ok := podToAddr[pod]; ok && prev != a {
			t.Fatalf("pod %s has addresses %s and %s", pod, prev, a)
		}
		addrToPod[a], podToAddr[pod] = pod, a
	}
	for _, srv := range spans {
		if serviceOf(srv.Resource()) != "payments" || srv.SpanKind() != trace.SpanKindServer {
			continue
		}
		call := byID[srv.Parent().SpanID()]
		peer, _ := spanAttr(call, "network.peer.address")
		pair(peer, resourceAttr(srv.Resource(), "k8s.pod.name"))
		client, _ := spanAttr(srv, "client.address")
		pair(client, resourceAttr(call.Resource(), "k8s.pod.name"))
	}
	if len(podToAddr) != checkoutIdentity.replicas+paymentsIdentity.replicas {
		t.Fatalf("saw addresses for %d pods, want %d", len(podToAddr), checkoutIdentity.replicas+paymentsIdentity.replicas)
	}
}
