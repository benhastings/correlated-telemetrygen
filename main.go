// Command correlated-telemetrygen sends synthetic OTLP traces and logs in which every log
// record belongs to a span of the same request, so a backend can navigate from a log line to
// its trace and from a span to its logs.
//
// It simulates a checkout service that calls a payments service. The OTLP endpoint and any
// extra resource attributes come from the standard OTEL_EXPORTER_OTLP_* and
// OTEL_RESOURCE_ATTRIBUTES environment variables.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"math/rand/v2"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

const scope = "github.com/brandonapol/correlated-telemetrygen"

// version is set at build time with -ldflags "-X main.version=...".
var version = "dev"

type serviceIdentity struct {
	name, version, team, gitHash string
}

var (
	checkoutIdentity = serviceIdentity{name: "checkout", version: "v2.4.1", team: "core-commerce", gitHash: "8f7a2b3c"}
	paymentsIdentity = serviceIdentity{name: "payments", version: "v1.9.0", team: "core-payments", gitHash: "c41d09e7"}
)

func main() {
	rate := flag.Float64("rate", 2, "checkout requests per second")
	errorRatio := flag.Float64("error-ratio", 0.25, "fraction of payments that time out at the gateway")
	environment := flag.String("environment", "local", "deployment.environment.name on every resource")
	namespace := flag.String("namespace", "shop", "service.namespace on every resource")
	duration := flag.Duration("duration", 0, "how long to run; 0 runs until interrupted")
	seed := flag.Uint64("seed", 0, "random seed; 0 picks one from the clock")
	showVersion := flag.Bool("version", false, "print the version and exit")
	flag.Parse()

	if *showVersion {
		fmt.Println(version)
		return
	}
	if *rate <= 0 || *errorRatio < 0 || *errorRatio > 1 {
		log.Fatal("--rate must be positive and --error-ratio between 0 and 1")
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	var shutdowns []func(context.Context) error
	newService := func(id serviceIdentity) service {
		svc, shutdown, err := newOTLPService(ctx, id, *namespace, *environment)
		if err != nil {
			log.Fatalf("setting up %s: %v", id.name, err)
		}
		shutdowns = append(shutdowns, shutdown)
		return svc
	}

	if *seed == 0 {
		*seed = uint64(time.Now().UnixNano())
	}
	sim := &simulator{
		checkout:   newService(checkoutIdentity),
		payments:   newService(paymentsIdentity),
		errorRatio: *errorRatio,
		rng:        rand.New(rand.NewPCG(*seed, *seed>>1)),
	}

	log.Printf("correlated-telemetrygen %s: %.2g checkout requests/s, %.0f%% payment errors", version, *rate, *errorRatio*100)
	ticker := time.NewTicker(time.Duration(float64(time.Second) / *rate))
	defer ticker.Stop()
	sent := 0
loop:
	for {
		select {
		case <-ctx.Done():
			break loop
		case <-ticker.C:
			sim.run(context.Background(), time.Now())
			sent++
		}
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var errs []error
	for _, shutdown := range shutdowns {
		errs = append(errs, shutdown(shutdownCtx))
	}
	if err := errors.Join(errs...); err != nil {
		log.Fatalf("flushing telemetry: %v", err)
	}
	log.Printf("sent %d interactions", sent)
}

// newOTLPService builds a tracer and a logger that share one resource and export over OTLP gRPC.
func newOTLPService(ctx context.Context, id serviceIdentity, namespace, environment string) (service, func(context.Context) error, error) {
	res, err := resource.New(ctx,
		resource.WithTelemetrySDK(),
		resource.WithFromEnv(),
		resource.WithAttributes(
			attribute.String("service.name", id.name),
			attribute.String("service.namespace", namespace),
			attribute.String("service.version", id.version),
			attribute.String("service.team", id.team),
			attribute.String("service.build.git_hash", id.gitHash),
			attribute.String("deployment.environment.name", environment),
		),
	)
	if err != nil {
		return service{}, nil, err
	}

	spanExporter, err := otlptracegrpc.New(ctx)
	if err != nil {
		return service{}, nil, err
	}
	logExporter, err := otlploggrpc.New(ctx)
	if err != nil {
		return service{}, nil, err
	}
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(spanExporter))
	lp := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(logExporter)))

	shutdown := func(ctx context.Context) error {
		return errors.Join(tp.Shutdown(ctx), lp.Shutdown(ctx))
	}
	return service{tracer: tp.Tracer(scope), logger: lp.Logger(scope)}, shutdown, nil
}
