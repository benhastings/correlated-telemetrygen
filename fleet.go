package main

import (
	"fmt"
	"math/rand/v2"
	"runtime"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk"
)

// deployment is where the simulated fleet runs: one EKS cluster in one region.
type deployment struct {
	environment, namespace, region string
}

func (d deployment) clusterName() string {
	return fmt.Sprintf("%s-%s-%s", d.namespace, d.environment, d.region)
}

type serviceIdentity struct {
	name, version, team, gitHash, criticality string
	replicas                                  int
}

var (
	checkoutIdentity = serviceIdentity{name: "checkout", version: "v2.4.1", team: "core-commerce", gitHash: "8f7a2b3c", criticality: "high", replicas: 3}
	paymentsIdentity = serviceIdentity{name: "payments", version: "v1.9.0", team: "core-payments", gitHash: "c41d09e7", criticality: "critical", replicas: 2}
)

const awsAccountID = "123456789012"

// replicaResources lays the replicas of a service out across availability zones and returns
// the resource attributes of each, as the SDK, a resource detector and the k8s attributes
// processor would report them for a pod in EKS. The second replica of each service runs on
// an arm64 (Graviton) node.
func replicaResources(id serviceIdentity, d deployment, rng *rand.Rand) [][]attribute.KeyValue {
	zones := []string{"a", "b", "c"}
	replicaSet := id.name + "-" + k8sSuffix(rng, 9)
	var replicas [][]attribute.KeyValue
	for i := range id.replicas {
		zone := i % len(zones)
		arch := "amd64"
		if i == 1 {
			arch = "arm64"
		}
		// Each zone has its own /19 subnet in a 10.0.0.0/16 VPC.
		node := fmt.Sprintf("ip-10-0-%d-%d.ec2.internal", 32*zone+rng.IntN(32), 1+rng.IntN(254))
		replicas = append(replicas, []attribute.KeyValue{
			attribute.String("service.name", id.name),
			attribute.String("service.namespace", d.namespace),
			attribute.String("service.version", id.version),
			attribute.String("service.instance.id", uuidV4(rng)),
			attribute.String("service.criticality", id.criticality),
			attribute.String("service.team", id.team),
			attribute.String("service.build.git_hash", id.gitHash),
			attribute.String("deployment.environment.name", d.environment),
			attribute.String("cloud.provider", "aws"),
			attribute.String("cloud.platform", "aws_eks"),
			attribute.String("cloud.account.id", awsAccountID),
			attribute.String("cloud.region", d.region),
			attribute.String("cloud.availability_zone", d.region+zones[zone]),
			attribute.String("k8s.cluster.name", d.clusterName()),
			attribute.String("k8s.namespace.name", d.namespace),
			attribute.String("k8s.deployment.name", id.name),
			attribute.String("k8s.replicaset.name", replicaSet),
			attribute.String("k8s.pod.name", replicaSet+"-"+k8sSuffix(rng, 5)),
			attribute.String("k8s.pod.uid", uuidV4(rng)),
			attribute.String("k8s.node.name", node),
			attribute.String("container.name", id.name),
			attribute.String("container.image.name", "ghcr.io/shop/"+id.name),
			attribute.StringSlice("container.image.tags", []string{id.version}),
			attribute.String("host.arch", arch),
			attribute.String("process.runtime.name", "go"),
			attribute.String("process.runtime.version", runtime.Version()),
			attribute.String("telemetry.sdk.name", "opentelemetry"),
			attribute.String("telemetry.sdk.language", "go"),
			attribute.String("telemetry.sdk.version", sdk.Version()),
		})
	}
	return replicas
}

var webIdentity = serviceIdentity{name: "web", version: "v5.12.0", team: "web-platform", gitHash: "1e2d3c4b", criticality: "high"}

// browserProfile is a browser and device that shoppers use. Chromium-based browsers expose
// the User-Agent Client Hints that browser.brands, browser.platform and browser.mobile come
// from; Safari and Firefox don't, so those attributes are left unset for them.
type browserProfile struct {
	brands    []string
	platform  string
	mobile    bool
	language  string
	userAgent string
}

var browserProfiles = []browserProfile{
	{
		brands: []string{"Chromium 141", "Google Chrome 141", "Not?A_Brand 8"}, platform: "Windows", language: "en-US",
		userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
	},
	{
		brands: []string{"Chromium 141", "Google Chrome 141", "Not?A_Brand 8"}, platform: "macOS", language: "en-GB",
		userAgent: "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36",
	},
	{
		brands: []string{"Chromium 141", "Google Chrome 141", "Not?A_Brand 8"}, platform: "Android", mobile: true, language: "es-US",
		userAgent: "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Mobile Safari/537.36",
	},
	{
		brands: []string{"Microsoft Edge 141", "Chromium 141", "Not?A_Brand 8"}, platform: "Windows", language: "en-US",
		userAgent: "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/141.0.0.0 Safari/537.36 Edg/141.0.0.0",
	},
	{
		language:  "en-US",
		userAgent: "Mozilla/5.0 (iPhone; CPU iPhone OS 18_6 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.6 Mobile/15E148 Safari/604.1",
	},
	{
		language:  "de-DE",
		userAgent: "Mozilla/5.0 (X11; Linux x86_64; rv:143.0) Gecko/20100101 Firefox/143.0",
	},
}

// browserResources returns the resource attributes of the web front end in each browser
// profile, as the OpenTelemetry JS browser SDK reports them. A browser has no cloud or k8s
// context and no service.instance.id.
func browserResources(d deployment) [][]attribute.KeyValue {
	var resources [][]attribute.KeyValue
	for _, p := range browserProfiles {
		attrs := []attribute.KeyValue{
			attribute.String("service.name", webIdentity.name),
			attribute.String("service.namespace", d.namespace),
			attribute.String("service.version", webIdentity.version),
			attribute.String("service.criticality", webIdentity.criticality),
			attribute.String("service.team", webIdentity.team),
			attribute.String("service.build.git_hash", webIdentity.gitHash),
			attribute.String("deployment.environment.name", d.environment),
			attribute.String("browser.language", p.language),
			attribute.String("user_agent.original", p.userAgent),
			attribute.String("telemetry.sdk.name", "opentelemetry"),
			attribute.String("telemetry.sdk.language", "webjs"),
			attribute.String("telemetry.sdk.version", "2.1.0"),
		}
		if p.brands != nil {
			attrs = append(attrs,
				attribute.StringSlice("browser.brands", p.brands),
				attribute.String("browser.platform", p.platform),
				attribute.Bool("browser.mobile", p.mobile),
			)
		}
		resources = append(resources, attrs)
	}
	return resources
}

// k8sSuffix returns a random name suffix from the alphabet Kubernetes uses for generated names.
func k8sSuffix(rng *rand.Rand, n int) string {
	const alphabet = "bcdfghjklmnpqrstvwxz2456789"
	b := make([]byte, n)
	for i := range b {
		b[i] = alphabet[rng.IntN(len(alphabet))]
	}
	return string(b)
}

func uuidV4(rng *rand.Rand) string {
	var b [16]byte
	for i := range b {
		b[i] = byte(rng.Uint32())
	}
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}
