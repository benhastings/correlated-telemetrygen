package main

import (
	"fmt"
	"math/rand/v2"
	"runtime"

	"go.opentelemetry.io/otel/attribute"
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
		})
	}
	return replicas
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
