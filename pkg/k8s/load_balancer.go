package k8s

import (
	"math/rand"
	"sync"
)

// LoadBalancer a LoadBalancer support multi policy
type LoadBalancer interface {
	GetBackend() (string, error)
}

func NewLoadBalancer(policy string, fetcher UpstreamFetcher) LoadBalancer {
	var lb LoadBalancer
	switch policy {
	case "RoundRobin":
		lb = NewRoundRobinLB(fetcher)
	case "Random":
		lb = NewRandomLB(fetcher)
	default:
		// fallback to RoundRobin
		lb = NewRoundRobinLB(fetcher)
	}
	return lb
}

// NewRoundRobinLB construct a RoundRobinLB object
func NewRoundRobinLB(fetcher UpstreamFetcher) LoadBalancer {
	lb := RoundRobinLB{lastTarget: -1, fetcher: fetcher}
	return &lb
}

// RoundRobinLB load balancer using RodRobin policy
type RoundRobinLB struct {
	lastTarget int
	fetcher    UpstreamFetcher
	mu         sync.Mutex
}

// GetBackend select a backend from upstreams
func (lb *RoundRobinLB) GetBackend() (string, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	n := len(upstreams)
	target := (lb.lastTarget + 1) % n
	lb.lastTarget = target

	return upstreams[target], nil
}

func NewRandomLB(fetcher UpstreamFetcher) LoadBalancer {
	return &RandomLB{fetcher: fetcher}
}

// RandomLB load balancer using random policy
type RandomLB struct {
	fetcher UpstreamFetcher
}

func (lb *RandomLB) GetBackend() (string, error) {

	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	n := len(upstreams)

	target := rand.Intn(n)

	return upstreams[target], nil
}
