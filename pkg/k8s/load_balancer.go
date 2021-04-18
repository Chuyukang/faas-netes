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
	case "WeightedRR":
		lb = NewWeightedRRLB(fetcher)
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

func NewWeightedRRLB(fetcher UpstreamFetcher) LoadBalancer {
	// TODO: use dynamic weight
	lb := WeightedRRLB{fetcher: fetcher, endpointWeight: []int{1, 1, 3}, round: 0, curQueue: 0}
	return &lb
}

type WeightedRRLB struct {
	endpointWeight []int
	round          int
	curQueue       int

	fetcher UpstreamFetcher
	mu      sync.Mutex
}

func (lb *WeightedRRLB) GetBackend() (string, error) {
	lb.mu.Lock()
	defer lb.mu.Unlock()

	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	// TODO: update endpointWeight, set appropriate default weight

	n := len(upstreams)
	maxWeight := 0
	for _, weight := range lb.endpointWeight {
		if weight > maxWeight {
			maxWeight = weight
		}
	}

	target := 0
	var r, cur int
	// iterate all rounds
	for r = lb.round; ; r = (r + 1) % maxWeight {
		// iterate all queue, compare weight with round number
		for cur = lb.curQueue; cur < n; cur++ {
			if lb.endpointWeight[cur] > r {
				target = cur
				goto OUT
			}
		}
	}

OUT:
	// write back object value
	if cur == n-1 { // if we have iterated all queue
		lb.curQueue = 0
		lb.round = (r + 1) % maxWeight
	} else { // when we have not
		lb.curQueue = cur + 1
	}
	return upstreams[target], nil
}
