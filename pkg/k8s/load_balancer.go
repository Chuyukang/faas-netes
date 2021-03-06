package k8s

import (
	"fmt"
	"k8s.io/apimachinery/pkg/api/resource"
	v1 "k8s.io/client-go/listers/core/v1"
	metricsClient "k8s.io/metrics/pkg/client/clientset/versioned/typed/metrics/v1beta1"
	"log"
	"math/rand"
	"sync"
	"time"
)

// LoadBalancer a LoadBalancer support multi policy
type LoadBalancer interface {
	GetBackend() (string, error)
}

type FunctionLBInfo struct {
	functionName string
	namespace    string

	podLister     v1.PodLister
	metricsGetter metricsClient.PodMetricsesGetter
}

func NewLoadBalancer(policy string, fetcher UpstreamFetcher, info FunctionLBInfo) LoadBalancer {
	var lb LoadBalancer
	switch policy {
	case "RoundRobin":
		lb = NewRoundRobinLB(fetcher)
	case "Random":
		lb = NewRandomLB(fetcher)
	case "WeightedRR":
		lb = NewWeightedRRLB(fetcher)
	case "LeastCPU":
		lb = NewLeastCPULB(fetcher, info)
	case "LeastMem":
		lb = NewLeastMemLB(fetcher, info)
	case "LessCPU":
		lb = NewLessCPULB(fetcher, info)
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

func NewLeastCPULB(fetcher UpstreamFetcher, info FunctionLBInfo) LoadBalancer {
	lb := LeastCPULB{functionName: info.functionName, namespace: info.namespace, fetcher: fetcher,
		index: PodMetricsIndex{index: map[string]*PodSimpleMetrics{}}}
	go func() {
		for {
			updatePodMetricsIndex(&lb.index, info)

			time.Sleep(15 * time.Second)
		}
	}()
	return &lb
}

type LeastCPULB struct {
	namespace    string
	functionName string
	index        PodMetricsIndex

	fetcher UpstreamFetcher
}

func (lb *LeastCPULB) GetBackend() (string, error) {
	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	lb.index.mu.RLock()
	defer lb.index.mu.RUnlock()

	if len(upstreams) < 1 {
		return "", fmt.Errorf("no avaliable endpoint for function")
	}

	target := 0
	firstElem, ok := lb.index.index[upstreams[0]]
	if !ok {
		firstElem = &PodSimpleMetrics{
			PodCPU: resource.NewScaledQuantity(0, 0), PodMem: resource.NewScaledQuantity(0, 0),
		}
	}
	minCPU := firstElem.PodCPU
	log.Printf("--------\n")
	for i, backend := range upstreams {
		podSimpleMetrics, exists := lb.index.index[backend]
		if !exists {
			podSimpleMetrics = &PodSimpleMetrics{
				PodCPU: resource.NewScaledQuantity(0, 0), PodMem: resource.NewScaledQuantity(0, 0),
			}
		}
		curCPU := podSimpleMetrics.PodCPU
		if curCPU.Cmp(*minCPU) < 0 {
			target = i
			minCPU = curCPU
		}
		log.Printf("IP: %s, PodCPU: %s, PodMem: %s\n",
			backend, podSimpleMetrics.PodCPU.String(), podSimpleMetrics.PodMem.String())
	}
	log.Printf("target: %s\n", upstreams[target])
	log.Printf("--------\n")
	return upstreams[target], nil
}

func NewLeastMemLB(fetcher UpstreamFetcher, info FunctionLBInfo) LoadBalancer {
	lb := LeastMemLB{functionName: info.functionName, namespace: info.namespace, fetcher: fetcher,
		index: PodMetricsIndex{index: map[string]*PodSimpleMetrics{}}}
	go func() {
		for {
			updatePodMetricsIndex(&lb.index, info)

			time.Sleep(15 * time.Second)
		}
	}()
	return &lb
}

type LeastMemLB struct {
	namespace    string
	functionName string
	index        PodMetricsIndex

	fetcher UpstreamFetcher
}

func (lb *LeastMemLB) GetBackend() (string, error) {
	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	lb.index.mu.RLock()
	defer lb.index.mu.RUnlock()

	if len(upstreams) < 1 {
		return "", fmt.Errorf("no avaliable endpoint for function")
	}

	target := 0
	firstElem, ok := lb.index.index[upstreams[0]]
	if !ok {
		firstElem = &PodSimpleMetrics{
			PodCPU: resource.NewScaledQuantity(0, 0), PodMem: resource.NewScaledQuantity(0, 0),
		}
	}
	minMem := firstElem.PodMem
	for i, backend := range upstreams {
		podSimpleMetrics, exists := lb.index.index[backend]
		if !exists {
			podSimpleMetrics = &PodSimpleMetrics{
				PodCPU: resource.NewScaledQuantity(0, 0), PodMem: resource.NewScaledQuantity(0, 0),
			}
		}
		curMem := podSimpleMetrics.PodMem
		if curMem.Cmp(*minMem) < 0 {
			target = i
			minMem = curMem
			fmt.Printf("use %d as temp target\n", target)
		}
		fmt.Printf("IP: %s, PodCPU: %s, PodMem: %s\n",
			backend, podSimpleMetrics.PodCPU.String(), podSimpleMetrics.PodMem.String())
	}
	return upstreams[target], nil
}

func NewLessCPULB(fetcher UpstreamFetcher, info FunctionLBInfo) LoadBalancer {
	lb := LessCPULB{functionName: info.functionName, namespace: info.namespace, fetcher: fetcher,
		index: PodMetricsIndex{index: map[string]*PodSimpleMetrics{}}}
	go func() {
		for {
			updatePodMetricsIndex(&lb.index, info)

			time.Sleep(30 * time.Second)
		}
	}()
	return &lb
}

type LessCPULB struct {
	namespace    string
	functionName string
	index        PodMetricsIndex

	fetcher UpstreamFetcher
}

func (lb *LessCPULB) GetBackend() (string, error) {
	upstreams, err := lb.fetcher.FetchUpstream()
	if err != nil {
		return "", err
	}

	lb.index.mu.RLock()
	defer lb.index.mu.RUnlock()

	n := len(upstreams)
	target1 := rand.Intn(n)
	target2 := rand.Intn(n)

	target1Metrics, exist := lb.index.index[upstreams[target1]]
	if !exist {
		target1Metrics = &PodSimpleMetrics{PodCPU: resource.NewScaledQuantity(0, 0)}
	}
	target2Metrics, exist := lb.index.index[upstreams[target2]]
	if !exist {
		target2Metrics = &PodSimpleMetrics{PodCPU: resource.NewScaledQuantity(0, 0)}
	}

	target := target2
	if target1Metrics.PodCPU.Cmp(*target2Metrics.PodCPU) < 0 {
		target = target1
	}

	return upstreams[target], nil
}
