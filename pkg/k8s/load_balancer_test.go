package k8s

import (
	"fmt"
	"testing"
)

func TestRoundRobinLB_GetBackend(t *testing.T) {
	upstreams := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	fetcher := NewFakeUpstreamFetcher(upstreams)
	lb := NewRoundRobinLB(fetcher)

	testCases := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3", "10.0.0.1"}

	for i := 0; i < len(testCases); i++ {
		got, _ := lb.GetBackend()
		expect := testCases[i]
		if got != expect {
			t.Fatalf("expected %s, got %s", expect, got)
		}
	}
}

func TestWeightedRRLB_GetBackend(t *testing.T) {
	upstreams := []string{"10.0.0.1", "10.0.0.2", "10.0.0.3"}
	fetcher := NewFakeUpstreamFetcher(upstreams)
	lb := &WeightedRRLB{endpointWeight: []int{1,1,3}, fetcher: fetcher}

	bucket := make(map[string]int)
	for i := 0; i < 1000; i++ {
		backend, err := lb.GetBackend()
		if err!=nil {
			t.Fail()
		}
		bucket[backend]+=1
	}

	for k, v := range bucket {
		fmt.Printf("IP: %s, Count: %d\n", k, v)
	}
}

type FakeUpstreamFetcher struct {
	upstreams []string
}

func NewFakeUpstreamFetcher(upstreams []string) UpstreamFetcher {
	return &FakeUpstreamFetcher{upstreams: upstreams}
}

func (fetcher *FakeUpstreamFetcher) FetchUpstream() ([]string, error) {
	return fetcher.upstreams, nil
}
