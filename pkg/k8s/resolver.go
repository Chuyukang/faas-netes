package k8s

import (
	"fmt"
	v1 "k8s.io/client-go/listers/apps/v1"
	coreLister "k8s.io/client-go/listers/core/v1"
	"net/url"
	"sync"
)

// FunctionResolver a resolver enhanced by load balance policy
// available policy: RoundRobin, Random
// TODO: add hash policy support
type FunctionResolver struct {
	DefaultNamespace string
	DeploymentLister v1.DeploymentLister
	EndpointsLister  coreLister.EndpointsLister

	EndpointNSLister map[string]coreLister.EndpointsNamespaceLister
	LoadBalancers    map[string]LoadBalancer

	rwMu      sync.RWMutex // for EndpointNSLister
	cacheRWMu sync.RWMutex // for LoadBalancers
}

func (r *FunctionResolver) Resolve(name string) (url.URL, error) {
	functionName := name
	var namespace string
	functionName, namespace = GetFuncName(functionName, r.DefaultNamespace)

	var lb LoadBalancer
	// cache load balancer
	lb = r.GetLoadBalancer(namespace, functionName)
	if lb == nil {
		policy := GetLoadBalancePolicy(functionName, namespace, r.DeploymentLister)

		// cache EndpointsNamespaceLister
		var lister coreLister.EndpointsNamespaceLister
		lister = r.GetEndpointNSLister(namespace)
		if lister == nil {
			lister = r.EndpointsLister.Endpoints(namespace)
			r.SetEndpointNSLister(namespace, lister)
		}
		fetcher := NewServiceFetcher(namespace, functionName, lister)

		switch policy {
		case "RoundRobin":
			lb = NewRoundRobinLB(fetcher)
		case "Random":
			lb = NewRandomLB(fetcher)
		default:
			lb = NewRoundRobinLB(fetcher)
		}
		r.SetLoadBalancer(namespace, functionName, lb)
	}

	// select a backend using load balance algorithm
	serviceIP, err := lb.GetBackend()
	if err != nil {
		// todo: log the error or just return ?
		// todo: at which point, the error will be handled ?
		return url.URL{}, err
	}

	urlStr := fmt.Sprintf("http://%s:%d", serviceIP, watchdogPort)

	urlRes, err := url.Parse(urlStr)
	if err != nil {
		return url.URL{}, err
	}

	return *urlRes, nil
}

func (r *FunctionResolver) GetEndpointNSLister(namespace string) coreLister.EndpointsNamespaceLister {
	r.rwMu.RLock()
	defer r.rwMu.RUnlock()
	return r.EndpointNSLister[namespace]
}

func (r *FunctionResolver) SetEndpointNSLister(namespace string, lister coreLister.EndpointsNamespaceLister) {
	r.rwMu.Lock()
	defer r.rwMu.Unlock()
	r.EndpointNSLister[namespace] = lister
}

func (r *FunctionResolver) GetLoadBalancer(ns string, functionName string) LoadBalancer {
	r.cacheRWMu.RLock()
	defer r.cacheRWMu.RUnlock()
	key := ns + "#" + functionName
	return r.LoadBalancers[key]
}

func (r *FunctionResolver) SetLoadBalancer(ns string, functionName string, lb LoadBalancer) {
	r.cacheRWMu.Lock()
	defer r.cacheRWMu.Unlock()
	key := ns + "#" + functionName
	r.LoadBalancers[key] = lb
}

func (r *FunctionResolver) verifyNamespace(name string) error {
	if name != "kube-system" {
		return nil
	}
	// ToDo use global namepace parse and validation
	return fmt.Errorf("namespace not allowed")
}
