package k8s

import (
	"fmt"
	"github.com/openfaas/faas-provider/proxy"
	v1 "k8s.io/client-go/listers/apps/v1"
	coreLister "k8s.io/client-go/listers/core/v1"
	"log"
	"net/url"
	"sync"
	"time"
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

func NewFunctionResolver(defaultNamespace string,
	lister v1.DeploymentLister, endpointsLister coreLister.EndpointsLister) proxy.BaseURLResolver {
	r := FunctionResolver{
		DefaultNamespace: defaultNamespace,
		DeploymentLister: lister,
		EndpointsLister:  endpointsLister,
		EndpointNSLister: map[string]coreLister.EndpointsNamespaceLister{},
		LoadBalancers:    map[string]LoadBalancer{},
	}
	return &r
}

func (r *FunctionResolver) Resolve(name string) (url.URL, error) {
	functionName := name
	var namespace string
	functionName, namespace = GetFuncName(functionName, r.DefaultNamespace)

	var lb LoadBalancer
	// cache load balancer
	lb = r.GetLoadBalancer(namespace, functionName)
	if lb == nil {
		start := time.Now()
		policy := GetLoadBalancePolicy(namespace, functionName, r.DeploymentLister)
		past := time.Since(start)
		log.Printf("Function %s load balance policy: %s. Use time: %s\n", functionName, policy, past)

		// cache EndpointsNamespaceLister
		var lister coreLister.EndpointsNamespaceLister
		lister = r.GetEndpointNSLister(namespace)
		if lister == nil {
			r.SetEndpointNSLister(namespace, r.EndpointsLister.Endpoints(namespace))
			lister = r.GetEndpointNSLister(namespace) // Get Read Lock
		}

		// wire LoadBalancer
		fetcher := NewServiceFetcher(namespace, functionName, lister)
		r.SetLoadBalancer(namespace, functionName, NewLoadBalancer(policy, fetcher))
		lb = r.GetLoadBalancer(namespace, functionName) // Get Read Lock
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
