package k8s

import (
	"fmt"
	v1 "k8s.io/client-go/listers/core/v1"
)

// UpstreamFetcher get backends
// function name and namespace name is specified by construct UpstreamFetcher
type UpstreamFetcher interface {
	FetchUpstream() ([]string, error)
}

// ServiceFetcher get upstream by EndpointsNamespaceLister
type ServiceFetcher struct {
	nsEndpointLister v1.EndpointsNamespaceLister
	functionName     string
	namespace        string
}

func NewServiceFetcher(namespace string, functionName string, lister v1.EndpointsNamespaceLister) UpstreamFetcher {
	fetcher := ServiceFetcher{namespace: namespace, functionName: functionName, nsEndpointLister: lister}
	return &fetcher
}

func (f *ServiceFetcher) FetchUpstream() ([]string, error) {

	svc, err := f.nsEndpointLister.Get(f.functionName)
	if err != nil {
		return nil, fmt.Errorf("error listing \"%s.%s\": %s", f.functionName, f.namespace, err.Error())
	}

	if len(svc.Subsets) == 0 {
		return nil, fmt.Errorf("no subsets available for \"%s.%s\"", f.functionName, f.namespace)
	}

	all := len(svc.Subsets[0].Addresses)
	if len(svc.Subsets[0].Addresses) == 0 {
		return nil, fmt.Errorf("no addresses in subset for \"%s.%s\"", f.functionName, f.namespace)
	}

	//upstreams := make([]string, 0) // len=0, capacity=?
	upstreams := make([]string, 0, all) // len=0, capacity=all
	for _, s := range svc.Subsets[0].Addresses {
		upstreams = append(upstreams, s.IP)
	}

	return upstreams, nil
}
