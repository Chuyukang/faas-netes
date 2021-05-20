package k8s

import (
	"context"
	v13 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	v12 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
	metricsApi "k8s.io/metrics/pkg/apis/metrics/v1beta1"
	"log"
	"sync"
	"time"
)

type PodSimpleMetrics struct {
	PodCPU *resource.Quantity
	PodMem *resource.Quantity
}

type PodMetricsIndex struct {
	index map[string]*PodSimpleMetrics
	mu    sync.RWMutex
}

func getPodLabelSelector(functionName string) labels.Selector {
	// label selector: faas_function, faas_function=<functionName>
	selector := labels.NewSelector()
	legalFunctionReq, _ := labels.NewRequirement("faas_function", selection.Exists, []string{})
	functionNameReq, _ := labels.NewRequirement("faas_function", selection.Equals, []string{functionName})
	selector = selector.Add(*legalFunctionReq)
	selector = selector.Add(*functionNameReq)
	return selector
}

func getPodSimpleMetric(podMetric metricsApi.PodMetrics) *PodSimpleMetrics {
	// sum container metrics
	podMemory := &resource.Quantity{}
	podCPU := &resource.Quantity{}

	containersMetricsList := podMetric.Containers
	for _, containerMetrics := range containersMetricsList {
		podMemory.Add(containerMetrics.Usage[v13.ResourceMemory])
		podCPU.Add(containerMetrics.Usage[v13.ResourceCPU])
	}
	return &PodSimpleMetrics{PodCPU: podCPU, PodMem: podMemory}
}
//selector labels.Selector, lister v1.PodLister, metricsGetter metricsClient.PodMetricsInterface
func updatePodMetricsIndex(index *PodMetricsIndex, info FunctionLBInfo) {

	lister := info.podLister
	metricsLister := info.metricsGetter.PodMetricses(info.namespace)
	selector := getPodLabelSelector(info.functionName)

	podList, err := lister.List(selector)
	if err != nil {
		log.Printf("List Pods Error! When updating index!\n")
		return
	}
	podMetricsList, err := metricsLister.List(context.TODO(), v12.ListOptions{LabelSelector: selector.String()})
	if err != nil {
		log.Printf("List PodMetrics Error! When updating index!\n")
		return
	}

	// join operation
	ip2Name := map[string]string{}
	name2Metrics := map[string]*PodSimpleMetrics{}
	for _, item := range podList {
		podName := item.Name
		podIP := item.Status.PodIP
		ip2Name[podIP] = podName
	}
	for _, item := range podMetricsList.Items {
		podName := item.Name
		podMetrics := getPodSimpleMetric(item)
		name2Metrics[podName] = podMetrics
	}
	
	start := time.Now()
	
	index.mu.Lock()
	defer index.mu.Unlock()
	for ip, name := range ip2Name {
		podMetrics, exists := name2Metrics[name]
		if exists {
			index.index[ip] = podMetrics
		} else { // for least cpu and memory, zero default value is legal
			index.index[ip] = &PodSimpleMetrics{}
		}
	}

	elapsed := time.Since(start)
	log.Printf("update start at %s, elapsed %s\n", start, elapsed)
}
