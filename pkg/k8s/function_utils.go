package k8s

import (
	"fmt"
	"github.com/openfaas/faas-provider/types"
	"k8s.io/apimachinery/pkg/api/errors"
	v1 "k8s.io/client-go/listers/apps/v1"
	"log"
	"strings"
)

const LBPolicyLabel = "com.openfaas.LoadBalance.policy"

// GetService returns a function/service or nil if not found
func GetService(functionNamespace string, functionName string, lister v1.DeploymentLister) (*types.FunctionStatus, error) {

	item, err := lister.Deployments(functionNamespace).
		Get(functionName)

	if err != nil {
		if errors.IsNotFound(err) {
			return nil, nil
		}

		return nil, err
	}

	if item != nil {
		function := AsFunctionStatus(*item)
		if function != nil {
			return function, nil
		}
	}

	return nil, fmt.Errorf("function: %s not found", functionName)
}

// TODO
func GetLoadBalancePolicy(functionNamespace string, functionName string, lister v1.DeploymentLister) string {
	fallback := "RoundRobin"
	functionStatus, err := GetService(functionNamespace, functionName, lister)
	if err != nil {
		log.Printf("Could not get load balance policy. Use default RoundRobin. Internal error")
		return fallback
	}
	if functionStatus == nil {
		log.Printf("Could not get load balance policy. Use default RoundRobin. Could not find function.")
		return fallback
	}

	labels := *functionStatus.Labels
	policy, exists := labels[LBPolicyLabel]
	if exists == false {
		log.Printf("Could not get load balance policy. Use default RoundRobin. No policy specified.")
		return fallback
	}
	return policy
}

func GetFunctionBackends(functionName string, namespace string) {

}

// GetFuncName parse <function_name>.<namespace>
// if no namespace return defaultNamespace
func GetFuncName(name string, defaultNamespace string) (string, string) {
	functionName := name
	namespace := getNamespace(name, defaultNamespace)

	if strings.Contains(name, ".") {
		functionName = strings.TrimSuffix(name, "."+namespace)
	}
	return functionName, namespace
}
