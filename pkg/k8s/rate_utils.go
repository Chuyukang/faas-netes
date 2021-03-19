package k8s

import "strings"

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
