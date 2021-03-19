package handlers

import (
	"fmt"
	"github.com/gorilla/mux"
	"github.com/openfaas/faas-netes/pkg/k8s"
	"golang.org/x/time/rate"
	v1 "k8s.io/client-go/listers/apps/v1"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
	"time"
)

const RateQPSLabel = "com.openfaas.rate.qps"

type BucketService interface {
	GetBucket(functionName string, lookupNamespace string) (*rate.Limiter, error)
}

// MakeRateLimitedHandler make a layer of rate limited handler for function invoke api
func MakeRateLimitedHandler(next http.HandlerFunc, service BucketService, defaultNamespace string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		vars := mux.Vars(r)
		functionName := vars["name"]

		// In function invoke api, the namespace is specified by <function_name>.<namespace>
		var namespace string
		functionName, namespace = k8s.GetFuncName(functionName, defaultNamespace)

		bucket, err := service.GetBucket(functionName, namespace)
		if err != nil {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(fmt.Sprintf("Unable to get rate limiter for %s.%s", functionName, namespace)))
			return
		}
		// log.Printf("Rate qps: %f", float64(bucket.Limit()))

		if bucket.Allow() { // transfer to next handler func
			next.ServeHTTP(w, r)
			return
		} else {
			w.WriteHeader(http.StatusTooManyRequests)
			return
		}
	}
}

// TODO: add a clean up goroutine?
type FunctionBucketServiceImpl struct {
	cache map[string]*rate.Limiter
	// TODO: use read write lock?
	mu     sync.Mutex
	lister v1.DeploymentLister
}

func NewFunctionBucketService(lister v1.DeploymentLister) BucketService {
	s := FunctionBucketServiceImpl{
		cache:  make(map[string]*rate.Limiter),
		mu:     sync.Mutex{},
		lister: lister,
	}
	return &s
}

func (s *FunctionBucketServiceImpl) GetBucket(functionName string, namespace string) (*rate.Limiter, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// function name must not contain '#' as a legal dns entry
	key := namespace + "#" + functionName
	val, hit := s.cache[key]
	if hit {
		return val, nil
	}

	var err error
	val, err = computeFunctionBucket(functionName, namespace, s.lister)
	if err != nil {
		return nil, err
	}

	s.cache[key] = val
	return val, nil
}

func computeFunctionBucket(functionName string, namespace string, lister v1.DeploymentLister) (*rate.Limiter, error) {
	// TODO: add config for default value?
	defaultQPSRate := 20.0
	defaultBurst := 20
	fallback := rate.NewLimiter(rate.Limit(defaultQPSRate), defaultBurst)

	start := time.Now()

	function, err := getService(namespace, functionName, lister)
	if err != nil {
		log.Printf("Unable to fetch service: %s %s\n", functionName, namespace)
		log.Printf(err.Error())
		return nil, err
	}

	if function == nil {
		log.Printf("function not found")
		return nil, fmt.Errorf("function not found")
	}

	delay := time.Since(start)
	log.Printf("Ratelimiter query for %s.%s, %dms\n", functionName, namespace, delay.Milliseconds())

	labels := *function.Labels
	qps, exists := labels[RateQPSLabel]
	if !exists {
		return fallback, nil
	}

	val, e := strconv.ParseFloat(qps, 64)
	if e != nil {
		return fallback, nil
	}

	// default burst/bucket capacity is set accordingly
	return rate.NewLimiter(rate.Limit(val), int(math.Ceil(val))), nil
}
