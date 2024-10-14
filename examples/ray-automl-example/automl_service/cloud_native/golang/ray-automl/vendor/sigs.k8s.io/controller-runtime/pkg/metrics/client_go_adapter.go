/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package metrics

import (
	"context"
	"net/url"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	clientmetrics "k8s.io/client-go/tools/metrics"
)

// this file contains setup logic to initialize the myriad of places
// that client-go registers metrics.  We copy the names and formats
// from Kubernetes so that we match the core controllers.

// Metrics subsystem and all of the keys used by the rest client.
const (
	RestClientSubsystem = "rest_client"
	LatencyKey          = "request_latency_seconds"
	ResultKey           = "requests_total"
)

var (
	// client metrics.

	// RequestLatency reports the request latency in seconds per verb/URL.
	// Deprecated: This metric is deprecated for removal in a future release: using the URL as a
	// dimension results in cardinality explosion for some consumers. It was deprecated upstream
	// in k8s v1.14 and hidden in v1.17 via https://github.com/kubernetes/kubernetes/pull/83836.
	// It is not registered by default. To register:
	//	import (
	//		clientmetrics "k8s.io/client-go/tools/metrics"
	//		clmetrics "sigs.k8s.io/controller-runtime/metrics"
	//	)
	//
	//	func init() {
	//		clmetrics.Registry.MustRegister(clmetrics.RequestLatency)
	//		clientmetrics.Register(clientmetrics.RegisterOpts{
	//			RequestLatency: clmetrics.LatencyAdapter
	//		})
	//	}
	RequestLatency = prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Subsystem: RestClientSubsystem,
		Name:      LatencyKey,
		Help:      "Request latency in seconds. Broken down by verb and URL.",
		Buckets:   prometheus.ExponentialBuckets(0.001, 2, 10),
	}, []string{"verb", "url"})

	requestResult = prometheus.NewCounterVec(prometheus.CounterOpts{
		Subsystem: RestClientSubsystem,
		Name:      ResultKey,
		Help:      "Number of HTTP requests, partitioned by status code, method, and host.",
	}, []string{"code", "method", "host"})
)

func init() {
	registerClientMetrics()
}

// registerClientMetrics sets up the client latency metrics from client-go.
func registerClientMetrics() {
	// register the metrics with our registry
	Registry.MustRegister(requestResult)

	// register the metrics with client-go
	clientmetrics.Register(clientmetrics.RegisterOpts{
		RequestResult: &resultAdapter{metric: requestResult},
	})
}

// this section contains adapters, implementations, and other sundry organic, artisanally
// hand-crafted syntax trees required to convince client-go that it actually wants to let
// someone use its metrics.

// Client metrics adapters (method #1 for client-go metrics),
// copied (more-or-less directly) from k8s.io/kubernetes setup code
// (which isn't anywhere in an easily-importable place).

// LatencyAdapter implements LatencyMetric.
type LatencyAdapter struct {
	metric *prometheus.HistogramVec
}

// Observe increments the request latency metric for the given verb/URL.
func (l *LatencyAdapter) Observe(_ context.Context, verb string, u url.URL, latency time.Duration) {
	l.metric.WithLabelValues(verb, u.String()).Observe(latency.Seconds())
}

type resultAdapter struct {
	metric *prometheus.CounterVec
}

func (r *resultAdapter) Increment(_ context.Context, code, method, host string) {
	r.metric.WithLabelValues(code, method, host).Inc()
}
