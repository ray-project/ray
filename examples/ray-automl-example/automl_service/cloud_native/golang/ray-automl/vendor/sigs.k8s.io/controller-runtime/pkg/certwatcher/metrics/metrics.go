/*
Copyright 2022 The Kubernetes Authors.

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
	"github.com/prometheus/client_golang/prometheus"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
)

var (
	// ReadCertificateTotal is a prometheus counter metrics which holds the total
	// number of certificate reads.
	ReadCertificateTotal = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "certwatcher_read_certificate_total",
		Help: "Total number of certificate reads",
	})

	// ReadCertificateErrors is a prometheus counter metrics which holds the total
	// number of errors from certificate read.
	ReadCertificateErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "certwatcher_read_certificate_errors_total",
		Help: "Total number of certificate read errors",
	})
)

func init() {
	metrics.Registry.MustRegister(
		ReadCertificateTotal,
		ReadCertificateErrors,
	)
}
