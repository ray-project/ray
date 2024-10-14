/*
Copyright 2017 The Kubernetes Authors.

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

// Package envtest provides libraries for integration testing by starting a local control plane
//
// Control plane binaries (etcd and kube-apiserver) are loaded by default from
// /usr/local/kubebuilder/bin.  This can be overridden by setting the
// KUBEBUILDER_ASSETS environment variable, or by directly creating a
// ControlPlane for the Environment to use.
//
// Environment can also be configured to work with an existing cluster, and
// simply load CRDs and provide client configuration.
package envtest
