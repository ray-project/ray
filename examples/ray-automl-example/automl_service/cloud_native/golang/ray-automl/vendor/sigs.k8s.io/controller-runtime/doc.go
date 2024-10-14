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

// Package controllerruntime provides tools to construct Kubernetes-style
// controllers that manipulate both Kubernetes CRDs and aggregated/built-in
// Kubernetes APIs.
//
// It defines easy helpers for the common use cases when building CRDs, built
// on top of customizable layers of abstraction.  Common cases should be easy,
// and uncommon cases should be possible.  In general, controller-runtime tries
// to guide users towards Kubernetes controller best-practices.
//
// # Getting Started
//
// The main entrypoint for controller-runtime is this root package, which
// contains all of the common types needed to get started building controllers:
//
//	import (
//		ctrl "sigs.k8s.io/controller-runtime"
//	)
//
// The examples in this package walk through a basic controller setup.  The
// kubebuilder book (https://book.kubebuilder.io) has some more in-depth
// walkthroughs.
//
// controller-runtime favors structs with sane defaults over constructors, so
// it's fairly common to see structs being used directly in controller-runtime.
//
// # Organization
//
// A brief-ish walkthrough of the layout of this library can be found below. Each
// package contains more information about how to use it.
//
// Frequently asked questions about using controller-runtime and designing
// controllers can be found at
// https://github.com/kubernetes-sigs/controller-runtime/blob/master/FAQ.md.
//
// # Managers
//
// Every controller and webhook is ultimately run by a Manager (pkg/manager). A
// manager is responsible for running controllers and webhooks, and setting up
// common dependencies (pkg/runtime/inject), like shared caches and clients, as
// well as managing leader election (pkg/leaderelection).  Managers are
// generally configured to gracefully shut down controllers on pod termination
// by wiring up a signal handler (pkg/manager/signals).
//
// # Controllers
//
// Controllers (pkg/controller) use events (pkg/event) to eventually trigger
// reconcile requests.  They may be constructed manually, but are often
// constructed with a Builder (pkg/builder), which eases the wiring of event
// sources (pkg/source), like Kubernetes API object changes, to event handlers
// (pkg/handler), like "enqueue a reconcile request for the object owner".
// Predicates (pkg/predicate) can be used to filter which events actually
// trigger reconciles.  There are pre-written utilities for the common cases, and
// interfaces and helpers for advanced cases.
//
// # Reconcilers
//
// Controller logic is implemented in terms of Reconcilers (pkg/reconcile).  A
// Reconciler implements a function which takes a reconcile Request containing
// the name and namespace of the object to reconcile, reconciles the object,
// and returns a Response or an error indicating whether to requeue for a
// second round of processing.
//
// # Clients and Caches
//
// Reconcilers use Clients (pkg/client) to access API objects.  The default
// client provided by the manager reads from a local shared cache (pkg/cache)
// and writes directly to the API server, but clients can be constructed that
// only talk to the API server, without a cache.  The Cache will auto-populate
// with watched objects, as well as when other structured objects are
// requested. The default split client does not promise to invalidate the cache
// during writes (nor does it promise sequential create/get coherence), and code
// should not assume a get immediately following a create/update will return
// the updated resource. Caches may also have indexes, which can be created via
// a FieldIndexer (pkg/client) obtained from the manager.  Indexes can used to
// quickly and easily look up all objects with certain fields set.  Reconcilers
// may retrieve event recorders (pkg/recorder) to emit events using the
// manager.
//
// # Schemes
//
// Clients, Caches, and many other things in Kubernetes use Schemes
// (pkg/scheme) to associate Go types to Kubernetes API Kinds
// (Group-Version-Kinds, to be specific).
//
// # Webhooks
//
// Similarly, webhooks (pkg/webhook/admission) may be implemented directly, but
// are often constructed using a builder (pkg/webhook/admission/builder).  They
// are run via a server (pkg/webhook) which is managed by a Manager.
//
// # Logging and Metrics
//
// Logging (pkg/log) in controller-runtime is done via structured logs, using a
// log set of interfaces called logr
// (https://pkg.go.dev/github.com/go-logr/logr).  While controller-runtime
// provides easy setup for using Zap (https://go.uber.org/zap, pkg/log/zap),
// you can provide any implementation of logr as the base logger for
// controller-runtime.
//
// Metrics (pkg/metrics) provided by controller-runtime are registered into a
// controller-runtime-specific Prometheus metrics registry.  The manager can
// serve these by an HTTP endpoint, and additional metrics may be registered to
// this Registry as normal.
//
// # Testing
//
// You can easily build integration and unit tests for your controllers and
// webhooks using the test Environment (pkg/envtest).  This will automatically
// stand up a copy of etcd and kube-apiserver, and provide the correct options
// to connect to the API server.  It's designed to work well with the Ginkgo
// testing framework, but should work with any testing setup.
package controllerruntime
