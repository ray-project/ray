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

package controllerruntime

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/builder"
	"sigs.k8s.io/controller-runtime/pkg/client/config"
	cfg "sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/manager"
	"sigs.k8s.io/controller-runtime/pkg/manager/signals"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"
	"sigs.k8s.io/controller-runtime/pkg/scheme"
)

// Builder builds an Application ControllerManagedBy (e.g. Operator) and returns a manager.Manager to start it.
type Builder = builder.Builder

// Request contains the information necessary to reconcile a Kubernetes object.  This includes the
// information to uniquely identify the object - its Name and Namespace.  It does NOT contain information about
// any specific Event or the object contents itself.
type Request = reconcile.Request

// Result contains the result of a Reconciler invocation.
type Result = reconcile.Result

// Manager initializes shared dependencies such as Caches and Clients, and provides them to Runnables.
// A Manager is required to create Controllers.
type Manager = manager.Manager

// Options are the arguments for creating a new Manager.
type Options = manager.Options

// SchemeBuilder builds a new Scheme for mapping go types to Kubernetes GroupVersionKinds.
type SchemeBuilder = scheme.Builder

// GroupVersion contains the "group" and the "version", which uniquely identifies the API.
type GroupVersion = schema.GroupVersion

// GroupResource specifies a Group and a Resource, but does not force a version.  This is useful for identifying
// concepts during lookup stages without having partially valid types.
type GroupResource = schema.GroupResource

// TypeMeta describes an individual object in an API response or request
// with strings representing the type of the object and its API schema version.
// Structures that are versioned or persisted should inline TypeMeta.
//
// +k8s:deepcopy-gen=false
type TypeMeta = metav1.TypeMeta

// ObjectMeta is metadata that all persisted resources must have, which includes all objects
// users must create.
type ObjectMeta = metav1.ObjectMeta

var (
	// GetConfigOrDie creates a *rest.Config for talking to a Kubernetes apiserver.
	// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
	// in cluster and use the cluster provided kubeconfig.
	//
	// Will log an error and exit if there is an error creating the rest.Config.
	GetConfigOrDie = config.GetConfigOrDie

	// GetConfig creates a *rest.Config for talking to a Kubernetes apiserver.
	// If --kubeconfig is set, will use the kubeconfig file at that location.  Otherwise will assume running
	// in cluster and use the cluster provided kubeconfig.
	//
	// Config precedence
	//
	// * --kubeconfig flag pointing at a file
	//
	// * KUBECONFIG environment variable pointing at a file
	//
	// * In-cluster config if running in cluster
	//
	// * $HOME/.kube/config if exists.
	GetConfig = config.GetConfig

	// ConfigFile returns the cfg.File function for deferred config file loading,
	// this is passed into Options{}.From() to populate the Options fields for
	// the manager.
	ConfigFile = cfg.File

	// NewControllerManagedBy returns a new controller builder that will be started by the provided Manager.
	NewControllerManagedBy = builder.ControllerManagedBy

	// NewWebhookManagedBy returns a new webhook builder that will be started by the provided Manager.
	NewWebhookManagedBy = builder.WebhookManagedBy

	// NewManager returns a new Manager for creating Controllers.
	NewManager = manager.New

	// CreateOrUpdate creates or updates the given object obj in the Kubernetes
	// cluster. The object's desired state should be reconciled with the existing
	// state using the passed in ReconcileFn. obj must be a struct pointer so that
	// obj can be updated with the content returned by the Server.
	//
	// It returns the executed operation and an error.
	CreateOrUpdate = controllerutil.CreateOrUpdate

	// SetControllerReference sets owner as a Controller OwnerReference on owned.
	// This is used for garbage collection of the owned object and for
	// reconciling the owner object on changes to owned (with a Watch + EnqueueRequestForOwner).
	// Since only one OwnerReference can be a controller, it returns an error if
	// there is another OwnerReference with Controller flag set.
	SetControllerReference = controllerutil.SetControllerReference

	// SetupSignalHandler registered for SIGTERM and SIGINT. A stop channel is returned
	// which is closed on one of these signals. If a second signal is caught, the program
	// is terminated with exit code 1.
	SetupSignalHandler = signals.SetupSignalHandler

	// Log is the base logger used by controller-runtime.  It delegates
	// to another logr.Logger.  You *must* call SetLogger to
	// get any actual logging.
	Log = log.Log

	// LoggerFrom returns a logger with predefined values from a context.Context.
	// The logger, when used with controllers, can be expected to contain basic information about the object
	// that's being reconciled like:
	// - `reconciler group` and `reconciler kind` coming from the For(...) object passed in when building a controller.
	// - `name` and `namespace` injected from the reconciliation request.
	//
	// This is meant to be used with the context supplied in a struct that satisfies the Reconciler interface.
	LoggerFrom = log.FromContext

	// LoggerInto takes a context and sets the logger as one of its keys.
	//
	// This is meant to be used in reconcilers to enrich the logger within a context with additional values.
	LoggerInto = log.IntoContext

	// SetLogger sets a concrete logging implementation for all deferred Loggers.
	SetLogger = log.SetLogger
)
