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

package manager

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"reflect"
	"time"

	"github.com/go-logr/logr"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/controller-runtime/pkg/cache"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/cluster"
	"sigs.k8s.io/controller-runtime/pkg/config"
	"sigs.k8s.io/controller-runtime/pkg/config/v1alpha1"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	intrec "sigs.k8s.io/controller-runtime/pkg/internal/recorder"
	"sigs.k8s.io/controller-runtime/pkg/leaderelection"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/metrics"
	"sigs.k8s.io/controller-runtime/pkg/recorder"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/webhook"
)

// Manager initializes shared dependencies such as Caches and Clients, and provides them to Runnables.
// A Manager is required to create Controllers.
type Manager interface {
	// Cluster holds a variety of methods to interact with a cluster.
	cluster.Cluster

	// Add will set requested dependencies on the component, and cause the component to be
	// started when Start is called.  Add will inject any dependencies for which the argument
	// implements the inject interface - e.g. inject.Client.
	// Depending on if a Runnable implements LeaderElectionRunnable interface, a Runnable can be run in either
	// non-leaderelection mode (always running) or leader election mode (managed by leader election if enabled).
	Add(Runnable) error

	// Elected is closed when this manager is elected leader of a group of
	// managers, either because it won a leader election or because no leader
	// election was configured.
	Elected() <-chan struct{}

	// AddMetricsExtraHandler adds an extra handler served on path to the http server that serves metrics.
	// Might be useful to register some diagnostic endpoints e.g. pprof. Note that these endpoints meant to be
	// sensitive and shouldn't be exposed publicly.
	// If the simple path -> handler mapping offered here is not enough, a new http server/listener should be added as
	// Runnable to the manager via Add method.
	AddMetricsExtraHandler(path string, handler http.Handler) error

	// AddHealthzCheck allows you to add Healthz checker
	AddHealthzCheck(name string, check healthz.Checker) error

	// AddReadyzCheck allows you to add Readyz checker
	AddReadyzCheck(name string, check healthz.Checker) error

	// Start starts all registered Controllers and blocks until the context is cancelled.
	// Returns an error if there is an error starting any controller.
	//
	// If LeaderElection is used, the binary must be exited immediately after this returns,
	// otherwise components that need leader election might continue to run after the leader
	// lock was lost.
	Start(ctx context.Context) error

	// GetWebhookServer returns a webhook.Server
	GetWebhookServer() *webhook.Server

	// GetLogger returns this manager's logger.
	GetLogger() logr.Logger

	// GetControllerOptions returns controller global configuration options.
	GetControllerOptions() v1alpha1.ControllerConfigurationSpec
}

// Options are the arguments for creating a new Manager.
type Options struct {
	// Scheme is the scheme used to resolve runtime.Objects to GroupVersionKinds / Resources.
	// Defaults to the kubernetes/client-go scheme.Scheme, but it's almost always better
	// to pass your own scheme in. See the documentation in pkg/scheme for more information.
	Scheme *runtime.Scheme

	// MapperProvider provides the rest mapper used to map go types to Kubernetes APIs
	MapperProvider func(c *rest.Config) (meta.RESTMapper, error)

	// SyncPeriod determines the minimum frequency at which watched resources are
	// reconciled. A lower period will correct entropy more quickly, but reduce
	// responsiveness to change if there are many watched resources. Change this
	// value only if you know what you are doing. Defaults to 10 hours if unset.
	// there will a 10 percent jitter between the SyncPeriod of all controllers
	// so that all controllers will not send list requests simultaneously.
	//
	// This applies to all controllers.
	//
	// A period sync happens for two reasons:
	// 1. To insure against a bug in the controller that causes an object to not
	// be requeued, when it otherwise should be requeued.
	// 2. To insure against an unknown bug in controller-runtime, or its dependencies,
	// that causes an object to not be requeued, when it otherwise should be
	// requeued, or to be removed from the queue, when it otherwise should not
	// be removed.
	//
	// If you want
	// 1. to insure against missed watch events, or
	// 2. to poll services that cannot be watched,
	// then we recommend that, instead of changing the default period, the
	// controller requeue, with a constant duration `t`, whenever the controller
	// is "done" with an object, and would otherwise not requeue it, i.e., we
	// recommend the `Reconcile` function return `reconcile.Result{RequeueAfter: t}`,
	// instead of `reconcile.Result{}`.
	SyncPeriod *time.Duration

	// Logger is the logger that should be used by this manager.
	// If none is set, it defaults to log.Log global logger.
	Logger logr.Logger

	// LeaderElection determines whether or not to use leader election when
	// starting the manager.
	LeaderElection bool

	// LeaderElectionResourceLock determines which resource lock to use for leader election,
	// defaults to "leases". Change this value only if you know what you are doing.
	//
	// If you are using `configmaps`/`endpoints` resource lock and want to migrate to "leases",
	// you might do so by migrating to the respective multilock first ("configmapsleases" or "endpointsleases"),
	// which will acquire a leader lock on both resources.
	// After all your users have migrated to the multilock, you can go ahead and migrate to "leases".
	// Please also keep in mind, that users might skip versions of your controller.
	//
	// Note: before controller-runtime version v0.7, it was set to "configmaps".
	// And from v0.7 to v0.11, the default was "configmapsleases", which was
	// used to migrate from configmaps to leases.
	// Since the default was "configmapsleases" for over a year, spanning five minor releases,
	// any actively maintained operators are very likely to have a released version that uses
	// "configmapsleases". Therefore defaulting to "leases" should be safe since v0.12.
	//
	// So, what do you have to do when you are updating your controller-runtime dependency
	// from a lower version to v0.12 or newer?
	// - If your operator matches at least one of these conditions:
	//   - the LeaderElectionResourceLock in your operator has already been explicitly set to "leases"
	//   - the old controller-runtime version is between v0.7.0 and v0.11.x and the
	//     LeaderElectionResourceLock wasn't set or was set to "leases"/"configmapsleases"/"endpointsleases"
	//   feel free to update controller-runtime to v0.12 or newer.
	// - Otherwise, you may have to take these steps:
	//   1. update controller-runtime to v0.12 or newer in your go.mod
	//   2. set LeaderElectionResourceLock to "configmapsleases" (or "endpointsleases")
	//   3. package your operator and upgrade it in all your clusters
	//   4. only if you have finished 3, you can remove the LeaderElectionResourceLock to use the default "leases"
	// Otherwise, your operator might end up with multiple running instances that
	// each acquired leadership through different resource locks during upgrades and thus
	// act on the same resources concurrently.
	LeaderElectionResourceLock string

	// LeaderElectionNamespace determines the namespace in which the leader
	// election resource will be created.
	LeaderElectionNamespace string

	// LeaderElectionID determines the name of the resource that leader election
	// will use for holding the leader lock.
	LeaderElectionID string

	// LeaderElectionConfig can be specified to override the default configuration
	// that is used to build the leader election client.
	LeaderElectionConfig *rest.Config

	// LeaderElectionReleaseOnCancel defines if the leader should step down voluntarily
	// when the Manager ends. This requires the binary to immediately end when the
	// Manager is stopped, otherwise this setting is unsafe. Setting this significantly
	// speeds up voluntary leader transitions as the new leader doesn't have to wait
	// LeaseDuration time first.
	LeaderElectionReleaseOnCancel bool

	// LeaseDuration is the duration that non-leader candidates will
	// wait to force acquire leadership. This is measured against time of
	// last observed ack. Default is 15 seconds.
	LeaseDuration *time.Duration
	// RenewDeadline is the duration that the acting controlplane will retry
	// refreshing leadership before giving up. Default is 10 seconds.
	RenewDeadline *time.Duration
	// RetryPeriod is the duration the LeaderElector clients should wait
	// between tries of actions. Default is 2 seconds.
	RetryPeriod *time.Duration

	// Namespace, if specified, restricts the manager's cache to watch objects in
	// the desired namespace. Defaults to all namespaces.
	//
	// Note: If a namespace is specified, controllers can still Watch for a
	// cluster-scoped resource (e.g Node). For namespaced resources, the cache
	// will only hold objects from the desired namespace.
	Namespace string

	// MetricsBindAddress is the TCP address that the controller should bind to
	// for serving prometheus metrics.
	// It can be set to "0" to disable the metrics serving.
	MetricsBindAddress string

	// HealthProbeBindAddress is the TCP address that the controller should bind to
	// for serving health probes
	HealthProbeBindAddress string

	// Readiness probe endpoint name, defaults to "readyz"
	ReadinessEndpointName string

	// Liveness probe endpoint name, defaults to "healthz"
	LivenessEndpointName string

	// Port is the port that the webhook server serves at.
	// It is used to set webhook.Server.Port if WebhookServer is not set.
	Port int
	// Host is the hostname that the webhook server binds to.
	// It is used to set webhook.Server.Host if WebhookServer is not set.
	Host string

	// CertDir is the directory that contains the server key and certificate.
	// If not set, webhook server would look up the server key and certificate in
	// {TempDir}/k8s-webhook-server/serving-certs. The server key and certificate
	// must be named tls.key and tls.crt, respectively.
	// It is used to set webhook.Server.CertDir if WebhookServer is not set.
	CertDir string

	// WebhookServer is an externally configured webhook.Server. By default,
	// a Manager will create a default server using Port, Host, and CertDir;
	// if this is set, the Manager will use this server instead.
	WebhookServer *webhook.Server

	// Functions to allow for a user to customize values that will be injected.

	// NewCache is the function that will create the cache to be used
	// by the manager. If not set this will use the default new cache function.
	NewCache cache.NewCacheFunc

	// NewClient is the func that creates the client to be used by the manager.
	// If not set this will create the default DelegatingClient that will
	// use the cache for reads and the client for writes.
	NewClient cluster.NewClientFunc

	// BaseContext is the function that provides Context values to Runnables
	// managed by the Manager. If a BaseContext function isn't provided, Runnables
	// will receive a new Background Context instead.
	BaseContext BaseContextFunc

	// ClientDisableCacheFor tells the client that, if any cache is used, to bypass it
	// for the given objects.
	ClientDisableCacheFor []client.Object

	// DryRunClient specifies whether the client should be configured to enforce
	// dryRun mode.
	DryRunClient bool

	// EventBroadcaster records Events emitted by the manager and sends them to the Kubernetes API
	// Use this to customize the event correlator and spam filter
	//
	// Deprecated: using this may cause goroutine leaks if the lifetime of your manager or controllers
	// is shorter than the lifetime of your process.
	EventBroadcaster record.EventBroadcaster

	// GracefulShutdownTimeout is the duration given to runnable to stop before the manager actually returns on stop.
	// To disable graceful shutdown, set to time.Duration(0)
	// To use graceful shutdown without timeout, set to a negative duration, e.G. time.Duration(-1)
	// The graceful shutdown is skipped for safety reasons in case the leader election lease is lost.
	GracefulShutdownTimeout *time.Duration

	// Controller contains global configuration options for controllers
	// registered within this manager.
	// +optional
	Controller v1alpha1.ControllerConfigurationSpec

	// makeBroadcaster allows deferring the creation of the broadcaster to
	// avoid leaking goroutines if we never call Start on this manager.  It also
	// returns whether or not this is a "owned" broadcaster, and as such should be
	// stopped with the manager.
	makeBroadcaster intrec.EventBroadcasterProducer

	// Dependency injection for testing
	newRecorderProvider    func(config *rest.Config, scheme *runtime.Scheme, logger logr.Logger, makeBroadcaster intrec.EventBroadcasterProducer) (*intrec.Provider, error)
	newResourceLock        func(config *rest.Config, recorderProvider recorder.Provider, options leaderelection.Options) (resourcelock.Interface, error)
	newMetricsListener     func(addr string) (net.Listener, error)
	newHealthProbeListener func(addr string) (net.Listener, error)
}

// BaseContextFunc is a function used to provide a base Context to Runnables
// managed by a Manager.
type BaseContextFunc func() context.Context

// Runnable allows a component to be started.
// It's very important that Start blocks until
// it's done running.
type Runnable interface {
	// Start starts running the component.  The component will stop running
	// when the context is closed. Start blocks until the context is closed or
	// an error occurs.
	Start(context.Context) error
}

// RunnableFunc implements Runnable using a function.
// It's very important that the given function block
// until it's done running.
type RunnableFunc func(context.Context) error

// Start implements Runnable.
func (r RunnableFunc) Start(ctx context.Context) error {
	return r(ctx)
}

// LeaderElectionRunnable knows if a Runnable needs to be run in the leader election mode.
type LeaderElectionRunnable interface {
	// NeedLeaderElection returns true if the Runnable needs to be run in the leader election mode.
	// e.g. controllers need to be run in leader election mode, while webhook server doesn't.
	NeedLeaderElection() bool
}

// New returns a new Manager for creating Controllers.
func New(config *rest.Config, options Options) (Manager, error) {
	// Set default values for options fields
	options = setOptionsDefaults(options)

	cluster, err := cluster.New(config, func(clusterOptions *cluster.Options) {
		clusterOptions.Scheme = options.Scheme
		clusterOptions.MapperProvider = options.MapperProvider
		clusterOptions.Logger = options.Logger
		clusterOptions.SyncPeriod = options.SyncPeriod
		clusterOptions.Namespace = options.Namespace
		clusterOptions.NewCache = options.NewCache
		clusterOptions.NewClient = options.NewClient
		clusterOptions.ClientDisableCacheFor = options.ClientDisableCacheFor
		clusterOptions.DryRunClient = options.DryRunClient
		clusterOptions.EventBroadcaster = options.EventBroadcaster //nolint:staticcheck
	})
	if err != nil {
		return nil, err
	}

	// Create the recorder provider to inject event recorders for the components.
	// TODO(directxman12): the log for the event provider should have a context (name, tags, etc) specific
	// to the particular controller that it's being injected into, rather than a generic one like is here.
	recorderProvider, err := options.newRecorderProvider(config, cluster.GetScheme(), options.Logger.WithName("events"), options.makeBroadcaster)
	if err != nil {
		return nil, err
	}

	// Create the resource lock to enable leader election)
	var leaderConfig *rest.Config
	var leaderRecorderProvider *intrec.Provider

	if options.LeaderElectionConfig == nil {
		leaderConfig = rest.CopyConfig(config)
		leaderRecorderProvider = recorderProvider
	} else {
		leaderConfig = rest.CopyConfig(options.LeaderElectionConfig)
		leaderRecorderProvider, err = options.newRecorderProvider(leaderConfig, cluster.GetScheme(), options.Logger.WithName("events"), options.makeBroadcaster)
		if err != nil {
			return nil, err
		}
	}

	resourceLock, err := options.newResourceLock(leaderConfig, leaderRecorderProvider, leaderelection.Options{
		LeaderElection:             options.LeaderElection,
		LeaderElectionResourceLock: options.LeaderElectionResourceLock,
		LeaderElectionID:           options.LeaderElectionID,
		LeaderElectionNamespace:    options.LeaderElectionNamespace,
	})
	if err != nil {
		return nil, err
	}

	// Create the metrics listener. This will throw an error if the metrics bind
	// address is invalid or already in use.
	metricsListener, err := options.newMetricsListener(options.MetricsBindAddress)
	if err != nil {
		return nil, err
	}

	// By default we have no extra endpoints to expose on metrics http server.
	metricsExtraHandlers := make(map[string]http.Handler)

	// Create health probes listener. This will throw an error if the bind
	// address is invalid or already in use.
	healthProbeListener, err := options.newHealthProbeListener(options.HealthProbeBindAddress)
	if err != nil {
		return nil, err
	}

	errChan := make(chan error)
	runnables := newRunnables(options.BaseContext, errChan)

	return &controllerManager{
		stopProcedureEngaged:          pointer.Int64(0),
		cluster:                       cluster,
		runnables:                     runnables,
		errChan:                       errChan,
		recorderProvider:              recorderProvider,
		resourceLock:                  resourceLock,
		metricsListener:               metricsListener,
		metricsExtraHandlers:          metricsExtraHandlers,
		controllerOptions:             options.Controller,
		logger:                        options.Logger,
		elected:                       make(chan struct{}),
		port:                          options.Port,
		host:                          options.Host,
		certDir:                       options.CertDir,
		webhookServer:                 options.WebhookServer,
		leaseDuration:                 *options.LeaseDuration,
		renewDeadline:                 *options.RenewDeadline,
		retryPeriod:                   *options.RetryPeriod,
		healthProbeListener:           healthProbeListener,
		readinessEndpointName:         options.ReadinessEndpointName,
		livenessEndpointName:          options.LivenessEndpointName,
		gracefulShutdownTimeout:       *options.GracefulShutdownTimeout,
		internalProceduresStop:        make(chan struct{}),
		leaderElectionStopped:         make(chan struct{}),
		leaderElectionReleaseOnCancel: options.LeaderElectionReleaseOnCancel,
	}, nil
}

// AndFrom will use a supplied type and convert to Options
// any options already set on Options will be ignored, this is used to allow
// cli flags to override anything specified in the config file.
func (o Options) AndFrom(loader config.ControllerManagerConfiguration) (Options, error) {
	if inj, wantsScheme := loader.(inject.Scheme); wantsScheme {
		err := inj.InjectScheme(o.Scheme)
		if err != nil {
			return o, err
		}
	}

	newObj, err := loader.Complete()
	if err != nil {
		return o, err
	}

	o = o.setLeaderElectionConfig(newObj)

	if o.SyncPeriod == nil && newObj.SyncPeriod != nil {
		o.SyncPeriod = &newObj.SyncPeriod.Duration
	}

	if o.Namespace == "" && newObj.CacheNamespace != "" {
		o.Namespace = newObj.CacheNamespace
	}

	if o.MetricsBindAddress == "" && newObj.Metrics.BindAddress != "" {
		o.MetricsBindAddress = newObj.Metrics.BindAddress
	}

	if o.HealthProbeBindAddress == "" && newObj.Health.HealthProbeBindAddress != "" {
		o.HealthProbeBindAddress = newObj.Health.HealthProbeBindAddress
	}

	if o.ReadinessEndpointName == "" && newObj.Health.ReadinessEndpointName != "" {
		o.ReadinessEndpointName = newObj.Health.ReadinessEndpointName
	}

	if o.LivenessEndpointName == "" && newObj.Health.LivenessEndpointName != "" {
		o.LivenessEndpointName = newObj.Health.LivenessEndpointName
	}

	if o.Port == 0 && newObj.Webhook.Port != nil {
		o.Port = *newObj.Webhook.Port
	}

	if o.Host == "" && newObj.Webhook.Host != "" {
		o.Host = newObj.Webhook.Host
	}

	if o.CertDir == "" && newObj.Webhook.CertDir != "" {
		o.CertDir = newObj.Webhook.CertDir
	}

	if newObj.Controller != nil {
		if o.Controller.CacheSyncTimeout == nil && newObj.Controller.CacheSyncTimeout != nil {
			o.Controller.CacheSyncTimeout = newObj.Controller.CacheSyncTimeout
		}

		if len(o.Controller.GroupKindConcurrency) == 0 && len(newObj.Controller.GroupKindConcurrency) > 0 {
			o.Controller.GroupKindConcurrency = newObj.Controller.GroupKindConcurrency
		}
	}

	return o, nil
}

// AndFromOrDie will use options.AndFrom() and will panic if there are errors.
func (o Options) AndFromOrDie(loader config.ControllerManagerConfiguration) Options {
	o, err := o.AndFrom(loader)
	if err != nil {
		panic(fmt.Sprintf("could not parse config file: %v", err))
	}
	return o
}

func (o Options) setLeaderElectionConfig(obj v1alpha1.ControllerManagerConfigurationSpec) Options {
	if obj.LeaderElection == nil {
		// The source does not have any configuration; noop
		return o
	}

	if !o.LeaderElection && obj.LeaderElection.LeaderElect != nil {
		o.LeaderElection = *obj.LeaderElection.LeaderElect
	}

	if o.LeaderElectionResourceLock == "" && obj.LeaderElection.ResourceLock != "" {
		o.LeaderElectionResourceLock = obj.LeaderElection.ResourceLock
	}

	if o.LeaderElectionNamespace == "" && obj.LeaderElection.ResourceNamespace != "" {
		o.LeaderElectionNamespace = obj.LeaderElection.ResourceNamespace
	}

	if o.LeaderElectionID == "" && obj.LeaderElection.ResourceName != "" {
		o.LeaderElectionID = obj.LeaderElection.ResourceName
	}

	if o.LeaseDuration == nil && !reflect.DeepEqual(obj.LeaderElection.LeaseDuration, metav1.Duration{}) {
		o.LeaseDuration = &obj.LeaderElection.LeaseDuration.Duration
	}

	if o.RenewDeadline == nil && !reflect.DeepEqual(obj.LeaderElection.RenewDeadline, metav1.Duration{}) {
		o.RenewDeadline = &obj.LeaderElection.RenewDeadline.Duration
	}

	if o.RetryPeriod == nil && !reflect.DeepEqual(obj.LeaderElection.RetryPeriod, metav1.Duration{}) {
		o.RetryPeriod = &obj.LeaderElection.RetryPeriod.Duration
	}

	return o
}

// defaultHealthProbeListener creates the default health probes listener bound to the given address.
func defaultHealthProbeListener(addr string) (net.Listener, error) {
	if addr == "" || addr == "0" {
		return nil, nil
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("error listening on %s: %w", addr, err)
	}
	return ln, nil
}

// defaultBaseContext is used as the BaseContext value in Options if one
// has not already been set.
func defaultBaseContext() context.Context {
	return context.Background()
}

// setOptionsDefaults set default values for Options fields.
func setOptionsDefaults(options Options) Options {
	// Allow newResourceLock to be mocked
	if options.newResourceLock == nil {
		options.newResourceLock = leaderelection.NewResourceLock
	}

	// Allow newRecorderProvider to be mocked
	if options.newRecorderProvider == nil {
		options.newRecorderProvider = intrec.NewProvider
	}

	// This is duplicated with pkg/cluster, we need it here
	// for the leader election and there to provide the user with
	// an EventBroadcaster
	if options.EventBroadcaster == nil {
		// defer initialization to avoid leaking by default
		options.makeBroadcaster = func() (record.EventBroadcaster, bool) {
			return record.NewBroadcaster(), true
		}
	} else {
		options.makeBroadcaster = func() (record.EventBroadcaster, bool) {
			return options.EventBroadcaster, false
		}
	}

	if options.newMetricsListener == nil {
		options.newMetricsListener = metrics.NewListener
	}
	leaseDuration, renewDeadline, retryPeriod := defaultLeaseDuration, defaultRenewDeadline, defaultRetryPeriod
	if options.LeaseDuration == nil {
		options.LeaseDuration = &leaseDuration
	}

	if options.RenewDeadline == nil {
		options.RenewDeadline = &renewDeadline
	}

	if options.RetryPeriod == nil {
		options.RetryPeriod = &retryPeriod
	}

	if options.ReadinessEndpointName == "" {
		options.ReadinessEndpointName = defaultReadinessEndpoint
	}

	if options.LivenessEndpointName == "" {
		options.LivenessEndpointName = defaultLivenessEndpoint
	}

	if options.newHealthProbeListener == nil {
		options.newHealthProbeListener = defaultHealthProbeListener
	}

	if options.GracefulShutdownTimeout == nil {
		gracefulShutdownTimeout := defaultGracefulShutdownPeriod
		options.GracefulShutdownTimeout = &gracefulShutdownTimeout
	}

	if options.Logger.GetSink() == nil {
		options.Logger = log.Log
	}

	if options.BaseContext == nil {
		options.BaseContext = defaultBaseContext
	}

	return options
}
