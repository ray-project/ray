/*
Copyright 2016 The Kubernetes Authors.

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

package envtest

import (
	"fmt"
	"os"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"

	"sigs.k8s.io/controller-runtime/pkg/client/config"
	logf "sigs.k8s.io/controller-runtime/pkg/internal/log"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/controlplane"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/process"
)

var log = logf.RuntimeLog.WithName("test-env")

/*
It's possible to override some defaults, by setting the following environment variables:
* USE_EXISTING_CLUSTER (boolean): if set to true, envtest will use an existing cluster
* TEST_ASSET_KUBE_APISERVER (string): path to the api-server binary to use
* TEST_ASSET_ETCD (string): path to the etcd binary to use
* TEST_ASSET_KUBECTL (string): path to the kubectl binary to use
* KUBEBUILDER_ASSETS (string): directory containing the binaries to use (api-server, etcd and kubectl). Defaults to /usr/local/kubebuilder/bin.
* KUBEBUILDER_CONTROLPLANE_START_TIMEOUT (string supported by time.ParseDuration): timeout for test control plane to start. Defaults to 20s.
* KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT (string supported by time.ParseDuration): timeout for test control plane to start. Defaults to 20s.
* KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT (boolean): if set to true, the control plane's stdout and stderr are attached to os.Stdout and os.Stderr
*/
const (
	envUseExistingCluster = "USE_EXISTING_CLUSTER"
	envStartTimeout       = "KUBEBUILDER_CONTROLPLANE_START_TIMEOUT"
	envStopTimeout        = "KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT"
	envAttachOutput       = "KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT"
	StartTimeout          = 60
	StopTimeout           = 60

	defaultKubebuilderControlPlaneStartTimeout = 20 * time.Second
	defaultKubebuilderControlPlaneStopTimeout  = 20 * time.Second
)

// internal types we expose as part of our public API.
type (
	// ControlPlane is the re-exported ControlPlane type from the internal testing package.
	ControlPlane = controlplane.ControlPlane

	// APIServer is the re-exported APIServer from the internal testing package.
	APIServer = controlplane.APIServer

	// Etcd is the re-exported Etcd from the internal testing package.
	Etcd = controlplane.Etcd

	// User represents a Kubernetes user to provision for auth purposes.
	User = controlplane.User

	// AuthenticatedUser represets a Kubernetes user that's been provisioned.
	AuthenticatedUser = controlplane.AuthenticatedUser

	// ListenAddr indicates the address and port that the API server should listen on.
	ListenAddr = process.ListenAddr

	// SecureServing contains details describing how the API server should serve
	// its secure endpoint.
	SecureServing = controlplane.SecureServing

	// Authn is an authentication method that can be used with the control plane to
	// provision users.
	Authn = controlplane.Authn

	// Arguments allows configuring a process's flags.
	Arguments = process.Arguments

	// Arg is a single flag with one or more values.
	Arg = process.Arg
)

var (
	// EmptyArguments constructs a new set of flags with nothing set.
	//
	// This is mostly useful for testing helper methods -- you'll want to call
	// Configure on the APIServer (or etcd) to configure their arguments.
	EmptyArguments = process.EmptyArguments
)

// Environment creates a Kubernetes test environment that will start / stop the Kubernetes control plane and
// install extension APIs.
type Environment struct {
	// ControlPlane is the ControlPlane including the apiserver and etcd
	ControlPlane controlplane.ControlPlane

	// Scheme is used to determine if conversion webhooks should be enabled
	// for a particular CRD / object.
	//
	// Conversion webhooks are going to be enabled if an object in the scheme
	// implements Hub and Spoke conversions.
	//
	// If nil, scheme.Scheme is used.
	Scheme *runtime.Scheme

	// Config can be used to talk to the apiserver.  It's automatically
	// populated if not set using the standard controller-runtime config
	// loading.
	Config *rest.Config

	// CRDInstallOptions are the options for installing CRDs.
	CRDInstallOptions CRDInstallOptions

	// WebhookInstallOptions are the options for installing webhooks.
	WebhookInstallOptions WebhookInstallOptions

	// ErrorIfCRDPathMissing provides an interface for the underlying
	// CRDInstallOptions.ErrorIfPathMissing. It prevents silent failures
	// for missing CRD paths.
	ErrorIfCRDPathMissing bool

	// CRDs is a list of CRDs to install.
	// If both this field and CRDs field in CRDInstallOptions are specified, the
	// values are merged.
	CRDs []*apiextensionsv1.CustomResourceDefinition

	// CRDDirectoryPaths is a list of paths containing CRD yaml or json configs.
	// If both this field and Paths field in CRDInstallOptions are specified, the
	// values are merged.
	CRDDirectoryPaths []string

	// BinaryAssetsDirectory is the path where the binaries required for the envtest are
	// located in the local environment. This field can be overridden by setting KUBEBUILDER_ASSETS.
	BinaryAssetsDirectory string

	// UseExistingCluster indicates that this environments should use an
	// existing kubeconfig, instead of trying to stand up a new control plane.
	// This is useful in cases that need aggregated API servers and the like.
	UseExistingCluster *bool

	// ControlPlaneStartTimeout is the maximum duration each controlplane component
	// may take to start. It defaults to the KUBEBUILDER_CONTROLPLANE_START_TIMEOUT
	// environment variable or 20 seconds if unspecified
	ControlPlaneStartTimeout time.Duration

	// ControlPlaneStopTimeout is the maximum duration each controlplane component
	// may take to stop. It defaults to the KUBEBUILDER_CONTROLPLANE_STOP_TIMEOUT
	// environment variable or 20 seconds if unspecified
	ControlPlaneStopTimeout time.Duration

	// KubeAPIServerFlags is the set of flags passed while starting the api server.
	//
	// Deprecated: use ControlPlane.GetAPIServer().Configure() instead.
	KubeAPIServerFlags []string

	// AttachControlPlaneOutput indicates if control plane output will be attached to os.Stdout and os.Stderr.
	// Enable this to get more visibility of the testing control plane.
	// It respect KUBEBUILDER_ATTACH_CONTROL_PLANE_OUTPUT environment variable.
	AttachControlPlaneOutput bool
}

// Stop stops a running server.
// Previously installed CRDs, as listed in CRDInstallOptions.CRDs, will be uninstalled
// if CRDInstallOptions.CleanUpAfterUse are set to true.
func (te *Environment) Stop() error {
	if te.CRDInstallOptions.CleanUpAfterUse {
		if err := UninstallCRDs(te.Config, te.CRDInstallOptions); err != nil {
			return err
		}
	}

	if err := te.WebhookInstallOptions.Cleanup(); err != nil {
		return err
	}

	if te.useExistingCluster() {
		return nil
	}

	return te.ControlPlane.Stop()
}

// Start starts a local Kubernetes server and updates te.ApiserverPort with the port it is listening on.
func (te *Environment) Start() (*rest.Config, error) {
	if te.useExistingCluster() {
		log.V(1).Info("using existing cluster")
		if te.Config == nil {
			// we want to allow people to pass in their own config, so
			// only load a config if it hasn't already been set.
			log.V(1).Info("automatically acquiring client configuration")

			var err error
			te.Config, err = config.GetConfig()
			if err != nil {
				return nil, fmt.Errorf("unable to get configuration for existing cluster: %w", err)
			}
		}
	} else {
		apiServer := te.ControlPlane.GetAPIServer()
		if len(apiServer.Args) == 0 { //nolint:staticcheck
			// pass these through separately from above in case something like
			// AddUser defaults APIServer.
			//
			// TODO(directxman12): if/when we feel like making a bigger
			// breaking change here, just make APIServer and Etcd non-pointers
			// in ControlPlane.

			// NB(directxman12): we still pass these in so that things work if the
			// user manually specifies them, but in most cases we expect them to
			// be nil so that we use the new .Configure() logic.
			apiServer.Args = te.KubeAPIServerFlags //nolint:staticcheck
		}
		if te.ControlPlane.Etcd == nil {
			te.ControlPlane.Etcd = &controlplane.Etcd{}
		}

		if os.Getenv(envAttachOutput) == "true" {
			te.AttachControlPlaneOutput = true
		}
		if apiServer.Out == nil && te.AttachControlPlaneOutput {
			apiServer.Out = os.Stdout
		}
		if apiServer.Err == nil && te.AttachControlPlaneOutput {
			apiServer.Err = os.Stderr
		}
		if te.ControlPlane.Etcd.Out == nil && te.AttachControlPlaneOutput {
			te.ControlPlane.Etcd.Out = os.Stdout
		}
		if te.ControlPlane.Etcd.Err == nil && te.AttachControlPlaneOutput {
			te.ControlPlane.Etcd.Err = os.Stderr
		}

		apiServer.Path = process.BinPathFinder("kube-apiserver", te.BinaryAssetsDirectory)
		te.ControlPlane.Etcd.Path = process.BinPathFinder("etcd", te.BinaryAssetsDirectory)
		te.ControlPlane.KubectlPath = process.BinPathFinder("kubectl", te.BinaryAssetsDirectory)

		if err := te.defaultTimeouts(); err != nil {
			return nil, fmt.Errorf("failed to default controlplane timeouts: %w", err)
		}
		te.ControlPlane.Etcd.StartTimeout = te.ControlPlaneStartTimeout
		te.ControlPlane.Etcd.StopTimeout = te.ControlPlaneStopTimeout
		apiServer.StartTimeout = te.ControlPlaneStartTimeout
		apiServer.StopTimeout = te.ControlPlaneStopTimeout

		log.V(1).Info("starting control plane")
		if err := te.startControlPlane(); err != nil {
			return nil, fmt.Errorf("unable to start control plane itself: %w", err)
		}

		// Create the *rest.Config for creating new clients
		baseConfig := &rest.Config{
			// gotta go fast during tests -- we don't really care about overwhelming our test API server
			QPS:   1000.0,
			Burst: 2000.0,
		}

		adminInfo := User{Name: "admin", Groups: []string{"system:masters"}}
		adminUser, err := te.ControlPlane.AddUser(adminInfo, baseConfig)
		if err != nil {
			return te.Config, fmt.Errorf("unable to provision admin user: %w", err)
		}
		te.Config = adminUser.Config()
	}

	// Set the default scheme if nil.
	if te.Scheme == nil {
		te.Scheme = scheme.Scheme
	}

	// Call PrepWithoutInstalling to setup certificates first
	// and have them available to patch CRD conversion webhook as well.
	if err := te.WebhookInstallOptions.PrepWithoutInstalling(); err != nil {
		return nil, err
	}

	log.V(1).Info("installing CRDs")
	te.CRDInstallOptions.CRDs = mergeCRDs(te.CRDInstallOptions.CRDs, te.CRDs)
	te.CRDInstallOptions.Paths = mergePaths(te.CRDInstallOptions.Paths, te.CRDDirectoryPaths)
	te.CRDInstallOptions.ErrorIfPathMissing = te.ErrorIfCRDPathMissing
	te.CRDInstallOptions.WebhookOptions = te.WebhookInstallOptions
	crds, err := InstallCRDs(te.Config, te.CRDInstallOptions)
	if err != nil {
		return te.Config, fmt.Errorf("unable to install CRDs onto control plane: %w", err)
	}
	te.CRDs = crds

	log.V(1).Info("installing webhooks")
	if err := te.WebhookInstallOptions.Install(te.Config); err != nil {
		return nil, fmt.Errorf("unable to install webhooks onto control plane: %w", err)
	}
	return te.Config, nil
}

// AddUser provisions a new user for connecting to this Environment.  The user will
// have the specified name & belong to the specified groups.
//
// If you specify a "base" config, the returned REST Config will contain those
// settings as well as any required by the authentication method.  You can use
// this to easily specify options like QPS.
//
// This is effectively a convinience alias for ControlPlane.AddUser -- see that
// for more low-level details.
func (te *Environment) AddUser(user User, baseConfig *rest.Config) (*AuthenticatedUser, error) {
	return te.ControlPlane.AddUser(user, baseConfig)
}

func (te *Environment) startControlPlane() error {
	numTries, maxRetries := 0, 5
	var err error
	for ; numTries < maxRetries; numTries++ {
		// Start the control plane - retry if it fails
		err = te.ControlPlane.Start()
		if err == nil {
			break
		}
		log.Error(err, "unable to start the controlplane", "tries", numTries)
	}
	if numTries == maxRetries {
		return fmt.Errorf("failed to start the controlplane. retried %d times: %w", numTries, err)
	}
	return nil
}

func (te *Environment) defaultTimeouts() error {
	var err error
	if te.ControlPlaneStartTimeout == 0 {
		if envVal := os.Getenv(envStartTimeout); envVal != "" {
			te.ControlPlaneStartTimeout, err = time.ParseDuration(envVal)
			if err != nil {
				return err
			}
		} else {
			te.ControlPlaneStartTimeout = defaultKubebuilderControlPlaneStartTimeout
		}
	}

	if te.ControlPlaneStopTimeout == 0 {
		if envVal := os.Getenv(envStopTimeout); envVal != "" {
			te.ControlPlaneStopTimeout, err = time.ParseDuration(envVal)
			if err != nil {
				return err
			}
		} else {
			te.ControlPlaneStopTimeout = defaultKubebuilderControlPlaneStopTimeout
		}
	}
	return nil
}

func (te *Environment) useExistingCluster() bool {
	if te.UseExistingCluster == nil {
		return strings.ToLower(os.Getenv(envUseExistingCluster)) == "true"
	}
	return *te.UseExistingCluster
}

// DefaultKubeAPIServerFlags exposes the default args for the APIServer so that
// you can use those to append your own additional arguments.
//
// Deprecated: use APIServer.Configure() instead.
var DefaultKubeAPIServerFlags = controlplane.APIServerDefaultArgs //nolint:staticcheck
