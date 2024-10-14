/*
Copyright 2019 The Kubernetes Authors.
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
	"context"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"time"

	admissionv1 "k8s.io/api/admissionregistration/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/addr"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/certs"
)

// WebhookInstallOptions are the options for installing mutating or validating webhooks.
type WebhookInstallOptions struct {
	// Paths is a list of paths to the directories or files containing the mutating or validating webhooks yaml or json configs.
	Paths []string

	// MutatingWebhooks is a list of MutatingWebhookConfigurations to install
	MutatingWebhooks []*admissionv1.MutatingWebhookConfiguration

	// ValidatingWebhooks is a list of ValidatingWebhookConfigurations to install
	ValidatingWebhooks []*admissionv1.ValidatingWebhookConfiguration

	// IgnoreErrorIfPathMissing will ignore an error if a DirectoryPath does not exist when set to true
	IgnoreErrorIfPathMissing bool

	// LocalServingHost is the host for serving webhooks on.
	// it will be automatically populated
	LocalServingHost string

	// LocalServingPort is the allocated port for serving webhooks on.
	// it will be automatically populated by a random available local port
	LocalServingPort int

	// LocalServingCertDir is the allocated directory for serving certificates.
	// it will be automatically populated by the local temp dir
	LocalServingCertDir string

	// CAData is the CA that can be used to trust the serving certificates in LocalServingCertDir.
	LocalServingCAData []byte

	// LocalServingHostExternalName is the hostname to use to reach the webhook server.
	LocalServingHostExternalName string

	// MaxTime is the max time to wait
	MaxTime time.Duration

	// PollInterval is the interval to check
	PollInterval time.Duration
}

// ModifyWebhookDefinitions modifies webhook definitions by:
// - applying CABundle based on the provided tinyca
// - if webhook client config uses service spec, it's removed and replaced with direct url.
func (o *WebhookInstallOptions) ModifyWebhookDefinitions() error {
	caData := o.LocalServingCAData

	// generate host port.
	hostPort, err := o.generateHostPort()
	if err != nil {
		return err
	}

	for i := range o.MutatingWebhooks {
		for j := range o.MutatingWebhooks[i].Webhooks {
			updateClientConfig(&o.MutatingWebhooks[i].Webhooks[j].ClientConfig, hostPort, caData)
		}
	}

	for i := range o.ValidatingWebhooks {
		for j := range o.ValidatingWebhooks[i].Webhooks {
			updateClientConfig(&o.ValidatingWebhooks[i].Webhooks[j].ClientConfig, hostPort, caData)
		}
	}
	return nil
}

func updateClientConfig(cc *admissionv1.WebhookClientConfig, hostPort string, caData []byte) {
	cc.CABundle = caData
	if cc.Service != nil && cc.Service.Path != nil {
		url := fmt.Sprintf("https://%s/%s", hostPort, *cc.Service.Path)
		cc.URL = &url
		cc.Service = nil
	}
}

func (o *WebhookInstallOptions) generateHostPort() (string, error) {
	if o.LocalServingPort == 0 {
		port, host, err := addr.Suggest(o.LocalServingHost)
		if err != nil {
			return "", fmt.Errorf("unable to grab random port for serving webhooks on: %w", err)
		}
		o.LocalServingPort = port
		o.LocalServingHost = host
	}
	host := o.LocalServingHostExternalName
	if host == "" {
		host = o.LocalServingHost
	}
	return net.JoinHostPort(host, fmt.Sprintf("%d", o.LocalServingPort)), nil
}

// PrepWithoutInstalling does the setup parts of Install (populating host-port,
// setting up CAs, etc), without actually truing to do anything with webhook
// definitions.  This is largely useful for internal testing of
// controller-runtime, where we need a random host-port & caData for webhook
// tests, but may be useful in similar scenarios.
func (o *WebhookInstallOptions) PrepWithoutInstalling() error {
	if err := o.setupCA(); err != nil {
		return err
	}

	if err := parseWebhook(o); err != nil {
		return err
	}

	return o.ModifyWebhookDefinitions()
}

// Install installs specified webhooks to the API server.
func (o *WebhookInstallOptions) Install(config *rest.Config) error {
	if len(o.LocalServingCAData) == 0 {
		if err := o.PrepWithoutInstalling(); err != nil {
			return err
		}
	}

	if err := createWebhooks(config, o.MutatingWebhooks, o.ValidatingWebhooks); err != nil {
		return err
	}

	return WaitForWebhooks(config, o.MutatingWebhooks, o.ValidatingWebhooks, *o)
}

// Cleanup cleans up cert directories.
func (o *WebhookInstallOptions) Cleanup() error {
	if o.LocalServingCertDir != "" {
		return os.RemoveAll(o.LocalServingCertDir)
	}
	return nil
}

// WaitForWebhooks waits for the Webhooks to be available through API server.
func WaitForWebhooks(config *rest.Config,
	mutatingWebhooks []*admissionv1.MutatingWebhookConfiguration,
	validatingWebhooks []*admissionv1.ValidatingWebhookConfiguration,
	options WebhookInstallOptions) error {
	waitingFor := map[schema.GroupVersionKind]*sets.String{}

	for _, hook := range mutatingWebhooks {
		h := hook
		gvk, err := apiutil.GVKForObject(h, scheme.Scheme)
		if err != nil {
			return fmt.Errorf("unable to get gvk for MutatingWebhookConfiguration %s: %w", hook.GetName(), err)
		}

		if _, ok := waitingFor[gvk]; !ok {
			waitingFor[gvk] = &sets.String{}
		}
		waitingFor[gvk].Insert(h.GetName())
	}

	for _, hook := range validatingWebhooks {
		h := hook
		gvk, err := apiutil.GVKForObject(h, scheme.Scheme)
		if err != nil {
			return fmt.Errorf("unable to get gvk for ValidatingWebhookConfiguration %s: %w", hook.GetName(), err)
		}

		if _, ok := waitingFor[gvk]; !ok {
			waitingFor[gvk] = &sets.String{}
		}
		waitingFor[gvk].Insert(hook.GetName())
	}

	// Poll until all resources are found in discovery
	p := &webhookPoller{config: config, waitingFor: waitingFor}
	return wait.PollImmediate(options.PollInterval, options.MaxTime, p.poll)
}

// poller checks if all the resources have been found in discovery, and returns false if not.
type webhookPoller struct {
	// config is used to get discovery
	config *rest.Config

	// waitingFor is the map of resources keyed by group version that have not yet been found in discovery
	waitingFor map[schema.GroupVersionKind]*sets.String
}

// poll checks if all the resources have been found in discovery, and returns false if not.
func (p *webhookPoller) poll() (done bool, err error) {
	// Create a new clientset to avoid any client caching of discovery
	c, err := client.New(p.config, client.Options{})
	if err != nil {
		return false, err
	}

	allFound := true
	for gvk, names := range p.waitingFor {
		if names.Len() == 0 {
			delete(p.waitingFor, gvk)
			continue
		}
		for _, name := range names.List() {
			var obj = &unstructured.Unstructured{}
			obj.SetGroupVersionKind(gvk)
			err := c.Get(context.Background(), client.ObjectKey{
				Namespace: "",
				Name:      name,
			}, obj)

			if err == nil {
				names.Delete(name)
			}

			if apierrors.IsNotFound(err) {
				allFound = false
			}
			if err != nil {
				return false, err
			}
		}
	}
	return allFound, nil
}

// setupCA creates CA for testing and writes them to disk.
func (o *WebhookInstallOptions) setupCA() error {
	hookCA, err := certs.NewTinyCA()
	if err != nil {
		return fmt.Errorf("unable to set up webhook CA: %w", err)
	}

	names := []string{"localhost", o.LocalServingHost, o.LocalServingHostExternalName}
	hookCert, err := hookCA.NewServingCert(names...)
	if err != nil {
		return fmt.Errorf("unable to set up webhook serving certs: %w", err)
	}

	localServingCertsDir, err := os.MkdirTemp("", "envtest-serving-certs-")
	o.LocalServingCertDir = localServingCertsDir
	if err != nil {
		return fmt.Errorf("unable to create directory for webhook serving certs: %w", err)
	}

	certData, keyData, err := hookCert.AsBytes()
	if err != nil {
		return fmt.Errorf("unable to marshal webhook serving certs: %w", err)
	}

	if err := os.WriteFile(filepath.Join(localServingCertsDir, "tls.crt"), certData, 0640); err != nil { //nolint:gosec
		return fmt.Errorf("unable to write webhook serving cert to disk: %w", err)
	}
	if err := os.WriteFile(filepath.Join(localServingCertsDir, "tls.key"), keyData, 0640); err != nil { //nolint:gosec
		return fmt.Errorf("unable to write webhook serving key to disk: %w", err)
	}

	o.LocalServingCAData = certData
	return err
}

func createWebhooks(config *rest.Config, mutHooks []*admissionv1.MutatingWebhookConfiguration, valHooks []*admissionv1.ValidatingWebhookConfiguration) error {
	cs, err := client.New(config, client.Options{})
	if err != nil {
		return err
	}

	// Create each webhook
	for _, hook := range mutHooks {
		hook := hook
		log.V(1).Info("installing mutating webhook", "webhook", hook.GetName())
		if err := ensureCreated(cs, hook); err != nil {
			return err
		}
	}
	for _, hook := range valHooks {
		hook := hook
		log.V(1).Info("installing validating webhook", "webhook", hook.GetName())
		if err := ensureCreated(cs, hook); err != nil {
			return err
		}
	}
	return nil
}

// ensureCreated creates or update object if already exists in the cluster.
func ensureCreated(cs client.Client, obj client.Object) error {
	existing := obj.DeepCopyObject().(client.Object)
	err := cs.Get(context.Background(), client.ObjectKey{Name: obj.GetName()}, existing)
	switch {
	case apierrors.IsNotFound(err):
		if err := cs.Create(context.Background(), obj); err != nil {
			return err
		}
	case err != nil:
		return err
	default:
		log.V(1).Info("Webhook configuration already exists, updating", "webhook", obj.GetName())
		obj.SetResourceVersion(existing.GetResourceVersion())
		if err := cs.Update(context.Background(), obj); err != nil {
			return err
		}
	}
	return nil
}

// parseWebhook reads the directories or files of Webhooks in options.Paths and adds the Webhook structs to options.
func parseWebhook(options *WebhookInstallOptions) error {
	if len(options.Paths) > 0 {
		for _, path := range options.Paths {
			_, err := os.Stat(path)
			if options.IgnoreErrorIfPathMissing && os.IsNotExist(err) {
				continue // skip this path
			}
			if !options.IgnoreErrorIfPathMissing && os.IsNotExist(err) {
				return err // treat missing path as error
			}
			mutHooks, valHooks, err := readWebhooks(path)
			if err != nil {
				return err
			}
			options.MutatingWebhooks = append(options.MutatingWebhooks, mutHooks...)
			options.ValidatingWebhooks = append(options.ValidatingWebhooks, valHooks...)
		}
	}
	return nil
}

// readWebhooks reads the Webhooks from files and Unmarshals them into structs
// returns slice of mutating and validating webhook configurations.
func readWebhooks(path string) ([]*admissionv1.MutatingWebhookConfiguration, []*admissionv1.ValidatingWebhookConfiguration, error) {
	// Get the webhook files
	var files []string
	var err error
	log.V(1).Info("reading Webhooks from path", "path", path)
	info, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}
	if !info.IsDir() {
		path, files = filepath.Dir(path), []string{info.Name()}
	} else {
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, nil, err
		}
		for _, e := range entries {
			files = append(files, e.Name())
		}
	}

	// file extensions that may contain Webhooks
	resourceExtensions := sets.NewString(".json", ".yaml", ".yml")

	var mutHooks []*admissionv1.MutatingWebhookConfiguration
	var valHooks []*admissionv1.ValidatingWebhookConfiguration
	for _, file := range files {
		// Only parse allowlisted file types
		if !resourceExtensions.Has(filepath.Ext(file)) {
			continue
		}

		// Unmarshal Webhooks from file into structs
		docs, err := readDocuments(filepath.Join(path, file))
		if err != nil {
			return nil, nil, err
		}

		for _, doc := range docs {
			var generic metav1.PartialObjectMetadata
			if err = yaml.Unmarshal(doc, &generic); err != nil {
				return nil, nil, err
			}

			const (
				admissionregv1 = "admissionregistration.k8s.io/v1"
			)
			switch {
			case generic.Kind == "MutatingWebhookConfiguration":
				if generic.APIVersion != admissionregv1 {
					return nil, nil, fmt.Errorf("only v1 is supported right now for MutatingWebhookConfiguration (name: %s)", generic.Name)
				}
				hook := &admissionv1.MutatingWebhookConfiguration{}
				if err := yaml.Unmarshal(doc, hook); err != nil {
					return nil, nil, err
				}
				mutHooks = append(mutHooks, hook)
			case generic.Kind == "ValidatingWebhookConfiguration":
				if generic.APIVersion != admissionregv1 {
					return nil, nil, fmt.Errorf("only v1 is supported right now for ValidatingWebhookConfiguration (name: %s)", generic.Name)
				}
				hook := &admissionv1.ValidatingWebhookConfiguration{}
				if err := yaml.Unmarshal(doc, hook); err != nil {
					return nil, nil, err
				}
				valHooks = append(valHooks, hook)
			default:
				continue
			}
		}

		log.V(1).Info("read webhooks from file", "file", file)
	}
	return mutHooks, valHooks, nil
}
