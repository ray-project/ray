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

package envtest

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/apimachinery/pkg/util/wait"
	k8syaml "k8s.io/apimachinery/pkg/util/yaml"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/util/retry"
	"k8s.io/utils/pointer"
	"sigs.k8s.io/yaml"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/webhook/conversion"
)

// CRDInstallOptions are the options for installing CRDs.
type CRDInstallOptions struct {
	// Scheme is used to determine if conversion webhooks should be enabled
	// for a particular CRD / object.
	//
	// Conversion webhooks are going to be enabled if an object in the scheme
	// implements Hub and Spoke conversions.
	//
	// If nil, scheme.Scheme is used.
	Scheme *runtime.Scheme

	// Paths is a list of paths to the directories or files containing CRDs
	Paths []string

	// CRDs is a list of CRDs to install
	CRDs []*apiextensionsv1.CustomResourceDefinition

	// ErrorIfPathMissing will cause an error if a Path does not exist
	ErrorIfPathMissing bool

	// MaxTime is the max time to wait
	MaxTime time.Duration

	// PollInterval is the interval to check
	PollInterval time.Duration

	// CleanUpAfterUse will cause the CRDs listed for installation to be
	// uninstalled when terminating the test environment.
	// Defaults to false.
	CleanUpAfterUse bool

	// WebhookOptions contains the conversion webhook information to install
	// on the CRDs. This field is usually inherited by the EnvTest options.
	//
	// If you're passing this field manually, you need to make sure that
	// the CA information and host port is filled in properly.
	WebhookOptions WebhookInstallOptions
}

const defaultPollInterval = 100 * time.Millisecond
const defaultMaxWait = 10 * time.Second

// InstallCRDs installs a collection of CRDs into a cluster by reading the crd yaml files from a directory.
func InstallCRDs(config *rest.Config, options CRDInstallOptions) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	defaultCRDOptions(&options)

	// Read the CRD yamls into options.CRDs
	if err := readCRDFiles(&options); err != nil {
		return nil, fmt.Errorf("unable to read CRD files: %w", err)
	}

	if err := modifyConversionWebhooks(options.CRDs, options.Scheme, options.WebhookOptions); err != nil {
		return nil, err
	}

	// Create the CRDs in the apiserver
	if err := CreateCRDs(config, options.CRDs); err != nil {
		return options.CRDs, fmt.Errorf("unable to create CRD instances: %w", err)
	}

	// Wait for the CRDs to appear as Resources in the apiserver
	if err := WaitForCRDs(config, options.CRDs, options); err != nil {
		return options.CRDs, fmt.Errorf("something went wrong waiting for CRDs to appear as API resources: %w", err)
	}

	return options.CRDs, nil
}

// readCRDFiles reads the directories of CRDs in options.Paths and adds the CRD structs to options.CRDs.
func readCRDFiles(options *CRDInstallOptions) error {
	if len(options.Paths) > 0 {
		crdList, err := renderCRDs(options)
		if err != nil {
			return err
		}

		options.CRDs = append(options.CRDs, crdList...)
	}
	return nil
}

// defaultCRDOptions sets the default values for CRDs.
func defaultCRDOptions(o *CRDInstallOptions) {
	if o.Scheme == nil {
		o.Scheme = scheme.Scheme
	}
	if o.MaxTime == 0 {
		o.MaxTime = defaultMaxWait
	}
	if o.PollInterval == 0 {
		o.PollInterval = defaultPollInterval
	}
}

// WaitForCRDs waits for the CRDs to appear in discovery.
func WaitForCRDs(config *rest.Config, crds []*apiextensionsv1.CustomResourceDefinition, options CRDInstallOptions) error {
	// Add each CRD to a map of GroupVersion to Resource
	waitingFor := map[schema.GroupVersion]*sets.String{}
	for _, crd := range crds {
		gvs := []schema.GroupVersion{}
		for _, version := range crd.Spec.Versions {
			if version.Served {
				gvs = append(gvs, schema.GroupVersion{Group: crd.Spec.Group, Version: version.Name})
			}
		}

		for _, gv := range gvs {
			log.V(1).Info("adding API in waitlist", "GV", gv)
			if _, found := waitingFor[gv]; !found {
				// Initialize the set
				waitingFor[gv] = &sets.String{}
			}
			// Add the Resource
			waitingFor[gv].Insert(crd.Spec.Names.Plural)
		}
	}

	// Poll until all resources are found in discovery
	p := &poller{config: config, waitingFor: waitingFor}
	return wait.PollImmediate(options.PollInterval, options.MaxTime, p.poll)
}

// poller checks if all the resources have been found in discovery, and returns false if not.
type poller struct {
	// config is used to get discovery
	config *rest.Config

	// waitingFor is the map of resources keyed by group version that have not yet been found in discovery
	waitingFor map[schema.GroupVersion]*sets.String
}

// poll checks if all the resources have been found in discovery, and returns false if not.
func (p *poller) poll() (done bool, err error) {
	// Create a new clientset to avoid any client caching of discovery
	cs, err := clientset.NewForConfig(p.config)
	if err != nil {
		return false, err
	}

	allFound := true
	for gv, resources := range p.waitingFor {
		// All resources found, do nothing
		if resources.Len() == 0 {
			delete(p.waitingFor, gv)
			continue
		}

		// Get the Resources for this GroupVersion
		// TODO: Maybe the controller-runtime client should be able to do this...
		resourceList, err := cs.Discovery().ServerResourcesForGroupVersion(gv.Group + "/" + gv.Version)
		if err != nil {
			return false, nil //nolint:nilerr
		}

		// Remove each found resource from the resources set that we are waiting for
		for _, resource := range resourceList.APIResources {
			resources.Delete(resource.Name)
		}

		// Still waiting on some resources in this group version
		if resources.Len() != 0 {
			allFound = false
		}
	}
	return allFound, nil
}

// UninstallCRDs uninstalls a collection of CRDs by reading the crd yaml files from a directory.
func UninstallCRDs(config *rest.Config, options CRDInstallOptions) error {
	// Read the CRD yamls into options.CRDs
	if err := readCRDFiles(&options); err != nil {
		return err
	}

	// Delete the CRDs from the apiserver
	cs, err := client.New(config, client.Options{})
	if err != nil {
		return err
	}

	// Uninstall each CRD
	for _, crd := range options.CRDs {
		crd := crd
		log.V(1).Info("uninstalling CRD", "crd", crd.GetName())
		if err := cs.Delete(context.TODO(), crd); err != nil {
			// If CRD is not found, we can consider success
			if !apierrors.IsNotFound(err) {
				return err
			}
		}
	}

	return nil
}

// CreateCRDs creates the CRDs.
func CreateCRDs(config *rest.Config, crds []*apiextensionsv1.CustomResourceDefinition) error {
	cs, err := client.New(config, client.Options{})
	if err != nil {
		return fmt.Errorf("unable to create client: %w", err)
	}

	// Create each CRD
	for _, crd := range crds {
		crd := crd
		log.V(1).Info("installing CRD", "crd", crd.GetName())
		existingCrd := crd.DeepCopy()
		err := cs.Get(context.TODO(), client.ObjectKey{Name: crd.GetName()}, existingCrd)
		switch {
		case apierrors.IsNotFound(err):
			if err := cs.Create(context.TODO(), crd); err != nil {
				return fmt.Errorf("unable to create CRD %q: %w", crd.GetName(), err)
			}
		case err != nil:
			return fmt.Errorf("unable to get CRD %q to check if it exists: %w", crd.GetName(), err)
		default:
			log.V(1).Info("CRD already exists, updating", "crd", crd.GetName())
			if err := retry.RetryOnConflict(retry.DefaultBackoff, func() error {
				if err := cs.Get(context.TODO(), client.ObjectKey{Name: crd.GetName()}, existingCrd); err != nil {
					return err
				}
				crd.SetResourceVersion(existingCrd.GetResourceVersion())
				return cs.Update(context.TODO(), crd)
			}); err != nil {
				return err
			}
		}
	}
	return nil
}

// renderCRDs iterate through options.Paths and extract all CRD files.
func renderCRDs(options *CRDInstallOptions) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	type GVKN struct {
		GVK  schema.GroupVersionKind
		Name string
	}

	crds := map[GVKN]*apiextensionsv1.CustomResourceDefinition{}

	for _, path := range options.Paths {
		var (
			err      error
			info     os.FileInfo
			files    []string
			filePath = path
		)

		// Return the error if ErrorIfPathMissing exists
		if info, err = os.Stat(path); os.IsNotExist(err) {
			if options.ErrorIfPathMissing {
				return nil, err
			}
			continue
		}

		if !info.IsDir() {
			filePath, files = filepath.Dir(path), []string{info.Name()}
		} else {
			entries, err := os.ReadDir(path)
			if err != nil {
				return nil, err
			}
			for _, e := range entries {
				files = append(files, e.Name())
			}
		}

		log.V(1).Info("reading CRDs from path", "path", path)
		crdList, err := readCRDs(filePath, files)
		if err != nil {
			return nil, err
		}

		for i, crd := range crdList {
			gvkn := GVKN{GVK: crd.GroupVersionKind(), Name: crd.GetName()}
			if _, found := crds[gvkn]; found {
				// Currently, we only print a log when there are duplicates. We may want to error out if that makes more sense.
				log.Info("there are more than one CRD definitions with the same <Group, Version, Kind, Name>", "GVKN", gvkn)
			}
			// We always use the CRD definition that we found last.
			crds[gvkn] = crdList[i]
		}
	}

	// Converting map to a list to return
	res := []*apiextensionsv1.CustomResourceDefinition{}
	for _, obj := range crds {
		res = append(res, obj)
	}
	return res, nil
}

// modifyConversionWebhooks takes all the registered CustomResourceDefinitions and applies modifications
// to conditionally enable webhooks if the type is registered within the scheme.
func modifyConversionWebhooks(crds []*apiextensionsv1.CustomResourceDefinition, scheme *runtime.Scheme, webhookOptions WebhookInstallOptions) error {
	if len(webhookOptions.LocalServingCAData) == 0 {
		return nil
	}

	// Determine all registered convertible types.
	convertibles := map[schema.GroupKind]struct{}{}
	for gvk := range scheme.AllKnownTypes() {
		obj, err := scheme.New(gvk)
		if err != nil {
			return err
		}
		if ok, err := conversion.IsConvertible(scheme, obj); ok && err == nil {
			convertibles[gvk.GroupKind()] = struct{}{}
		}
	}

	// generate host port.
	hostPort, err := webhookOptions.generateHostPort()
	if err != nil {
		return err
	}
	url := pointer.StringPtr(fmt.Sprintf("https://%s/convert", hostPort))

	for i := range crds {
		// Continue if we're preserving unknown fields.
		if crds[i].Spec.PreserveUnknownFields {
			continue
		}
		// Continue if the GroupKind isn't registered as being convertible.
		if _, ok := convertibles[schema.GroupKind{
			Group: crds[i].Spec.Group,
			Kind:  crds[i].Spec.Names.Kind,
		}]; !ok {
			continue
		}
		if crds[i].Spec.Conversion == nil {
			crds[i].Spec.Conversion = &apiextensionsv1.CustomResourceConversion{
				Webhook: &apiextensionsv1.WebhookConversion{},
			}
		}
		crds[i].Spec.Conversion.Strategy = apiextensionsv1.WebhookConverter
		crds[i].Spec.Conversion.Webhook.ConversionReviewVersions = []string{"v1", "v1beta1"}
		crds[i].Spec.Conversion.Webhook.ClientConfig = &apiextensionsv1.WebhookClientConfig{
			Service:  nil,
			URL:      url,
			CABundle: webhookOptions.LocalServingCAData,
		}
	}

	return nil
}

// readCRDs reads the CRDs from files and Unmarshals them into structs.
func readCRDs(basePath string, files []string) ([]*apiextensionsv1.CustomResourceDefinition, error) {
	var crds []*apiextensionsv1.CustomResourceDefinition

	// White list the file extensions that may contain CRDs
	crdExts := sets.NewString(".json", ".yaml", ".yml")

	for _, file := range files {
		// Only parse allowlisted file types
		if !crdExts.Has(filepath.Ext(file)) {
			continue
		}

		// Unmarshal CRDs from file into structs
		docs, err := readDocuments(filepath.Join(basePath, file))
		if err != nil {
			return nil, err
		}

		for _, doc := range docs {
			crd := &apiextensionsv1.CustomResourceDefinition{}
			if err = yaml.Unmarshal(doc, crd); err != nil {
				return nil, err
			}

			if crd.Kind != "CustomResourceDefinition" || crd.Spec.Names.Kind == "" || crd.Spec.Group == "" {
				continue
			}
			crds = append(crds, crd)
		}

		log.V(1).Info("read CRDs from file", "file", file)
	}
	return crds, nil
}

// readDocuments reads documents from file.
func readDocuments(fp string) ([][]byte, error) {
	b, err := os.ReadFile(fp)
	if err != nil {
		return nil, err
	}

	docs := [][]byte{}
	reader := k8syaml.NewYAMLReader(bufio.NewReader(bytes.NewReader(b)))
	for {
		// Read document
		doc, err := reader.Read()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}

			return nil, err
		}

		docs = append(docs, doc)
	}

	return docs, nil
}
