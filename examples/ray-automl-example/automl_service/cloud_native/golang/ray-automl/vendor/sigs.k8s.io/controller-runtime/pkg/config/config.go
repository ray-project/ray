/*
Copyright 2020 The Kubernetes Authors.

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

package config

import (
	"fmt"
	"os"
	"sync"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"sigs.k8s.io/controller-runtime/pkg/config/v1alpha1"
)

// ControllerManagerConfiguration defines the functions necessary to parse a config file
// and to configure the Options struct for the ctrl.Manager.
type ControllerManagerConfiguration interface {
	runtime.Object

	// Complete returns the versioned configuration
	Complete() (v1alpha1.ControllerManagerConfigurationSpec, error)
}

// DeferredFileLoader is used to configure the decoder for loading controller
// runtime component config types.
type DeferredFileLoader struct {
	ControllerManagerConfiguration
	path   string
	scheme *runtime.Scheme
	once   sync.Once
	err    error
}

// File will set up the deferred file loader for the configuration
// this will also configure the defaults for the loader if nothing is
//
// Defaults:
// * Path: "./config.yaml"
// * Kind: GenericControllerManagerConfiguration
func File() *DeferredFileLoader {
	scheme := runtime.NewScheme()
	utilruntime.Must(v1alpha1.AddToScheme(scheme))
	return &DeferredFileLoader{
		path:                           "./config.yaml",
		ControllerManagerConfiguration: &v1alpha1.ControllerManagerConfiguration{},
		scheme:                         scheme,
	}
}

// Complete will use sync.Once to set the scheme.
func (d *DeferredFileLoader) Complete() (v1alpha1.ControllerManagerConfigurationSpec, error) {
	d.once.Do(d.loadFile)
	if d.err != nil {
		return v1alpha1.ControllerManagerConfigurationSpec{}, d.err
	}
	return d.ControllerManagerConfiguration.Complete()
}

// AtPath will set the path to load the file for the decoder.
func (d *DeferredFileLoader) AtPath(path string) *DeferredFileLoader {
	d.path = path
	return d
}

// OfKind will set the type to be used for decoding the file into.
func (d *DeferredFileLoader) OfKind(obj ControllerManagerConfiguration) *DeferredFileLoader {
	d.ControllerManagerConfiguration = obj
	return d
}

// InjectScheme will configure the scheme to be used for decoding the file.
func (d *DeferredFileLoader) InjectScheme(scheme *runtime.Scheme) error {
	d.scheme = scheme
	return nil
}

// loadFile is used from the mutex.Once to load the file.
func (d *DeferredFileLoader) loadFile() {
	if d.scheme == nil {
		d.err = fmt.Errorf("scheme not supplied to controller configuration loader")
		return
	}

	content, err := os.ReadFile(d.path)
	if err != nil {
		d.err = fmt.Errorf("could not read file at %s", d.path)
		return
	}

	codecs := serializer.NewCodecFactory(d.scheme)

	// Regardless of if the bytes are of any external version,
	// it will be read successfully and converted into the internal version
	if err = runtime.DecodeInto(codecs.UniversalDecoder(), content, d.ControllerManagerConfiguration); err != nil {
		d.err = fmt.Errorf("could not decode file into runtime.Object")
	}
}
