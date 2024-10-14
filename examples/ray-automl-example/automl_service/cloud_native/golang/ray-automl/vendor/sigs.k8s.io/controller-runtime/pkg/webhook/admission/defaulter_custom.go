/*
Copyright 2021 The Kubernetes Authors.

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

package admission

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
)

// CustomDefaulter defines functions for setting defaults on resources.
type CustomDefaulter interface {
	Default(ctx context.Context, obj runtime.Object) error
}

// WithCustomDefaulter creates a new Webhook for a CustomDefaulter interface.
func WithCustomDefaulter(obj runtime.Object, defaulter CustomDefaulter) *Webhook {
	return &Webhook{
		Handler: &defaulterForType{object: obj, defaulter: defaulter},
	}
}

type defaulterForType struct {
	defaulter CustomDefaulter
	object    runtime.Object
	decoder   *Decoder
}

var _ DecoderInjector = &defaulterForType{}

func (h *defaulterForType) InjectDecoder(d *Decoder) error {
	h.decoder = d
	return nil
}

// Handle handles admission requests.
func (h *defaulterForType) Handle(ctx context.Context, req Request) Response {
	if h.defaulter == nil {
		panic("defaulter should never be nil")
	}
	if h.object == nil {
		panic("object should never be nil")
	}

	ctx = NewContextWithRequest(ctx, req)

	// Get the object in the request
	obj := h.object.DeepCopyObject()
	if err := h.decoder.Decode(req, obj); err != nil {
		return Errored(http.StatusBadRequest, err)
	}

	// Default the object
	if err := h.defaulter.Default(ctx, obj); err != nil {
		var apiStatus apierrors.APIStatus
		if errors.As(err, &apiStatus) {
			return validationResponseFromStatus(false, apiStatus.Status())
		}
		return Denied(err.Error())
	}

	// Create the patch
	marshalled, err := json.Marshal(obj)
	if err != nil {
		return Errored(http.StatusInternalServerError, err)
	}
	return PatchResponseFromRaw(req.Object.Raw, marshalled)
}
