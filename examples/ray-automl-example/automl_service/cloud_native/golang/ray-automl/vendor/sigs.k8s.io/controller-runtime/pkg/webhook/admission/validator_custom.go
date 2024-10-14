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
	"errors"
	"fmt"
	"net/http"

	v1 "k8s.io/api/admission/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
)

// CustomValidator defines functions for validating an operation.
type CustomValidator interface {
	ValidateCreate(ctx context.Context, obj runtime.Object) error
	ValidateUpdate(ctx context.Context, oldObj, newObj runtime.Object) error
	ValidateDelete(ctx context.Context, obj runtime.Object) error
}

// WithCustomValidator creates a new Webhook for validating the provided type.
func WithCustomValidator(obj runtime.Object, validator CustomValidator) *Webhook {
	return &Webhook{
		Handler: &validatorForType{object: obj, validator: validator},
	}
}

type validatorForType struct {
	validator CustomValidator
	object    runtime.Object
	decoder   *Decoder
}

var _ DecoderInjector = &validatorForType{}

// InjectDecoder injects the decoder into a validatingHandler.
func (h *validatorForType) InjectDecoder(d *Decoder) error {
	h.decoder = d
	return nil
}

// Handle handles admission requests.
func (h *validatorForType) Handle(ctx context.Context, req Request) Response {
	if h.validator == nil {
		panic("validator should never be nil")
	}
	if h.object == nil {
		panic("object should never be nil")
	}

	ctx = NewContextWithRequest(ctx, req)

	// Get the object in the request
	obj := h.object.DeepCopyObject()

	var err error
	switch req.Operation {
	case v1.Create:
		if err := h.decoder.Decode(req, obj); err != nil {
			return Errored(http.StatusBadRequest, err)
		}

		err = h.validator.ValidateCreate(ctx, obj)
	case v1.Update:
		oldObj := obj.DeepCopyObject()
		if err := h.decoder.DecodeRaw(req.Object, obj); err != nil {
			return Errored(http.StatusBadRequest, err)
		}
		if err := h.decoder.DecodeRaw(req.OldObject, oldObj); err != nil {
			return Errored(http.StatusBadRequest, err)
		}

		err = h.validator.ValidateUpdate(ctx, oldObj, obj)
	case v1.Delete:
		// In reference to PR: https://github.com/kubernetes/kubernetes/pull/76346
		// OldObject contains the object being deleted
		if err := h.decoder.DecodeRaw(req.OldObject, obj); err != nil {
			return Errored(http.StatusBadRequest, err)
		}

		err = h.validator.ValidateDelete(ctx, obj)
	default:
		return Errored(http.StatusBadRequest, fmt.Errorf("unknown operation request %q", req.Operation))
	}

	// Check the error message first.
	if err != nil {
		var apiStatus apierrors.APIStatus
		if errors.As(err, &apiStatus) {
			return validationResponseFromStatus(false, apiStatus.Status())
		}
		return Denied(err.Error())
	}

	// Return allowed if everything succeeded.
	return Allowed("")
}
