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

package admission

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"

	jsonpatch "gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

type multiMutating []Handler

func (hs multiMutating) Handle(ctx context.Context, req Request) Response {
	patches := []jsonpatch.JsonPatchOperation{}
	for _, handler := range hs {
		resp := handler.Handle(ctx, req)
		if !resp.Allowed {
			return resp
		}
		if resp.PatchType != nil && *resp.PatchType != admissionv1.PatchTypeJSONPatch {
			return Errored(http.StatusInternalServerError,
				fmt.Errorf("unexpected patch type returned by the handler: %v, only allow: %v",
					resp.PatchType, admissionv1.PatchTypeJSONPatch))
		}
		patches = append(patches, resp.Patches...)
	}
	var err error
	marshaledPatch, err := json.Marshal(patches)
	if err != nil {
		return Errored(http.StatusBadRequest, fmt.Errorf("error when marshaling the patch: %w", err))
	}
	return Response{
		AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: true,
			Result: &metav1.Status{
				Code: http.StatusOK,
			},
			Patch:     marshaledPatch,
			PatchType: func() *admissionv1.PatchType { pt := admissionv1.PatchTypeJSONPatch; return &pt }(),
		},
	}
}

// InjectFunc injects the field setter into the handlers.
func (hs multiMutating) InjectFunc(f inject.Func) error {
	// inject directly into the handlers.  It would be more correct
	// to do this in a sync.Once in Handle (since we don't have some
	// other start/finalize-type method), but it's more efficient to
	// do it here, presumably.
	for _, handler := range hs {
		if err := f(handler); err != nil {
			return err
		}
	}

	return nil
}

// InjectDecoder injects the decoder into the handlers.
func (hs multiMutating) InjectDecoder(d *Decoder) error {
	for _, handler := range hs {
		if _, err := InjectDecoderInto(d, handler); err != nil {
			return err
		}
	}
	return nil
}

// MultiMutatingHandler combines multiple mutating webhook handlers into a single
// mutating webhook handler.  Handlers are called in sequential order, and the first
// `allowed: false`	response may short-circuit the rest.  Users must take care to
// ensure patches are disjoint.
func MultiMutatingHandler(handlers ...Handler) Handler {
	return multiMutating(handlers)
}

type multiValidating []Handler

func (hs multiValidating) Handle(ctx context.Context, req Request) Response {
	for _, handler := range hs {
		resp := handler.Handle(ctx, req)
		if !resp.Allowed {
			return resp
		}
	}
	return Response{
		AdmissionResponse: admissionv1.AdmissionResponse{
			Allowed: true,
			Result: &metav1.Status{
				Code: http.StatusOK,
			},
		},
	}
}

// MultiValidatingHandler combines multiple validating webhook handlers into a single
// validating webhook handler.  Handlers are called in sequential order, and the first
// `allowed: false`	response may short-circuit the rest.
func MultiValidatingHandler(handlers ...Handler) Handler {
	return multiValidating(handlers)
}

// InjectFunc injects the field setter into the handlers.
func (hs multiValidating) InjectFunc(f inject.Func) error {
	// inject directly into the handlers.  It would be more correct
	// to do this in a sync.Once in Handle (since we don't have some
	// other start/finalize-type method), but it's more efficient to
	// do it here, presumably.
	for _, handler := range hs {
		if err := f(handler); err != nil {
			return err
		}
	}

	return nil
}

// InjectDecoder injects the decoder into the handlers.
func (hs multiValidating) InjectDecoder(d *Decoder) error {
	for _, handler := range hs {
		if _, err := InjectDecoderInto(d, handler); err != nil {
			return err
		}
	}
	return nil
}
