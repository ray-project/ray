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

package webhook

import (
	"gomodules.xyz/jsonpatch/v2"
	"sigs.k8s.io/controller-runtime/pkg/webhook/admission"
)

// define some aliases for common bits of the webhook functionality

// Defaulter defines functions for setting defaults on resources.
type Defaulter = admission.Defaulter

// Validator defines functions for validating an operation.
type Validator = admission.Validator

// CustomDefaulter defines functions for setting defaults on resources.
type CustomDefaulter = admission.CustomDefaulter

// CustomValidator defines functions for validating an operation.
type CustomValidator = admission.CustomValidator

// AdmissionRequest defines the input for an admission handler.
// It contains information to identify the object in
// question (group, version, kind, resource, subresource,
// name, namespace), as well as the operation in question
// (e.g. Get, Create, etc), and the object itself.
type AdmissionRequest = admission.Request

// AdmissionResponse is the output of an admission handler.
// It contains a response indicating if a given
// operation is allowed, as well as a set of patches
// to mutate the object in the case of a mutating admission handler.
type AdmissionResponse = admission.Response

// Admission is webhook suitable for registration with the server
// an admission webhook that validates API operations and potentially
// mutates their contents.
type Admission = admission.Webhook

// AdmissionHandler knows how to process admission requests, validating them,
// and potentially mutating the objects they contain.
type AdmissionHandler = admission.Handler

// AdmissionDecoder knows how to decode objects from admission requests.
type AdmissionDecoder = admission.Decoder

// JSONPatchOp represents a single JSONPatch patch operation.
type JSONPatchOp = jsonpatch.Operation

var (
	// Allowed indicates that the admission request should be allowed for the given reason.
	Allowed = admission.Allowed

	// Denied indicates that the admission request should be denied for the given reason.
	Denied = admission.Denied

	// Patched indicates that the admission request should be allowed for the given reason,
	// and that the contained object should be mutated using the given patches.
	Patched = admission.Patched

	// Errored indicates that an error occurred in the admission request.
	Errored = admission.Errored
)
