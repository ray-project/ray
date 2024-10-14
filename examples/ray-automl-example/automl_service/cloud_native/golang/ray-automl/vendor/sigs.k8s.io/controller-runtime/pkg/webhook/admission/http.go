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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"

	v1 "k8s.io/api/admission/v1"
	"k8s.io/api/admission/v1beta1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
)

var admissionScheme = runtime.NewScheme()
var admissionCodecs = serializer.NewCodecFactory(admissionScheme)

func init() {
	utilruntime.Must(v1.AddToScheme(admissionScheme))
	utilruntime.Must(v1beta1.AddToScheme(admissionScheme))
}

var _ http.Handler = &Webhook{}

func (wh *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var body []byte
	var err error
	ctx := r.Context()
	if wh.WithContextFunc != nil {
		ctx = wh.WithContextFunc(ctx, r)
	}

	var reviewResponse Response
	if r.Body == nil {
		err = errors.New("request body is empty")
		wh.log.Error(err, "bad request")
		reviewResponse = Errored(http.StatusBadRequest, err)
		wh.writeResponse(w, reviewResponse)
		return
	}

	defer r.Body.Close()
	if body, err = io.ReadAll(r.Body); err != nil {
		wh.log.Error(err, "unable to read the body from the incoming request")
		reviewResponse = Errored(http.StatusBadRequest, err)
		wh.writeResponse(w, reviewResponse)
		return
	}

	// verify the content type is accurate
	if contentType := r.Header.Get("Content-Type"); contentType != "application/json" {
		err = fmt.Errorf("contentType=%s, expected application/json", contentType)
		wh.log.Error(err, "unable to process a request with an unknown content type", "content type", contentType)
		reviewResponse = Errored(http.StatusBadRequest, err)
		wh.writeResponse(w, reviewResponse)
		return
	}

	// Both v1 and v1beta1 AdmissionReview types are exactly the same, so the v1beta1 type can
	// be decoded into the v1 type. However the runtime codec's decoder guesses which type to
	// decode into by type name if an Object's TypeMeta isn't set. By setting TypeMeta of an
	// unregistered type to the v1 GVK, the decoder will coerce a v1beta1 AdmissionReview to v1.
	// The actual AdmissionReview GVK will be used to write a typed response in case the
	// webhook config permits multiple versions, otherwise this response will fail.
	req := Request{}
	ar := unversionedAdmissionReview{}
	// avoid an extra copy
	ar.Request = &req.AdmissionRequest
	ar.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("AdmissionReview"))
	_, actualAdmRevGVK, err := admissionCodecs.UniversalDeserializer().Decode(body, nil, &ar)
	if err != nil {
		wh.log.Error(err, "unable to decode the request")
		reviewResponse = Errored(http.StatusBadRequest, err)
		wh.writeResponse(w, reviewResponse)
		return
	}
	wh.log.V(1).Info("received request", "UID", req.UID, "kind", req.Kind, "resource", req.Resource)

	reviewResponse = wh.Handle(ctx, req)
	wh.writeResponseTyped(w, reviewResponse, actualAdmRevGVK)
}

// writeResponse writes response to w generically, i.e. without encoding GVK information.
func (wh *Webhook) writeResponse(w io.Writer, response Response) {
	wh.writeAdmissionResponse(w, v1.AdmissionReview{Response: &response.AdmissionResponse})
}

// writeResponseTyped writes response to w with GVK set to admRevGVK, which is necessary
// if multiple AdmissionReview versions are permitted by the webhook.
func (wh *Webhook) writeResponseTyped(w io.Writer, response Response, admRevGVK *schema.GroupVersionKind) {
	ar := v1.AdmissionReview{
		Response: &response.AdmissionResponse,
	}
	// Default to a v1 AdmissionReview, otherwise the API server may not recognize the request
	// if multiple AdmissionReview versions are permitted by the webhook config.
	// TODO(estroz): this should be configurable since older API servers won't know about v1.
	if admRevGVK == nil || *admRevGVK == (schema.GroupVersionKind{}) {
		ar.SetGroupVersionKind(v1.SchemeGroupVersion.WithKind("AdmissionReview"))
	} else {
		ar.SetGroupVersionKind(*admRevGVK)
	}
	wh.writeAdmissionResponse(w, ar)
}

// writeAdmissionResponse writes ar to w.
func (wh *Webhook) writeAdmissionResponse(w io.Writer, ar v1.AdmissionReview) {
	if err := json.NewEncoder(w).Encode(ar); err != nil {
		wh.log.Error(err, "unable to encode and write the response")
		// Since the `ar v1.AdmissionReview` is a clear and legal object,
		// it should not have problem to be marshalled into bytes.
		// The error here is probably caused by the abnormal HTTP connection,
		// e.g., broken pipe, so we can only write the error response once,
		// to avoid endless circular calling.
		serverError := Errored(http.StatusInternalServerError, err)
		if err = json.NewEncoder(w).Encode(v1.AdmissionReview{Response: &serverError.AdmissionResponse}); err != nil {
			wh.log.Error(err, "still unable to encode and write the InternalServerError response")
		}
	} else {
		res := ar.Response
		if log := wh.log; log.V(1).Enabled() {
			if res.Result != nil {
				log = log.WithValues("code", res.Result.Code, "reason", res.Result.Reason)
			}
			log.V(1).Info("wrote response", "UID", res.UID, "allowed", res.Allowed)
		}
	}
}

// unversionedAdmissionReview is used to decode both v1 and v1beta1 AdmissionReview types.
type unversionedAdmissionReview struct {
	v1.AdmissionReview
}

var _ runtime.Object = &unversionedAdmissionReview{}
