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
	"errors"
	"fmt"
	"net/http"

	"github.com/go-logr/logr"
	jsonpatch "gomodules.xyz/jsonpatch/v2"
	admissionv1 "k8s.io/api/admission/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/json"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	"k8s.io/client-go/kubernetes/scheme"

	logf "sigs.k8s.io/controller-runtime/pkg/internal/log"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/webhook/internal/metrics"
)

var (
	errUnableToEncodeResponse = errors.New("unable to encode response")
)

// Request defines the input for an admission handler.
// It contains information to identify the object in
// question (group, version, kind, resource, subresource,
// name, namespace), as well as the operation in question
// (e.g. Get, Create, etc), and the object itself.
type Request struct {
	admissionv1.AdmissionRequest
}

// Response is the output of an admission handler.
// It contains a response indicating if a given
// operation is allowed, as well as a set of patches
// to mutate the object in the case of a mutating admission handler.
type Response struct {
	// Patches are the JSON patches for mutating webhooks.
	// Using this instead of setting Response.Patch to minimize
	// overhead of serialization and deserialization.
	// Patches set here will override any patches in the response,
	// so leave this empty if you want to set the patch response directly.
	Patches []jsonpatch.JsonPatchOperation
	// AdmissionResponse is the raw admission response.
	// The Patch field in it will be overwritten by the listed patches.
	admissionv1.AdmissionResponse
}

// Complete populates any fields that are yet to be set in
// the underlying AdmissionResponse, It mutates the response.
func (r *Response) Complete(req Request) error {
	r.UID = req.UID

	// ensure that we have a valid status code
	if r.Result == nil {
		r.Result = &metav1.Status{}
	}
	if r.Result.Code == 0 {
		r.Result.Code = http.StatusOK
	}
	// TODO(directxman12): do we need to populate this further, and/or
	// is code actually necessary (the same webhook doesn't use it)

	if len(r.Patches) == 0 {
		return nil
	}

	var err error
	r.Patch, err = json.Marshal(r.Patches)
	if err != nil {
		return err
	}
	patchType := admissionv1.PatchTypeJSONPatch
	r.PatchType = &patchType

	return nil
}

// Handler can handle an AdmissionRequest.
type Handler interface {
	// Handle yields a response to an AdmissionRequest.
	//
	// The supplied context is extracted from the received http.Request, allowing wrapping
	// http.Handlers to inject values into and control cancelation of downstream request processing.
	Handle(context.Context, Request) Response
}

// HandlerFunc implements Handler interface using a single function.
type HandlerFunc func(context.Context, Request) Response

var _ Handler = HandlerFunc(nil)

// Handle process the AdmissionRequest by invoking the underlying function.
func (f HandlerFunc) Handle(ctx context.Context, req Request) Response {
	return f(ctx, req)
}

// Webhook represents each individual webhook.
//
// It must be registered with a webhook.Server or
// populated by StandaloneWebhook to be ran on an arbitrary HTTP server.
type Webhook struct {
	// Handler actually processes an admission request returning whether it was allowed or denied,
	// and potentially patches to apply to the handler.
	Handler Handler

	// RecoverPanic indicates whether the panic caused by webhook should be recovered.
	RecoverPanic bool

	// WithContextFunc will allow you to take the http.Request.Context() and
	// add any additional information such as passing the request path or
	// headers thus allowing you to read them from within the handler
	WithContextFunc func(context.Context, *http.Request) context.Context

	// decoder is constructed on receiving a scheme and passed down to then handler
	decoder *Decoder

	log logr.Logger
}

// InjectLogger gets a handle to a logging instance, hopefully with more info about this particular webhook.
func (wh *Webhook) InjectLogger(l logr.Logger) error {
	wh.log = l
	return nil
}

// WithRecoverPanic takes a bool flag which indicates whether the panic caused by webhook should be recovered.
func (wh *Webhook) WithRecoverPanic(recoverPanic bool) *Webhook {
	wh.RecoverPanic = recoverPanic
	return wh
}

// Handle processes AdmissionRequest.
// If the webhook is mutating type, it delegates the AdmissionRequest to each handler and merge the patches.
// If the webhook is validating type, it delegates the AdmissionRequest to each handler and
// deny the request if anyone denies.
func (wh *Webhook) Handle(ctx context.Context, req Request) (response Response) {
	if wh.RecoverPanic {
		defer func() {
			if r := recover(); r != nil {
				for _, fn := range utilruntime.PanicHandlers {
					fn(r)
				}
				response = Errored(http.StatusInternalServerError, fmt.Errorf("panic: %v [recovered]", r))
				return
			}
		}()
	}

	resp := wh.Handler.Handle(ctx, req)
	if err := resp.Complete(req); err != nil {
		wh.log.Error(err, "unable to encode response")
		return Errored(http.StatusInternalServerError, errUnableToEncodeResponse)
	}

	return resp
}

// InjectScheme injects a scheme into the webhook, in order to construct a Decoder.
func (wh *Webhook) InjectScheme(s *runtime.Scheme) error {
	// TODO(directxman12): we should have a better way to pass this down

	var err error
	wh.decoder, err = NewDecoder(s)
	if err != nil {
		return err
	}

	// inject the decoder here too, just in case the order of calling this is not
	// scheme first, then inject func
	if wh.Handler != nil {
		if _, err := InjectDecoderInto(wh.GetDecoder(), wh.Handler); err != nil {
			return err
		}
	}

	return nil
}

// GetDecoder returns a decoder to decode the objects embedded in admission requests.
// It may be nil if we haven't received a scheme to use to determine object types yet.
func (wh *Webhook) GetDecoder() *Decoder {
	return wh.decoder
}

// InjectFunc injects the field setter into the webhook.
func (wh *Webhook) InjectFunc(f inject.Func) error {
	// inject directly into the handlers.  It would be more correct
	// to do this in a sync.Once in Handle (since we don't have some
	// other start/finalize-type method), but it's more efficient to
	// do it here, presumably.

	// also inject a decoder, and wrap this so that we get a setFields
	// that injects a decoder (hopefully things don't ignore the duplicate
	// InjectorInto call).

	var setFields inject.Func
	setFields = func(target interface{}) error {
		if err := f(target); err != nil {
			return err
		}

		if _, err := inject.InjectorInto(setFields, target); err != nil {
			return err
		}

		if _, err := InjectDecoderInto(wh.GetDecoder(), target); err != nil {
			return err
		}

		return nil
	}

	return setFields(wh.Handler)
}

// StandaloneOptions let you configure a StandaloneWebhook.
type StandaloneOptions struct {
	// Scheme is the scheme used to resolve runtime.Objects to GroupVersionKinds / Resources
	// Defaults to the kubernetes/client-go scheme.Scheme, but it's almost always better
	// idea to pass your own scheme in.  See the documentation in pkg/scheme for more information.
	Scheme *runtime.Scheme
	// Logger to be used by the webhook.
	// If none is set, it defaults to log.Log global logger.
	Logger logr.Logger
	// MetricsPath is used for labelling prometheus metrics
	// by the path is served on.
	// If none is set, prometheus metrics will not be generated.
	MetricsPath string
}

// StandaloneWebhook prepares a webhook for use without a webhook.Server,
// passing in the information normally populated by webhook.Server
// and instrumenting the webhook with metrics.
//
// Use this to attach your webhook to an arbitrary HTTP server or mux.
//
// Note that you are responsible for terminating TLS if you use StandaloneWebhook
// in your own server/mux. In order to be accessed by a kubernetes cluster,
// all webhook servers require TLS.
func StandaloneWebhook(hook *Webhook, opts StandaloneOptions) (http.Handler, error) {
	if opts.Scheme == nil {
		opts.Scheme = scheme.Scheme
	}

	if err := hook.InjectScheme(opts.Scheme); err != nil {
		return nil, err
	}

	if opts.Logger.GetSink() == nil {
		opts.Logger = logf.RuntimeLog.WithName("webhook")
	}
	hook.log = opts.Logger

	if opts.MetricsPath == "" {
		return hook, nil
	}
	return metrics.InstrumentedHook(opts.MetricsPath, hook), nil
}

// requestContextKey is how we find the admission.Request in a context.Context.
type requestContextKey struct{}

// RequestFromContext returns an admission.Request from ctx.
func RequestFromContext(ctx context.Context) (Request, error) {
	if v, ok := ctx.Value(requestContextKey{}).(Request); ok {
		return v, nil
	}

	return Request{}, errors.New("admission.Request not found in context")
}

// NewContextWithRequest returns a new Context, derived from ctx, which carries the
// provided admission.Request.
func NewContextWithRequest(ctx context.Context, req Request) context.Context {
	return context.WithValue(ctx, requestContextKey{}, req)
}
