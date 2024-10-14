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

/*
Package conversion provides implementation for CRD conversion webhook that implements handler for version conversion requests for types that are convertible.

See pkg/conversion for interface definitions required to ensure an API Type is convertible.
*/
package conversion

import (
	"encoding/json"
	"fmt"
	"net/http"

	apix "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/conversion"
	logf "sigs.k8s.io/controller-runtime/pkg/log"
)

var (
	log = logf.Log.WithName("conversion-webhook")
)

// Webhook implements a CRD conversion webhook HTTP handler.
type Webhook struct {
	scheme  *runtime.Scheme
	decoder *Decoder
}

// InjectScheme injects a scheme into the webhook, in order to construct a Decoder.
func (wh *Webhook) InjectScheme(s *runtime.Scheme) error {
	var err error
	wh.scheme = s
	wh.decoder, err = NewDecoder(s)
	if err != nil {
		return err
	}

	return nil
}

// ensure Webhook implements http.Handler
var _ http.Handler = &Webhook{}

func (wh *Webhook) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	convertReview := &apix.ConversionReview{}
	err := json.NewDecoder(r.Body).Decode(convertReview)
	if err != nil {
		log.Error(err, "failed to read conversion request")
		w.WriteHeader(http.StatusBadRequest)
		return
	}

	// TODO(droot): may be move the conversion logic to a separate module to
	// decouple it from the http layer ?
	resp, err := wh.handleConvertRequest(convertReview.Request)
	if err != nil {
		log.Error(err, "failed to convert", "request", convertReview.Request.UID)
		convertReview.Response = errored(err)
	} else {
		convertReview.Response = resp
	}
	convertReview.Response.UID = convertReview.Request.UID
	convertReview.Request = nil

	err = json.NewEncoder(w).Encode(convertReview)
	if err != nil {
		log.Error(err, "failed to write response")
		return
	}
}

// handles a version conversion request.
func (wh *Webhook) handleConvertRequest(req *apix.ConversionRequest) (*apix.ConversionResponse, error) {
	if req == nil {
		return nil, fmt.Errorf("conversion request is nil")
	}
	var objects []runtime.RawExtension

	for _, obj := range req.Objects {
		src, gvk, err := wh.decoder.Decode(obj.Raw)
		if err != nil {
			return nil, err
		}
		dst, err := wh.allocateDstObject(req.DesiredAPIVersion, gvk.Kind)
		if err != nil {
			return nil, err
		}
		err = wh.convertObject(src, dst)
		if err != nil {
			return nil, err
		}
		objects = append(objects, runtime.RawExtension{Object: dst})
	}
	return &apix.ConversionResponse{
		UID:              req.UID,
		ConvertedObjects: objects,
		Result: metav1.Status{
			Status: metav1.StatusSuccess,
		},
	}, nil
}

// convertObject will convert given a src object to dst object.
// Note(droot): couldn't find a way to reduce the cyclomatic complexity under 10
// without compromising readability, so disabling gocyclo linter
func (wh *Webhook) convertObject(src, dst runtime.Object) error {
	srcGVK := src.GetObjectKind().GroupVersionKind()
	dstGVK := dst.GetObjectKind().GroupVersionKind()

	if srcGVK.GroupKind() != dstGVK.GroupKind() {
		return fmt.Errorf("src %T and dst %T does not belong to same API Group", src, dst)
	}

	if srcGVK == dstGVK {
		return fmt.Errorf("conversion is not allowed between same type %T", src)
	}

	srcIsHub, dstIsHub := isHub(src), isHub(dst)
	srcIsConvertible, dstIsConvertible := isConvertible(src), isConvertible(dst)

	switch {
	case srcIsHub && dstIsConvertible:
		return dst.(conversion.Convertible).ConvertFrom(src.(conversion.Hub))
	case dstIsHub && srcIsConvertible:
		return src.(conversion.Convertible).ConvertTo(dst.(conversion.Hub))
	case srcIsConvertible && dstIsConvertible:
		return wh.convertViaHub(src.(conversion.Convertible), dst.(conversion.Convertible))
	default:
		return fmt.Errorf("%T is not convertible to %T", src, dst)
	}
}

func (wh *Webhook) convertViaHub(src, dst conversion.Convertible) error {
	hub, err := wh.getHub(src)
	if err != nil {
		return err
	}

	if hub == nil {
		return fmt.Errorf("%s does not have any Hub defined", src)
	}

	err = src.ConvertTo(hub)
	if err != nil {
		return fmt.Errorf("%T failed to convert to hub version %T : %w", src, hub, err)
	}

	err = dst.ConvertFrom(hub)
	if err != nil {
		return fmt.Errorf("%T failed to convert from hub version %T : %w", dst, hub, err)
	}

	return nil
}

// getHub returns an instance of the Hub for passed-in object's group/kind.
func (wh *Webhook) getHub(obj runtime.Object) (conversion.Hub, error) {
	gvks, err := objectGVKs(wh.scheme, obj)
	if err != nil {
		return nil, err
	}
	if len(gvks) == 0 {
		return nil, fmt.Errorf("error retrieving gvks for object : %v", obj)
	}

	var hub conversion.Hub
	var hubFoundAlready bool
	for _, gvk := range gvks {
		instance, err := wh.scheme.New(gvk)
		if err != nil {
			return nil, fmt.Errorf("failed to allocate an instance for gvk %v: %w", gvk, err)
		}
		if val, isHub := instance.(conversion.Hub); isHub {
			if hubFoundAlready {
				return nil, fmt.Errorf("multiple hub version defined for %T", obj)
			}
			hubFoundAlready = true
			hub = val
		}
	}
	return hub, nil
}

// allocateDstObject returns an instance for a given GVK.
func (wh *Webhook) allocateDstObject(apiVersion, kind string) (runtime.Object, error) {
	gvk := schema.FromAPIVersionAndKind(apiVersion, kind)

	obj, err := wh.scheme.New(gvk)
	if err != nil {
		return obj, err
	}

	t, err := meta.TypeAccessor(obj)
	if err != nil {
		return obj, err
	}

	t.SetAPIVersion(apiVersion)
	t.SetKind(kind)

	return obj, nil
}

// IsConvertible determines if given type is convertible or not. For a type
// to be convertible, the group-kind needs to have a Hub type defined and all
// non-hub types must be able to convert to/from Hub.
func IsConvertible(scheme *runtime.Scheme, obj runtime.Object) (bool, error) {
	var hubs, spokes, nonSpokes []runtime.Object

	gvks, err := objectGVKs(scheme, obj)
	if err != nil {
		return false, err
	}
	if len(gvks) == 0 {
		return false, fmt.Errorf("error retrieving gvks for object : %v", obj)
	}

	for _, gvk := range gvks {
		instance, err := scheme.New(gvk)
		if err != nil {
			return false, fmt.Errorf("failed to allocate an instance for gvk %v: %w", gvk, err)
		}

		if isHub(instance) {
			hubs = append(hubs, instance)
			continue
		}

		if !isConvertible(instance) {
			nonSpokes = append(nonSpokes, instance)
			continue
		}

		spokes = append(spokes, instance)
	}

	if len(gvks) == 1 {
		return false, nil // single version
	}

	if len(hubs) == 0 && len(spokes) == 0 {
		// multiple version detected with no conversion implementation. This is
		// true for multi-version built-in types.
		return false, nil
	}

	if len(hubs) == 1 && len(nonSpokes) == 0 { // convertible
		return true, nil
	}

	return false, PartialImplementationError{
		hubs:      hubs,
		nonSpokes: nonSpokes,
		spokes:    spokes,
	}
}

// objectGVKs returns all (Group,Version,Kind) for the Group/Kind of given object.
func objectGVKs(scheme *runtime.Scheme, obj runtime.Object) ([]schema.GroupVersionKind, error) {
	// NB: we should not use `obj.GetObjectKind().GroupVersionKind()` to get the
	// GVK here, since it is parsed from apiVersion and kind fields and it may
	// return empty GVK if obj is an uninitialized object.
	objGVKs, _, err := scheme.ObjectKinds(obj)
	if err != nil {
		return nil, err
	}
	if len(objGVKs) != 1 {
		return nil, fmt.Errorf("expect to get only one GVK for %v", obj)
	}
	objGVK := objGVKs[0]
	knownTypes := scheme.AllKnownTypes()

	var gvks []schema.GroupVersionKind
	for gvk := range knownTypes {
		if objGVK.GroupKind() == gvk.GroupKind() {
			gvks = append(gvks, gvk)
		}
	}
	return gvks, nil
}

// PartialImplementationError represents an error due to partial conversion
// implementation such as hub without spokes, multiple hubs or spokes without hub.
type PartialImplementationError struct {
	gvk       schema.GroupVersionKind
	hubs      []runtime.Object
	nonSpokes []runtime.Object
	spokes    []runtime.Object
}

func (e PartialImplementationError) Error() string {
	if len(e.hubs) == 0 {
		return fmt.Sprintf("no hub defined for gvk %s", e.gvk)
	}
	if len(e.hubs) > 1 {
		return fmt.Sprintf("multiple(%d) hubs defined for group-kind '%s' ",
			len(e.hubs), e.gvk.GroupKind())
	}
	if len(e.nonSpokes) > 0 {
		return fmt.Sprintf("%d inconvertible types detected for group-kind '%s'",
			len(e.nonSpokes), e.gvk.GroupKind())
	}
	return ""
}

// isHub determines if passed-in object is a Hub or not.
func isHub(obj runtime.Object) bool {
	_, yes := obj.(conversion.Hub)
	return yes
}

// isConvertible determines if passed-in object is a convertible.
func isConvertible(obj runtime.Object) bool {
	_, yes := obj.(conversion.Convertible)
	return yes
}

// helper to construct error response.
func errored(err error) *apix.ConversionResponse {
	return &apix.ConversionResponse{
		Result: metav1.Status{
			Status:  metav1.StatusFailure,
			Message: err.Error(),
		},
	}
}
