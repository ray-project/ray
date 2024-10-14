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

package predicate

import (
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/event"
	logf "sigs.k8s.io/controller-runtime/pkg/internal/log"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
)

var log = logf.RuntimeLog.WithName("predicate").WithName("eventFilters")

// Predicate filters events before enqueuing the keys.
type Predicate interface {
	// Create returns true if the Create event should be processed
	Create(event.CreateEvent) bool

	// Delete returns true if the Delete event should be processed
	Delete(event.DeleteEvent) bool

	// Update returns true if the Update event should be processed
	Update(event.UpdateEvent) bool

	// Generic returns true if the Generic event should be processed
	Generic(event.GenericEvent) bool
}

var _ Predicate = Funcs{}
var _ Predicate = ResourceVersionChangedPredicate{}
var _ Predicate = GenerationChangedPredicate{}
var _ Predicate = AnnotationChangedPredicate{}
var _ Predicate = or{}
var _ Predicate = and{}

// Funcs is a function that implements Predicate.
type Funcs struct {
	// Create returns true if the Create event should be processed
	CreateFunc func(event.CreateEvent) bool

	// Delete returns true if the Delete event should be processed
	DeleteFunc func(event.DeleteEvent) bool

	// Update returns true if the Update event should be processed
	UpdateFunc func(event.UpdateEvent) bool

	// Generic returns true if the Generic event should be processed
	GenericFunc func(event.GenericEvent) bool
}

// Create implements Predicate.
func (p Funcs) Create(e event.CreateEvent) bool {
	if p.CreateFunc != nil {
		return p.CreateFunc(e)
	}
	return true
}

// Delete implements Predicate.
func (p Funcs) Delete(e event.DeleteEvent) bool {
	if p.DeleteFunc != nil {
		return p.DeleteFunc(e)
	}
	return true
}

// Update implements Predicate.
func (p Funcs) Update(e event.UpdateEvent) bool {
	if p.UpdateFunc != nil {
		return p.UpdateFunc(e)
	}
	return true
}

// Generic implements Predicate.
func (p Funcs) Generic(e event.GenericEvent) bool {
	if p.GenericFunc != nil {
		return p.GenericFunc(e)
	}
	return true
}

// NewPredicateFuncs returns a predicate funcs that applies the given filter function
// on CREATE, UPDATE, DELETE and GENERIC events. For UPDATE events, the filter is applied
// to the new object.
func NewPredicateFuncs(filter func(object client.Object) bool) Funcs {
	return Funcs{
		CreateFunc: func(e event.CreateEvent) bool {
			return filter(e.Object)
		},
		UpdateFunc: func(e event.UpdateEvent) bool {
			return filter(e.ObjectNew)
		},
		DeleteFunc: func(e event.DeleteEvent) bool {
			return filter(e.Object)
		},
		GenericFunc: func(e event.GenericEvent) bool {
			return filter(e.Object)
		},
	}
}

// ResourceVersionChangedPredicate implements a default update predicate function on resource version change.
type ResourceVersionChangedPredicate struct {
	Funcs
}

// Update implements default UpdateEvent filter for validating resource version change.
func (ResourceVersionChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		log.Error(nil, "Update event has no old object to update", "event", e)
		return false
	}
	if e.ObjectNew == nil {
		log.Error(nil, "Update event has no new object to update", "event", e)
		return false
	}

	return e.ObjectNew.GetResourceVersion() != e.ObjectOld.GetResourceVersion()
}

// GenerationChangedPredicate implements a default update predicate function on Generation change.
//
// This predicate will skip update events that have no change in the object's metadata.generation field.
// The metadata.generation field of an object is incremented by the API server when writes are made to the spec field of an object.
// This allows a controller to ignore update events where the spec is unchanged, and only the metadata and/or status fields are changed.
//
// For CustomResource objects the Generation is only incremented when the status subresource is enabled.
//
// Caveats:
//
// * The assumption that the Generation is incremented only on writing to the spec does not hold for all APIs.
// E.g For Deployment objects the Generation is also incremented on writes to the metadata.annotations field.
// For object types other than CustomResources be sure to verify which fields will trigger a Generation increment when they are written to.
//
// * With this predicate, any update events with writes only to the status field will not be reconciled.
// So in the event that the status block is overwritten or wiped by someone else the controller will not self-correct to restore the correct status.
type GenerationChangedPredicate struct {
	Funcs
}

// Update implements default UpdateEvent filter for validating generation change.
func (GenerationChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		log.Error(nil, "Update event has no old object to update", "event", e)
		return false
	}
	if e.ObjectNew == nil {
		log.Error(nil, "Update event has no new object for update", "event", e)
		return false
	}

	return e.ObjectNew.GetGeneration() != e.ObjectOld.GetGeneration()
}

// AnnotationChangedPredicate implements a default update predicate function on annotation change.
//
// This predicate will skip update events that have no change in the object's annotation.
// It is intended to be used in conjunction with the GenerationChangedPredicate, as in the following example:
//
//	Controller.Watch(
//		&source.Kind{Type: v1.MyCustomKind},
//		&handler.EnqueueRequestForObject{},
//		predicate.Or(predicate.GenerationChangedPredicate{}, predicate.AnnotationChangedPredicate{}))
//
// This is mostly useful for controllers that needs to trigger both when the resource's generation is incremented
// (i.e., when the resource' .spec changes), or an annotation changes (e.g., for a staging/alpha API).
type AnnotationChangedPredicate struct {
	Funcs
}

// Update implements default UpdateEvent filter for validating annotation change.
func (AnnotationChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		log.Error(nil, "Update event has no old object to update", "event", e)
		return false
	}
	if e.ObjectNew == nil {
		log.Error(nil, "Update event has no new object for update", "event", e)
		return false
	}

	return !reflect.DeepEqual(e.ObjectNew.GetAnnotations(), e.ObjectOld.GetAnnotations())
}

// LabelChangedPredicate implements a default update predicate function on label change.
//
// This predicate will skip update events that have no change in the object's label.
// It is intended to be used in conjunction with the GenerationChangedPredicate, as in the following example:
//
// Controller.Watch(
//
//	&source.Kind{Type: v1.MyCustomKind},
//	&handler.EnqueueRequestForObject{},
//	predicate.Or(predicate.GenerationChangedPredicate{}, predicate.LabelChangedPredicate{}))
//
// This will be helpful when object's labels is carrying some extra specification information beyond object's spec,
// and the controller will be triggered if any valid spec change (not only in spec, but also in labels) happens.
type LabelChangedPredicate struct {
	Funcs
}

// Update implements default UpdateEvent filter for checking label change.
func (LabelChangedPredicate) Update(e event.UpdateEvent) bool {
	if e.ObjectOld == nil {
		log.Error(nil, "Update event has no old object to update", "event", e)
		return false
	}
	if e.ObjectNew == nil {
		log.Error(nil, "Update event has no new object for update", "event", e)
		return false
	}

	return !reflect.DeepEqual(e.ObjectNew.GetLabels(), e.ObjectOld.GetLabels())
}

// And returns a composite predicate that implements a logical AND of the predicates passed to it.
func And(predicates ...Predicate) Predicate {
	return and{predicates}
}

type and struct {
	predicates []Predicate
}

func (a and) InjectFunc(f inject.Func) error {
	for _, p := range a.predicates {
		if err := f(p); err != nil {
			return err
		}
	}
	return nil
}

func (a and) Create(e event.CreateEvent) bool {
	for _, p := range a.predicates {
		if !p.Create(e) {
			return false
		}
	}
	return true
}

func (a and) Update(e event.UpdateEvent) bool {
	for _, p := range a.predicates {
		if !p.Update(e) {
			return false
		}
	}
	return true
}

func (a and) Delete(e event.DeleteEvent) bool {
	for _, p := range a.predicates {
		if !p.Delete(e) {
			return false
		}
	}
	return true
}

func (a and) Generic(e event.GenericEvent) bool {
	for _, p := range a.predicates {
		if !p.Generic(e) {
			return false
		}
	}
	return true
}

// Or returns a composite predicate that implements a logical OR of the predicates passed to it.
func Or(predicates ...Predicate) Predicate {
	return or{predicates}
}

type or struct {
	predicates []Predicate
}

func (o or) InjectFunc(f inject.Func) error {
	for _, p := range o.predicates {
		if err := f(p); err != nil {
			return err
		}
	}
	return nil
}

func (o or) Create(e event.CreateEvent) bool {
	for _, p := range o.predicates {
		if p.Create(e) {
			return true
		}
	}
	return false
}

func (o or) Update(e event.UpdateEvent) bool {
	for _, p := range o.predicates {
		if p.Update(e) {
			return true
		}
	}
	return false
}

func (o or) Delete(e event.DeleteEvent) bool {
	for _, p := range o.predicates {
		if p.Delete(e) {
			return true
		}
	}
	return false
}

func (o or) Generic(e event.GenericEvent) bool {
	for _, p := range o.predicates {
		if p.Generic(e) {
			return true
		}
	}
	return false
}

// LabelSelectorPredicate constructs a Predicate from a LabelSelector.
// Only objects matching the LabelSelector will be admitted.
func LabelSelectorPredicate(s metav1.LabelSelector) (Predicate, error) {
	selector, err := metav1.LabelSelectorAsSelector(&s)
	if err != nil {
		return Funcs{}, err
	}
	return NewPredicateFuncs(func(o client.Object) bool {
		return selector.Matches(labels.Set(o.GetLabels()))
	}), nil
}
