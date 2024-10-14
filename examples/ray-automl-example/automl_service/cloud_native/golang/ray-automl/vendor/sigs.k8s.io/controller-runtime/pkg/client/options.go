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

package client

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/selection"
)

// {{{ "Functional" Option Interfaces

// CreateOption is some configuration that modifies options for a create request.
type CreateOption interface {
	// ApplyToCreate applies this configuration to the given create options.
	ApplyToCreate(*CreateOptions)
}

// DeleteOption is some configuration that modifies options for a delete request.
type DeleteOption interface {
	// ApplyToDelete applies this configuration to the given delete options.
	ApplyToDelete(*DeleteOptions)
}

// GetOption is some configuration that modifies options for a get request.
type GetOption interface {
	// ApplyToGet applies this configuration to the given get options.
	ApplyToGet(*GetOptions)
}

// ListOption is some configuration that modifies options for a list request.
type ListOption interface {
	// ApplyToList applies this configuration to the given list options.
	ApplyToList(*ListOptions)
}

// UpdateOption is some configuration that modifies options for a update request.
type UpdateOption interface {
	// ApplyToUpdate applies this configuration to the given update options.
	ApplyToUpdate(*UpdateOptions)
}

// PatchOption is some configuration that modifies options for a patch request.
type PatchOption interface {
	// ApplyToPatch applies this configuration to the given patch options.
	ApplyToPatch(*PatchOptions)
}

// DeleteAllOfOption is some configuration that modifies options for a delete request.
type DeleteAllOfOption interface {
	// ApplyToDeleteAllOf applies this configuration to the given deletecollection options.
	ApplyToDeleteAllOf(*DeleteAllOfOptions)
}

// }}}

// {{{ Multi-Type Options

// DryRunAll sets the "dry run" option to "all", executing all
// validation, etc without persisting the change to storage.
var DryRunAll = dryRunAll{}

type dryRunAll struct{}

// ApplyToCreate applies this configuration to the given create options.
func (dryRunAll) ApplyToCreate(opts *CreateOptions) {
	opts.DryRun = []string{metav1.DryRunAll}
}

// ApplyToUpdate applies this configuration to the given update options.
func (dryRunAll) ApplyToUpdate(opts *UpdateOptions) {
	opts.DryRun = []string{metav1.DryRunAll}
}

// ApplyToPatch applies this configuration to the given patch options.
func (dryRunAll) ApplyToPatch(opts *PatchOptions) {
	opts.DryRun = []string{metav1.DryRunAll}
}

// ApplyToPatch applies this configuration to the given delete options.
func (dryRunAll) ApplyToDelete(opts *DeleteOptions) {
	opts.DryRun = []string{metav1.DryRunAll}
}
func (dryRunAll) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	opts.DryRun = []string{metav1.DryRunAll}
}

// FieldOwner set the field manager name for the given server-side apply patch.
type FieldOwner string

// ApplyToPatch applies this configuration to the given patch options.
func (f FieldOwner) ApplyToPatch(opts *PatchOptions) {
	opts.FieldManager = string(f)
}

// ApplyToCreate applies this configuration to the given create options.
func (f FieldOwner) ApplyToCreate(opts *CreateOptions) {
	opts.FieldManager = string(f)
}

// ApplyToUpdate applies this configuration to the given update options.
func (f FieldOwner) ApplyToUpdate(opts *UpdateOptions) {
	opts.FieldManager = string(f)
}

// }}}

// {{{ Create Options

// CreateOptions contains options for create requests. It's generally a subset
// of metav1.CreateOptions.
type CreateOptions struct {
	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	DryRun []string

	// FieldManager is the name of the user or component submitting
	// this request.  It must be set with server-side apply.
	FieldManager string

	// Raw represents raw CreateOptions, as passed to the API server.
	Raw *metav1.CreateOptions
}

// AsCreateOptions returns these options as a metav1.CreateOptions.
// This may mutate the Raw field.
func (o *CreateOptions) AsCreateOptions() *metav1.CreateOptions {
	if o == nil {
		return &metav1.CreateOptions{}
	}
	if o.Raw == nil {
		o.Raw = &metav1.CreateOptions{}
	}

	o.Raw.DryRun = o.DryRun
	o.Raw.FieldManager = o.FieldManager
	return o.Raw
}

// ApplyOptions applies the given create options on these options,
// and then returns itself (for convenient chaining).
func (o *CreateOptions) ApplyOptions(opts []CreateOption) *CreateOptions {
	for _, opt := range opts {
		opt.ApplyToCreate(o)
	}
	return o
}

// ApplyToCreate implements CreateOption.
func (o *CreateOptions) ApplyToCreate(co *CreateOptions) {
	if o.DryRun != nil {
		co.DryRun = o.DryRun
	}
	if o.FieldManager != "" {
		co.FieldManager = o.FieldManager
	}
	if o.Raw != nil {
		co.Raw = o.Raw
	}
}

var _ CreateOption = &CreateOptions{}

// }}}

// {{{ Delete Options

// DeleteOptions contains options for delete requests. It's generally a subset
// of metav1.DeleteOptions.
type DeleteOptions struct {
	// GracePeriodSeconds is the duration in seconds before the object should be
	// deleted. Value must be non-negative integer. The value zero indicates
	// delete immediately. If this value is nil, the default grace period for the
	// specified type will be used.
	GracePeriodSeconds *int64

	// Preconditions must be fulfilled before a deletion is carried out. If not
	// possible, a 409 Conflict status will be returned.
	Preconditions *metav1.Preconditions

	// PropagationPolicy determined whether and how garbage collection will be
	// performed. Either this field or OrphanDependents may be set, but not both.
	// The default policy is decided by the existing finalizer set in the
	// metadata.finalizers and the resource-specific default policy.
	// Acceptable values are: 'Orphan' - orphan the dependents; 'Background' -
	// allow the garbage collector to delete the dependents in the background;
	// 'Foreground' - a cascading policy that deletes all dependents in the
	// foreground.
	PropagationPolicy *metav1.DeletionPropagation

	// Raw represents raw DeleteOptions, as passed to the API server.
	Raw *metav1.DeleteOptions

	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	DryRun []string
}

// AsDeleteOptions returns these options as a metav1.DeleteOptions.
// This may mutate the Raw field.
func (o *DeleteOptions) AsDeleteOptions() *metav1.DeleteOptions {
	if o == nil {
		return &metav1.DeleteOptions{}
	}
	if o.Raw == nil {
		o.Raw = &metav1.DeleteOptions{}
	}

	o.Raw.GracePeriodSeconds = o.GracePeriodSeconds
	o.Raw.Preconditions = o.Preconditions
	o.Raw.PropagationPolicy = o.PropagationPolicy
	o.Raw.DryRun = o.DryRun
	return o.Raw
}

// ApplyOptions applies the given delete options on these options,
// and then returns itself (for convenient chaining).
func (o *DeleteOptions) ApplyOptions(opts []DeleteOption) *DeleteOptions {
	for _, opt := range opts {
		opt.ApplyToDelete(o)
	}
	return o
}

var _ DeleteOption = &DeleteOptions{}

// ApplyToDelete implements DeleteOption.
func (o *DeleteOptions) ApplyToDelete(do *DeleteOptions) {
	if o.GracePeriodSeconds != nil {
		do.GracePeriodSeconds = o.GracePeriodSeconds
	}
	if o.Preconditions != nil {
		do.Preconditions = o.Preconditions
	}
	if o.PropagationPolicy != nil {
		do.PropagationPolicy = o.PropagationPolicy
	}
	if o.Raw != nil {
		do.Raw = o.Raw
	}
	if o.DryRun != nil {
		do.DryRun = o.DryRun
	}
}

// GracePeriodSeconds sets the grace period for the deletion
// to the given number of seconds.
type GracePeriodSeconds int64

// ApplyToDelete applies this configuration to the given delete options.
func (s GracePeriodSeconds) ApplyToDelete(opts *DeleteOptions) {
	secs := int64(s)
	opts.GracePeriodSeconds = &secs
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (s GracePeriodSeconds) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	s.ApplyToDelete(&opts.DeleteOptions)
}

// Preconditions must be fulfilled before an operation (update, delete, etc.) is carried out.
type Preconditions metav1.Preconditions

// ApplyToDelete applies this configuration to the given delete options.
func (p Preconditions) ApplyToDelete(opts *DeleteOptions) {
	preconds := metav1.Preconditions(p)
	opts.Preconditions = &preconds
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (p Preconditions) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	p.ApplyToDelete(&opts.DeleteOptions)
}

// PropagationPolicy determined whether and how garbage collection will be
// performed. Either this field or OrphanDependents may be set, but not both.
// The default policy is decided by the existing finalizer set in the
// metadata.finalizers and the resource-specific default policy.
// Acceptable values are: 'Orphan' - orphan the dependents; 'Background' -
// allow the garbage collector to delete the dependents in the background;
// 'Foreground' - a cascading policy that deletes all dependents in the
// foreground.
type PropagationPolicy metav1.DeletionPropagation

// ApplyToDelete applies the given delete options on these options.
// It will propagate to the dependents of the object to let the garbage collector handle it.
func (p PropagationPolicy) ApplyToDelete(opts *DeleteOptions) {
	policy := metav1.DeletionPropagation(p)
	opts.PropagationPolicy = &policy
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (p PropagationPolicy) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	p.ApplyToDelete(&opts.DeleteOptions)
}

// }}}

// {{{ Get Options

// GetOptions contains options for get operation.
// Now it only has a Raw field, with support for specific resourceVersion.
type GetOptions struct {
	// Raw represents raw GetOptions, as passed to the API server.  Note
	// that these may not be respected by all implementations of interface.
	Raw *metav1.GetOptions
}

var _ GetOption = &GetOptions{}

// ApplyToGet implements GetOption for GetOptions.
func (o *GetOptions) ApplyToGet(lo *GetOptions) {
	if o.Raw != nil {
		lo.Raw = o.Raw
	}
}

// AsGetOptions returns these options as a flattened metav1.GetOptions.
// This may mutate the Raw field.
func (o *GetOptions) AsGetOptions() *metav1.GetOptions {
	if o == nil || o.Raw == nil {
		return &metav1.GetOptions{}
	}
	return o.Raw
}

// ApplyOptions applies the given get options on these options,
// and then returns itself (for convenient chaining).
func (o *GetOptions) ApplyOptions(opts []GetOption) *GetOptions {
	for _, opt := range opts {
		opt.ApplyToGet(o)
	}
	return o
}

// }}}

// {{{ List Options

// ListOptions contains options for limiting or filtering results.
// It's generally a subset of metav1.ListOptions, with support for
// pre-parsed selectors (since generally, selectors will be executed
// against the cache).
type ListOptions struct {
	// LabelSelector filters results by label. Use labels.Parse() to
	// set from raw string form.
	LabelSelector labels.Selector
	// FieldSelector filters results by a particular field.  In order
	// to use this with cache-based implementations, restrict usage to
	// a single field-value pair that's been added to the indexers.
	FieldSelector fields.Selector

	// Namespace represents the namespace to list for, or empty for
	// non-namespaced objects, or to list across all namespaces.
	Namespace string

	// Limit specifies the maximum number of results to return from the server. The server may
	// not support this field on all resource types, but if it does and more results remain it
	// will set the continue field on the returned list object. This field is not supported if watch
	// is true in the Raw ListOptions.
	Limit int64
	// Continue is a token returned by the server that lets a client retrieve chunks of results
	// from the server by specifying limit. The server may reject requests for continuation tokens
	// it does not recognize and will return a 410 error if the token can no longer be used because
	// it has expired. This field is not supported if watch is true in the Raw ListOptions.
	Continue string

	// Raw represents raw ListOptions, as passed to the API server.  Note
	// that these may not be respected by all implementations of interface,
	// and the LabelSelector, FieldSelector, Limit and Continue fields are ignored.
	Raw *metav1.ListOptions
}

var _ ListOption = &ListOptions{}

// ApplyToList implements ListOption for ListOptions.
func (o *ListOptions) ApplyToList(lo *ListOptions) {
	if o.LabelSelector != nil {
		lo.LabelSelector = o.LabelSelector
	}
	if o.FieldSelector != nil {
		lo.FieldSelector = o.FieldSelector
	}
	if o.Namespace != "" {
		lo.Namespace = o.Namespace
	}
	if o.Raw != nil {
		lo.Raw = o.Raw
	}
	if o.Limit > 0 {
		lo.Limit = o.Limit
	}
	if o.Continue != "" {
		lo.Continue = o.Continue
	}
}

// AsListOptions returns these options as a flattened metav1.ListOptions.
// This may mutate the Raw field.
func (o *ListOptions) AsListOptions() *metav1.ListOptions {
	if o == nil {
		return &metav1.ListOptions{}
	}
	if o.Raw == nil {
		o.Raw = &metav1.ListOptions{}
	}
	if o.LabelSelector != nil {
		o.Raw.LabelSelector = o.LabelSelector.String()
	}
	if o.FieldSelector != nil {
		o.Raw.FieldSelector = o.FieldSelector.String()
	}
	if !o.Raw.Watch {
		o.Raw.Limit = o.Limit
		o.Raw.Continue = o.Continue
	}
	return o.Raw
}

// ApplyOptions applies the given list options on these options,
// and then returns itself (for convenient chaining).
func (o *ListOptions) ApplyOptions(opts []ListOption) *ListOptions {
	for _, opt := range opts {
		opt.ApplyToList(o)
	}
	return o
}

// MatchingLabels filters the list/delete operation on the given set of labels.
type MatchingLabels map[string]string

// ApplyToList applies this configuration to the given list options.
func (m MatchingLabels) ApplyToList(opts *ListOptions) {
	// TODO(directxman12): can we avoid reserializing this over and over?
	sel := labels.SelectorFromValidatedSet(map[string]string(m))
	opts.LabelSelector = sel
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (m MatchingLabels) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	m.ApplyToList(&opts.ListOptions)
}

// HasLabels filters the list/delete operation checking if the set of labels exists
// without checking their values.
type HasLabels []string

// ApplyToList applies this configuration to the given list options.
func (m HasLabels) ApplyToList(opts *ListOptions) {
	sel := labels.NewSelector()
	for _, label := range m {
		r, err := labels.NewRequirement(label, selection.Exists, nil)
		if err == nil {
			sel = sel.Add(*r)
		}
	}
	opts.LabelSelector = sel
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (m HasLabels) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	m.ApplyToList(&opts.ListOptions)
}

// MatchingLabelsSelector filters the list/delete operation on the given label
// selector (or index in the case of cached lists). A struct is used because
// labels.Selector is an interface, which cannot be aliased.
type MatchingLabelsSelector struct {
	labels.Selector
}

// ApplyToList applies this configuration to the given list options.
func (m MatchingLabelsSelector) ApplyToList(opts *ListOptions) {
	opts.LabelSelector = m
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (m MatchingLabelsSelector) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	m.ApplyToList(&opts.ListOptions)
}

// MatchingFields filters the list/delete operation on the given field Set
// (or index in the case of cached lists).
type MatchingFields fields.Set

// ApplyToList applies this configuration to the given list options.
func (m MatchingFields) ApplyToList(opts *ListOptions) {
	// TODO(directxman12): can we avoid re-serializing this?
	sel := fields.Set(m).AsSelector()
	opts.FieldSelector = sel
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (m MatchingFields) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	m.ApplyToList(&opts.ListOptions)
}

// MatchingFieldsSelector filters the list/delete operation on the given field
// selector (or index in the case of cached lists). A struct is used because
// fields.Selector is an interface, which cannot be aliased.
type MatchingFieldsSelector struct {
	fields.Selector
}

// ApplyToList applies this configuration to the given list options.
func (m MatchingFieldsSelector) ApplyToList(opts *ListOptions) {
	opts.FieldSelector = m
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (m MatchingFieldsSelector) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	m.ApplyToList(&opts.ListOptions)
}

// InNamespace restricts the list/delete operation to the given namespace.
type InNamespace string

// ApplyToList applies this configuration to the given list options.
func (n InNamespace) ApplyToList(opts *ListOptions) {
	opts.Namespace = string(n)
}

// ApplyToDeleteAllOf applies this configuration to the given an List options.
func (n InNamespace) ApplyToDeleteAllOf(opts *DeleteAllOfOptions) {
	n.ApplyToList(&opts.ListOptions)
}

// Limit specifies the maximum number of results to return from the server.
// Limit does not implement DeleteAllOfOption interface because the server
// does not support setting it for deletecollection operations.
type Limit int64

// ApplyToList applies this configuration to the given an list options.
func (l Limit) ApplyToList(opts *ListOptions) {
	opts.Limit = int64(l)
}

// Continue sets a continuation token to retrieve chunks of results when using limit.
// Continue does not implement DeleteAllOfOption interface because the server
// does not support setting it for deletecollection operations.
type Continue string

// ApplyToList applies this configuration to the given an List options.
func (c Continue) ApplyToList(opts *ListOptions) {
	opts.Continue = string(c)
}

// }}}

// {{{ Update Options

// UpdateOptions contains options for create requests. It's generally a subset
// of metav1.UpdateOptions.
type UpdateOptions struct {
	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	DryRun []string

	// FieldManager is the name of the user or component submitting
	// this request.  It must be set with server-side apply.
	FieldManager string

	// Raw represents raw UpdateOptions, as passed to the API server.
	Raw *metav1.UpdateOptions
}

// AsUpdateOptions returns these options as a metav1.UpdateOptions.
// This may mutate the Raw field.
func (o *UpdateOptions) AsUpdateOptions() *metav1.UpdateOptions {
	if o == nil {
		return &metav1.UpdateOptions{}
	}
	if o.Raw == nil {
		o.Raw = &metav1.UpdateOptions{}
	}

	o.Raw.DryRun = o.DryRun
	o.Raw.FieldManager = o.FieldManager
	return o.Raw
}

// ApplyOptions applies the given update options on these options,
// and then returns itself (for convenient chaining).
func (o *UpdateOptions) ApplyOptions(opts []UpdateOption) *UpdateOptions {
	for _, opt := range opts {
		opt.ApplyToUpdate(o)
	}
	return o
}

var _ UpdateOption = &UpdateOptions{}

// ApplyToUpdate implements UpdateOption.
func (o *UpdateOptions) ApplyToUpdate(uo *UpdateOptions) {
	if o.DryRun != nil {
		uo.DryRun = o.DryRun
	}
	if o.FieldManager != "" {
		uo.FieldManager = o.FieldManager
	}
	if o.Raw != nil {
		uo.Raw = o.Raw
	}
}

// }}}

// {{{ Patch Options

// PatchOptions contains options for patch requests.
type PatchOptions struct {
	// When present, indicates that modifications should not be
	// persisted. An invalid or unrecognized dryRun directive will
	// result in an error response and no further processing of the
	// request. Valid values are:
	// - All: all dry run stages will be processed
	DryRun []string

	// Force is going to "force" Apply requests. It means user will
	// re-acquire conflicting fields owned by other people. Force
	// flag must be unset for non-apply patch requests.
	// +optional
	Force *bool

	// FieldManager is the name of the user or component submitting
	// this request.  It must be set with server-side apply.
	FieldManager string

	// Raw represents raw PatchOptions, as passed to the API server.
	Raw *metav1.PatchOptions
}

// ApplyOptions applies the given patch options on these options,
// and then returns itself (for convenient chaining).
func (o *PatchOptions) ApplyOptions(opts []PatchOption) *PatchOptions {
	for _, opt := range opts {
		opt.ApplyToPatch(o)
	}
	return o
}

// AsPatchOptions returns these options as a metav1.PatchOptions.
// This may mutate the Raw field.
func (o *PatchOptions) AsPatchOptions() *metav1.PatchOptions {
	if o == nil {
		return &metav1.PatchOptions{}
	}
	if o.Raw == nil {
		o.Raw = &metav1.PatchOptions{}
	}

	o.Raw.DryRun = o.DryRun
	o.Raw.Force = o.Force
	o.Raw.FieldManager = o.FieldManager
	return o.Raw
}

var _ PatchOption = &PatchOptions{}

// ApplyToPatch implements PatchOptions.
func (o *PatchOptions) ApplyToPatch(po *PatchOptions) {
	if o.DryRun != nil {
		po.DryRun = o.DryRun
	}
	if o.Force != nil {
		po.Force = o.Force
	}
	if o.FieldManager != "" {
		po.FieldManager = o.FieldManager
	}
	if o.Raw != nil {
		po.Raw = o.Raw
	}
}

// ForceOwnership indicates that in case of conflicts with server-side apply,
// the client should acquire ownership of the conflicting field.  Most
// controllers should use this.
var ForceOwnership = forceOwnership{}

type forceOwnership struct{}

func (forceOwnership) ApplyToPatch(opts *PatchOptions) {
	definitelyTrue := true
	opts.Force = &definitelyTrue
}

// }}}

// {{{ DeleteAllOf Options

// these are all just delete options and list options

// DeleteAllOfOptions contains options for deletecollection (deleteallof) requests.
// It's just list and delete options smooshed together.
type DeleteAllOfOptions struct {
	ListOptions
	DeleteOptions
}

// ApplyOptions applies the given deleteallof options on these options,
// and then returns itself (for convenient chaining).
func (o *DeleteAllOfOptions) ApplyOptions(opts []DeleteAllOfOption) *DeleteAllOfOptions {
	for _, opt := range opts {
		opt.ApplyToDeleteAllOf(o)
	}
	return o
}

var _ DeleteAllOfOption = &DeleteAllOfOptions{}

// ApplyToDeleteAllOf implements DeleteAllOfOption.
func (o *DeleteAllOfOptions) ApplyToDeleteAllOf(do *DeleteAllOfOptions) {
	o.ApplyToList(&do.ListOptions)
	o.ApplyToDelete(&do.DeleteOptions)
}

// }}}
