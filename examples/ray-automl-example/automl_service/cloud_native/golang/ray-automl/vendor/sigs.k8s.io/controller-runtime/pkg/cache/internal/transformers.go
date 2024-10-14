package internal

import (
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/tools/cache"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

// TransformFuncByObject provides access to the correct transform function for
// any given GVK.
type TransformFuncByObject interface {
	Set(runtime.Object, *runtime.Scheme, cache.TransformFunc) error
	Get(schema.GroupVersionKind) cache.TransformFunc
	SetDefault(transformer cache.TransformFunc)
}

type transformFuncByGVK struct {
	defaultTransform cache.TransformFunc
	transformers     map[schema.GroupVersionKind]cache.TransformFunc
}

// NewTransformFuncByObject creates a new TransformFuncByObject instance.
func NewTransformFuncByObject() TransformFuncByObject {
	return &transformFuncByGVK{
		transformers:     make(map[schema.GroupVersionKind]cache.TransformFunc),
		defaultTransform: nil,
	}
}

func (t *transformFuncByGVK) SetDefault(transformer cache.TransformFunc) {
	t.defaultTransform = transformer
}

func (t *transformFuncByGVK) Set(obj runtime.Object, scheme *runtime.Scheme, transformer cache.TransformFunc) error {
	gvk, err := apiutil.GVKForObject(obj, scheme)
	if err != nil {
		return err
	}

	t.transformers[gvk] = transformer
	return nil
}

func (t transformFuncByGVK) Get(gvk schema.GroupVersionKind) cache.TransformFunc {
	if val, ok := t.transformers[gvk]; ok {
		return val
	}
	return t.defaultTransform
}
