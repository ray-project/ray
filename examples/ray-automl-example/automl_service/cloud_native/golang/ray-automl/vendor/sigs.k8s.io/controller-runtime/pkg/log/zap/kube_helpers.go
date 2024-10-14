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

package zap

import (
	"fmt"
	"reflect"

	"go.uber.org/zap/buffer"
	"go.uber.org/zap/zapcore"
	"k8s.io/apimachinery/pkg/api/meta"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
)

// KubeAwareEncoder is a Kubernetes-aware Zap Encoder.
// Instead of trying to force Kubernetes objects to implement
// ObjectMarshaller, we just implement a wrapper around a normal
// ObjectMarshaller that checks for Kubernetes objects.
type KubeAwareEncoder struct {
	// Encoder is the zapcore.Encoder that this encoder delegates to
	zapcore.Encoder

	// Verbose controls whether or not the full object is printed.
	// If false, only name, namespace, api version, and kind are printed.
	// Otherwise, the full object is logged.
	Verbose bool
}

// namespacedNameWrapper is a zapcore.ObjectMarshaler for Kubernetes NamespacedName.
type namespacedNameWrapper struct {
	types.NamespacedName
}

func (w namespacedNameWrapper) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	if w.Namespace != "" {
		enc.AddString("namespace", w.Namespace)
	}

	enc.AddString("name", w.Name)

	return nil
}

// kubeObjectWrapper is a zapcore.ObjectMarshaler for Kubernetes objects.
type kubeObjectWrapper struct {
	obj runtime.Object
}

// MarshalLogObject implements zapcore.ObjectMarshaler.
func (w kubeObjectWrapper) MarshalLogObject(enc zapcore.ObjectEncoder) error {
	// TODO(directxman12): log kind and apiversion if not set explicitly (common case)
	// -- needs an a scheme to convert to the GVK.

	if reflect.ValueOf(w.obj).IsNil() {
		return fmt.Errorf("got nil for runtime.Object")
	}

	if gvk := w.obj.GetObjectKind().GroupVersionKind(); gvk.Version != "" {
		enc.AddString("apiVersion", gvk.GroupVersion().String())
		enc.AddString("kind", gvk.Kind)
	}

	objMeta, err := meta.Accessor(w.obj)
	if err != nil {
		return fmt.Errorf("got runtime.Object without object metadata: %v", w.obj)
	}

	if ns := objMeta.GetNamespace(); ns != "" {
		enc.AddString("namespace", ns)
	}
	enc.AddString("name", objMeta.GetName())

	return nil
}

// NB(directxman12): can't just override AddReflected, since the encoder calls AddReflected on itself directly

// Clone implements zapcore.Encoder.
func (k *KubeAwareEncoder) Clone() zapcore.Encoder {
	return &KubeAwareEncoder{
		Encoder: k.Encoder.Clone(),
	}
}

// EncodeEntry implements zapcore.Encoder.
func (k *KubeAwareEncoder) EncodeEntry(entry zapcore.Entry, fields []zapcore.Field) (*buffer.Buffer, error) {
	if k.Verbose {
		// Kubernetes objects implement fmt.Stringer, so if we
		// want verbose output, just delegate to that.
		return k.Encoder.EncodeEntry(entry, fields)
	}

	for i, field := range fields {
		// intercept stringer fields that happen to be Kubernetes runtime.Object or
		// types.NamespacedName values (Kubernetes runtime.Objects commonly
		// implement String, apparently).
		// *unstructured.Unstructured does NOT implement fmt.Striger interface.
		// We have handle it specially.
		if field.Type == zapcore.StringerType || field.Type == zapcore.ReflectType {
			switch val := field.Interface.(type) {
			case runtime.Object:
				fields[i] = zapcore.Field{
					Type:      zapcore.ObjectMarshalerType,
					Key:       field.Key,
					Interface: kubeObjectWrapper{obj: val},
				}
			case types.NamespacedName:
				fields[i] = zapcore.Field{
					Type:      zapcore.ObjectMarshalerType,
					Key:       field.Key,
					Interface: namespacedNameWrapper{NamespacedName: val},
				}
			}
		}
	}

	return k.Encoder.EncodeEntry(entry, fields)
}
