// Copyright 2026 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package autoscaler

import (
	"fmt"
	"github.com/ray-project/ray/go/pkg/log"
)

func DeleteMapEle(mapIn map[string]interface{}, key string) {
	if mapIn == nil {
		return
	}

	delete(mapIn, key)
}

// MergeMap updates target by merging every key/value pair of in into it.
func MergeMap(target map[string]interface{}, in map[string]interface{}) map[string]interface{} {
	for k, v := range in {
		target[k] = v
	}
	return target
}

// GetValueWithDefault returns the value of a nested map key, or the given
// default value when the key is missing or the type does not match.
func GetValueWithDefault[T any](mapIn map[string]interface{}, defaultValue T, keys ...string) T {
	var current interface{} = mapIn

	for _, k := range keys {
		v, ok := current.(map[string]interface{})
		if !ok {
			return defaultValue
		}

		current, ok = v[k]
		if !ok {
			return defaultValue
		}
	}

	if typedValue, ok := current.(T); ok {
		return typedValue
	}

	return defaultValue
}

// GetValue returns the value of a nested map key with a generic result.
func GetValue[T any](m map[string]interface{}, keys ...string) (T, error) {
	var current interface{} = m
	var zero T
	var err error
	err = nil

	for _, k := range keys {
		v, ok := current.(map[string]interface{})
		if !ok {
			err = fmt.Errorf("the data type of %s is wrong", k)
			log.Log.V(1).Error(err, "")
			return zero, err
		}

		current, ok = v[k]
		if !ok {
			err = fmt.Errorf("%s not found in map", k)
			log.Log.V(1).Error(err, "")
			return zero, err
		}
	}

	typedValue, ok := current.(T)
	if !ok {
		err = fmt.Errorf("the data type of %s is wrong", keys[len(keys)-1])
		log.Log.V(1).Error(err, "")
		return zero, err
	}

	return typedValue, nil
}

func ContainKey(mapIn map[string]interface{}, key string) bool {
	_, isExist := mapIn[key]
	return isExist
}

func PutIfAbsent[T any](mapIn map[string]interface{}, val T, key string) {
	if _, exists := mapIn[key]; !exists {
		mapIn[key] = val
	}
}
