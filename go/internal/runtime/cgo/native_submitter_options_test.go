// Copyright 2025 The Ray Authors.
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

//go:build cgo
// +build cgo

package cgo

/*
#include <stdlib.h>
*/
import "C"
import (
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/submitter"
	"github.com/stretchr/testify/assert"
)

// TestConvertTaskOptionsToC_NameAndConcurrencyGroup verifies the name and
// concurrency_group_name fields are converted and freed correctly.
func TestConvertTaskOptionsToC_NameAndConcurrencyGroup(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "task-1",
		ConcurrencyGroupName: "cg1",
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.NotNil(t, result.name)
	assert.Equal(t, "task-1", C.GoString(result.name))
	assert.NotNil(t, result.concurrency_group_name)
	assert.Equal(t, "cg1", C.GoString(result.concurrency_group_name))
	freeTaskOptions(result)
}

// TestConvertTaskOptionsToC_EmptyStringsNotAllocated verifies empty name and
// group name produce NULL C pointers, so an empty name/group does not allocate
// a C string.
func TestConvertTaskOptionsToC_EmptyStringsNotAllocated(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "",
		ConcurrencyGroupName: "",
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.Nil(t, result.name, "Empty name must not allocate a C string")
	assert.Nil(t, result.concurrency_group_name, "Empty group name must not allocate a C string")
	freeTaskOptions(result)
}

// TestConvertTaskOptionsToC_AllFieldsCombined verifies the new fields coexist
// with the pre-existing conversion fields.
func TestConvertTaskOptionsToC_AllFieldsCombined(t *testing.T) {
	opts := &submitter.TaskOptions{
		Name:                 "my-task",
		ConcurrencyGroupName: "cg2",
		RuntimeEnv:           `{"pip": ["numpy"]}`,
	}
	result := convertTaskOptionsToC(opts)
	assert.NotNil(t, result)
	assert.Equal(t, "my-task", C.GoString(result.name))
	assert.Equal(t, "cg2", C.GoString(result.concurrency_group_name))
	assert.Equal(t, `{"pip": ["numpy"]}`, C.GoString(result.runtime_env))
	freeTaskOptions(result)
}
