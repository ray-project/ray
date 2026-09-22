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

package function

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewPythonFunctionDescriptor(t *testing.T) {
	desc, err := NewPythonFunctionDescriptor("my_module", "Calculator", "add", "hash123")
	assert.NoError(t, err)
	assert.NotNil(t, desc)
	assert.Equal(t, "my_module", desc.ModuleName)
	assert.Equal(t, "Calculator", desc.ClassName)
	assert.Equal(t, "add", desc.FunctionName)
	assert.Equal(t, "hash123", desc.FunctionHash)
}

func TestNewPythonFunctionDescriptor_ModuleLevel(t *testing.T) {
	desc, err := NewPythonFunctionDescriptor("my_module", "", "add", "")
	assert.NoError(t, err)
	assert.Equal(t, "", desc.ClassName)
	assert.Equal(t, "", desc.FunctionHash)
}

func TestNewPythonFunctionDescriptor_EmptyModuleName(t *testing.T) {
	desc, err := NewPythonFunctionDescriptor("", "", "add", "")
	assert.Error(t, err)
	assert.Nil(t, desc)
}

func TestNewPythonFunctionDescriptor_EmptyFunctionName(t *testing.T) {
	desc, err := NewPythonFunctionDescriptor("my_module", "", "", "")
	assert.Error(t, err)
	assert.Nil(t, desc)
}

func TestNewPythonFunctionDescriptor_InvalidModuleName(t *testing.T) {
	desc, err := NewPythonFunctionDescriptor("my module", "", "add", "")
	assert.Error(t, err)
	assert.Nil(t, desc)
}

func TestPythonFunctionDescriptor_ToList(t *testing.T) {
	desc := &PythonFunctionDescriptor{
		ModuleName:   "my_module",
		ClassName:    "Calculator",
		FunctionName: "add",
		FunctionHash: "abc",
	}
	assert.Equal(t, []string{"my_module", "Calculator", "add", "abc"}, desc.ToList())
}

func TestPythonFunctionDescriptor_GetLanguage(t *testing.T) {
	desc := &PythonFunctionDescriptor{ModuleName: "m", FunctionName: "f"}
	assert.Equal(t, LanguagePython, desc.GetLanguage())
}

func TestPythonFunctionDescriptor_Hash(t *testing.T) {
	desc1 := &PythonFunctionDescriptor{ModuleName: "m", FunctionName: "f"}
	desc2 := &PythonFunctionDescriptor{ModuleName: "m", FunctionName: "f"}
	desc3 := &PythonFunctionDescriptor{ModuleName: "m", FunctionName: "g"}
	assert.Equal(t, desc1.Hash(), desc2.Hash())
	assert.NotEqual(t, desc1.Hash(), desc3.Hash())
}

func TestPythonFunctionDescriptor_ImplementsInterface(t *testing.T) {
	var _ FunctionDescriptor = &PythonFunctionDescriptor{}
}

// TestFunctionDescriptorFromList_GoFunction verifies that the string-list
// constructor builds a Go function descriptor from
// [moduleName, packagePath, functionName, methodName].
func TestFunctionDescriptorFromList_GoFunction(t *testing.T) {
	desc, err := FunctionDescriptorFromList([]string{"github.com/example/app", "pkg", "MyTask", ""})
	assert.NoError(t, err)
	goDesc, ok := desc.(*GoFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "github.com/example/app", goDesc.ModuleName)
	assert.Equal(t, LanguageGo, goDesc.GetLanguage())
}
