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

func TestFunctionDescriptorFromListWithLanguage_Go(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguageGo,
		[]string{"github.com/example/app", "pkg/tasks", "MyTask", ""})
	assert.NoError(t, err)
	goDesc, ok := desc.(*GoFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "github.com/example/app", goDesc.ModuleName)
	assert.Equal(t, "pkg/tasks", goDesc.PackagePath)
	assert.Equal(t, "MyTask", goDesc.FunctionName)
	assert.Equal(t, LanguageGo, goDesc.GetLanguage())
}

func TestFunctionDescriptorFromListWithLanguage_GoActorMethod(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguageGo,
		[]string{"github.com/example/app", "pkg/actors", "MyActor", "DoWork"})
	assert.NoError(t, err)
	goDesc, ok := desc.(*GoFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "DoWork", goDesc.MethodName())
}

func TestFunctionDescriptorFromListWithLanguage_Python(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguagePython,
		[]string{"my_module", "Calculator", "add", "hash123"})
	assert.NoError(t, err)
	pyDesc, ok := desc.(*PythonFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "my_module", pyDesc.ModuleName)
	assert.Equal(t, "Calculator", pyDesc.ClassName)
	assert.Equal(t, "add", pyDesc.FunctionName)
	assert.Equal(t, "hash123", pyDesc.FunctionHash)
	assert.Equal(t, LanguagePython, pyDesc.GetLanguage())
}

// TestFunctionDescriptorFromListWithLanguage_PythonDottedModule verifies that a
// Python module name containing dots (e.g. "numpy.linalg") is recognized as
// Python. The previous heuristic (guessing Go when the module name contained
// '/') handled this case, but a dotted module with no slash would have been
// misclassified. With the explicit language, dotted Python modules are correct.
func TestFunctionDescriptorFromListWithLanguage_PythonDottedModule(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguagePython,
		[]string{"numpy.linalg", "", "norm", ""})
	assert.NoError(t, err)
	pyDesc, ok := desc.(*PythonFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "numpy.linalg", pyDesc.ModuleName)
	assert.Equal(t, "norm", pyDesc.FunctionName)
}

func TestFunctionDescriptorFromListWithLanguage_PythonModuleLevel(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguagePython,
		[]string{"my_module", "", "add", ""})
	assert.NoError(t, err)
	pyDesc, ok := desc.(*PythonFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "", pyDesc.ClassName)
}

func TestFunctionDescriptorFromListWithLanguage_WrongElementCount(t *testing.T) {
	_, err := FunctionDescriptorFromListWithLanguage(LanguageGo, []string{"a", "b"})
	assert.Error(t, err)
	_, err = FunctionDescriptorFromListWithLanguage(LanguagePython, []string{"a"})
	assert.Error(t, err)
}

func TestFunctionDescriptorFromListWithLanguage_UnsupportedLanguage(t *testing.T) {
	_, err := FunctionDescriptorFromListWithLanguage(LanguageJava, []string{"a", "b", "c", "d"})
	assert.Error(t, err)
}

func TestNewCppFunctionDescriptor(t *testing.T) {
	desc, err := NewCppFunctionDescriptor("add", "GO", "")
	assert.NoError(t, err)
	assert.NotNil(t, desc)
	assert.Equal(t, "add", desc.CreateFunctionName)
	assert.Equal(t, "GO", desc.Caller)
	assert.Equal(t, "", desc.ClassName)
}

func TestNewCppFunctionDescriptor_Actor(t *testing.T) {
	desc, err := NewCppFunctionDescriptor("create_actor", "GO", "Calculator")
	assert.NoError(t, err)
	assert.Equal(t, "create_actor", desc.CreateFunctionName)
	assert.Equal(t, "GO", desc.Caller)
	assert.Equal(t, "Calculator", desc.ClassName)
}

func TestNewCppFunctionDescriptor_EmptyFunctionName(t *testing.T) {
	desc, err := NewCppFunctionDescriptor("", "GO", "")
	assert.Error(t, err)
	assert.Nil(t, desc)
}

func TestNewCppFunctionDescriptor_EmptyCaller(t *testing.T) {
	// An empty caller identifies a native C++ function descriptor (the
	// 3-element parse form); only Go-side call sites pass a caller, and they
	// always pass cppCallerLanguage, so the constructor accepts it as-is.
	desc, err := NewCppFunctionDescriptor("add", "", "")
	assert.NoError(t, err)
	if assert.NotNil(t, desc) {
		assert.Equal(t, "add", desc.CreateFunctionName)
		assert.Equal(t, "", desc.Caller)
	}
}

func TestNewCppFunctionDescriptor_QualifiedName(t *testing.T) {
	// C++ identifiers may be namespace/class qualified or template
	// instantiations; unlike Go identifiers these must be accepted.
	valid := []string{
		"ns::Class::method",
		"example::Calculator::add",
		"factory<int>",
		"Operator",
	}
	for _, name := range valid {
		desc, err := NewCppFunctionDescriptor(name, "GO", "")
		assert.NoError(t, err, "name %q should be accepted", name)
		assert.Equal(t, name, desc.CreateFunctionName)
	}
}

func TestNewCppFunctionDescriptor_WhitespaceRejected(t *testing.T) {
	// Whitespace can never be part of a C++ identifier; it must be rejected so
	// a malformed descriptor cannot slip through to the C++ worker.
	invalid := []string{"my func", "add\n", "\tadd", "a b"}
	for _, name := range invalid {
		desc, err := NewCppFunctionDescriptor(name, "GO", "")
		assert.Error(t, err, "name %q should be rejected", name)
		assert.Nil(t, desc)
	}
}

func TestNewCppFunctionDescriptorOrUnknown(t *testing.T) {
	desc := NewCppFunctionDescriptorOrUnknown("", "", "")
	assert.NotNil(t, desc)
	assert.Equal(t, "unknown", desc.CreateFunctionName)
	// The caller is passed through unchanged: an empty caller is a valid
	// native-C++ descriptor value, not an invalid field to substitute.
	assert.Equal(t, "", desc.Caller)
}

func TestCppFunctionDescriptor_ToList(t *testing.T) {
	desc := &CppFunctionDescriptor{CreateFunctionName: "add", Caller: "GO", ClassName: "Calculator"}
	assert.Equal(t, []string{"add", "GO", "Calculator"}, desc.ToList())
	assert.Equal(t, []string{"add", "GO", ""}, (&CppFunctionDescriptor{CreateFunctionName: "add", Caller: "GO"}).ToList())
}

func TestCppFunctionDescriptor_GetLanguage(t *testing.T) {
	desc := &CppFunctionDescriptor{CreateFunctionName: "add", Caller: "GO"}
	assert.Equal(t, LanguageCpp, desc.GetLanguage())
}

func TestCppFunctionDescriptor_Hash(t *testing.T) {
	desc1 := &CppFunctionDescriptor{CreateFunctionName: "add", Caller: "GO", ClassName: ""}
	desc2 := &CppFunctionDescriptor{CreateFunctionName: "add", Caller: "GO", ClassName: ""}
	desc3 := &CppFunctionDescriptor{CreateFunctionName: "mul", Caller: "GO", ClassName: ""}
	assert.Equal(t, desc1.Hash(), desc2.Hash())
	assert.NotEqual(t, desc1.Hash(), desc3.Hash())
}

func TestCppFunctionDescriptor_ImplementsInterface(t *testing.T) {
	var _ FunctionDescriptor = &CppFunctionDescriptor{}
}

func TestFunctionDescriptorFromListWithLanguage_Cpp(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguageCpp,
		[]string{"add", "GO", ""})
	assert.NoError(t, err)
	cppDesc, ok := desc.(*CppFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "add", cppDesc.CreateFunctionName)
	assert.Equal(t, "GO", cppDesc.Caller)
	assert.Equal(t, "", cppDesc.ClassName)
	assert.Equal(t, LanguageCpp, cppDesc.GetLanguage())
}

func TestFunctionDescriptorFromListWithLanguage_CppActor(t *testing.T) {
	desc, err := FunctionDescriptorFromListWithLanguage(LanguageCpp,
		[]string{"create_actor", "GO", "Calculator"})
	assert.NoError(t, err)
	cppDesc, ok := desc.(*CppFunctionDescriptor)
	assert.True(t, ok)
	assert.Equal(t, "Calculator", cppDesc.ClassName)
}

func TestFunctionDescriptorFromListWithLanguage_CppWrongElementCount(t *testing.T) {
	_, err := FunctionDescriptorFromListWithLanguage(LanguageCpp, []string{"a", "b"})
	assert.Error(t, err)
	_, err = FunctionDescriptorFromListWithLanguage(LanguageCpp, []string{"a", "b", "c", "d"})
	assert.Error(t, err)
}

func TestFunctionDescriptorFromListWithLanguage_CppEmptyFunctionName(t *testing.T) {
	_, err := FunctionDescriptorFromListWithLanguage(LanguageCpp, []string{"", "GO", ""})
	assert.Error(t, err)
}
