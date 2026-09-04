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

// mockFunction is a simple mock function for testing
func mockFunction(args []FunctionArg) ([]SerializedObject, error) {
	return []SerializedObject{
		{Data: []byte("mock result")},
	}, nil
}

func TestNewFunctionManager(t *testing.T) {
	manager := NewFunctionManager([]string{"/path/to/code"})
	assert.NotNil(t, manager)
	assert.NotNil(t, manager.functions)
	assert.Equal(t, 0, len(manager.functions))
}

func TestFunctionManager_RegisterFunction(t *testing.T) {
	manager := NewFunctionManager(nil)

	desc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "MyTask",
	}

	manager.RegisterFunction(desc, mockFunction)

	// Verify the function is registered
	assert.Len(t, manager.functions, 1)
	key := desc.String()
	_, exists := manager.functions[key]
	assert.True(t, exists)
}

func TestFunctionManager_GetFunction_Exists(t *testing.T) {
	manager := NewFunctionManager(nil)

	desc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "MyTask",
	}

	manager.RegisterFunction(desc, mockFunction)

	// Get the function
	fn, err := manager.GetFunction(desc)
	assert.NoError(t, err)
	assert.NotNil(t, fn)

	// Execute the function to verify it works
	result, err := fn(nil)
	assert.NoError(t, err)
	assert.Len(t, result, 1)
	assert.Equal(t, []byte("mock result"), result[0].Data)
}

func TestFunctionManager_GetFunction_NotExists(t *testing.T) {
	manager := NewFunctionManager(nil)

	desc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "NonExistentTask",
	}

	fn, err := manager.GetFunction(desc)
	assert.Error(t, err)
	assert.Nil(t, fn)
	assert.Contains(t, err.Error(), "function not registered")
}

func TestFunctionManager_GetFunctionByBaseDescriptor(t *testing.T) {
	manager := NewFunctionManager(nil)

	// Register with GoFunctionDescriptor
	goDesc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "MyTask",
	}
	manager.RegisterFunction(goDesc, mockFunction)

	// Create base descriptor with same format
	baseDesc, err := FunctionDescriptorFromList([]string{
		"github.com/example/myapp/tasks",
		"pkg",
		"MyTask",
		"",
	})
	assert.NoError(t, err)

	// Get using base descriptor
	fn, err := manager.GetFunctionByBaseDescriptor(baseDesc)
	assert.NoError(t, err)
	assert.NotNil(t, fn)
}

func TestFunctionManager_IsRegistered(t *testing.T) {
	manager := NewFunctionManager(nil)

	desc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "MyTask",
	}

	// Initially not registered
	assert.False(t, manager.IsRegistered(desc))

	// Register and verify
	manager.RegisterFunction(desc, mockFunction)
	assert.True(t, manager.IsRegistered(desc))
}

func TestFunctionManager_ListRegisteredFunctions(t *testing.T) {
	manager := NewFunctionManager(nil)

	// Initially empty
	list := manager.ListRegisteredFunctions()
	assert.Len(t, list, 0)

	// Register multiple functions
	desc1 := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "Task1",
	}
	desc2 := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "Task2",
	}

	manager.RegisterFunction(desc1, mockFunction)
	manager.RegisterFunction(desc2, mockFunction)

	list = manager.ListRegisteredFunctions()
	assert.Len(t, list, 2)
}

func TestFunctionManager_UnregisterFunction(t *testing.T) {
	manager := NewFunctionManager(nil)

	desc := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "MyTask",
	}

	manager.RegisterFunction(desc, mockFunction)
	assert.True(t, manager.IsRegistered(desc))

	manager.UnregisterFunction(desc)
	assert.False(t, manager.IsRegistered(desc))
}

func TestFunctionManager_Clear(t *testing.T) {
	manager := NewFunctionManager(nil)

	// Register multiple functions
	desc1 := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "Task1",
	}
	desc2 := &GoFunctionDescriptor{
		ModuleName:  "github.com/example/myapp/tasks",
		PackagePath: "pkg",
		FunctionName: "Task2",
	}

	manager.RegisterFunction(desc1, mockFunction)
	manager.RegisterFunction(desc2, mockFunction)
	assert.Len(t, manager.ListRegisteredFunctions(), 2)

	// Clear all
	manager.Clear()
	assert.Len(t, manager.ListRegisteredFunctions(), 0)
}

func TestFunctionManager_ThreadSafety(t *testing.T) {
	manager := NewFunctionManager(nil)

	// Register functions concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(idx int) {
			desc := &GoFunctionDescriptor{
				ModuleName:  "github.com/example/myapp/tasks",
				PackagePath: "pkg",
				FunctionName: "Task" + string(rune('0'+idx)),
			}
			manager.RegisterFunction(desc, mockFunction)
			done <- true
		}(i)
	}

	// Wait for all goroutines to complete
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all functions are registered
	assert.Len(t, manager.ListRegisteredFunctions(), 10)

	// Read concurrently
	for i := 0; i < 10; i++ {
		go func(idx int) {
			desc := &GoFunctionDescriptor{
				ModuleName:  "github.com/example/myapp/tasks",
				PackagePath: "pkg",
				FunctionName: "Task" + string(rune('0'+idx)),
			}
			fn, err := manager.GetFunction(desc)
			assert.NoError(t, err)
			assert.NotNil(t, fn)
			done <- true
		}(i)
	}

	// Wait for all reads to complete
	for i := 0; i < 10; i++ {
		<-done
	}
}
