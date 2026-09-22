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

package local_mode

import (
	"testing"

	"github.com/ray-project/ray/go/pkg/runtime/function"
	"github.com/ray-project/ray/go/pkg/runtime/object"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// testLocalAdd is a plain Go function used to exercise registration.
func testLocalAdd(a, b int) int { return a + b }

// testLocalMul is registered after a sync has already recorded the registry
// version, to exercise the version-gated re-sync.
func testLocalMul(a, b int) int { return a * b }

func TestRegisterUserFunctionsFromRegistry(t *testing.T) {
	// Register a plain function into the global registry. This is the same path
	// taken by api.RegisterFunction / api.Remote at call time.
	require.NoError(t, function.Registry.Register(testLocalAdd))

	funcMgr := function.NewFunctionManager(nil)
	require.NoError(t, registerUserFunctions(funcMgr))

	// The function must now be resolvable from the local FunctionManager.
	desc := function.ExtractFunctionDescriptor(testLocalAdd)
	registered, err := funcMgr.GetFunction(desc)
	require.NoError(t, err, "function should be registered in local function manager")
	require.NotNil(t, registered)

	// Execute the registered function through the FunctionManager to verify the
	// wrapper can deserialize args and serialize results.
	ser := object.GetSerializer()
	in1, err := ser.Serialize(2)
	require.NoError(t, err)
	in2, err := ser.Serialize(3)
	require.NoError(t, err)
	args := []function.FunctionArg{
		function.NewFunctionArgByValue(in1.Data, in1.Metadata),
		function.NewFunctionArgByValue(in2.Data, in2.Metadata),
	}
	results, err := registered(args)
	require.NoError(t, err)
	require.Len(t, results, 1)

	var got int
	require.NoError(t, ser.DeserializeTo(&object.NativeRayObject{
		Data:     results[0].Data,
		Metadata: results[0].Metadata,
	}, &got))
	assert.Equal(t, 5, got)
}

func TestRegisterUserFunctionsNilManager(t *testing.T) {
	// A nil manager cannot be synchronized into; the caller must be told.
	require.Error(t, registerUserFunctions(nil))
}

func TestSyncFunctionsFromRegistryPicksUpLateRegistration(t *testing.T) {
	funcMgr := function.NewFunctionManager(nil)
	submitter := &LocalModeTaskSubmitter{functionMgr: funcMgr}

	// Record the current registry version, exactly as Start() does.
	submitter.lastSyncedVersion.Store(function.Registry.Version())

	// A function registered after that sync must be picked up by the next
	// submission's re-sync.
	require.NoError(t, function.Registry.Register(testLocalMul))
	submitter.syncFunctionsFromRegistry()

	_, err := funcMgr.GetFunction(function.ExtractFunctionDescriptor(testLocalMul))
	require.NoError(t, err, "late-registered function should be synced")

	// The sync must be idempotent and must not lose the recorded version.
	version := submitter.lastSyncedVersion.Load()
	submitter.syncFunctionsFromRegistry()
	assert.Equal(t, version, submitter.lastSyncedVersion.Load())
}
