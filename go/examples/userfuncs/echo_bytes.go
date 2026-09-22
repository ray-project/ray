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

package userfuncs

// echoBytes returns its input unchanged. It is the worker-side remote function
// used by the zero-copy benchmark's cross-process load: a large payload is put
// to plasma, passed to this task by reference (>100KB), and returned so the
// driver Get exercises the zero-copy view path.
//
// It is registered explicitly via api.RegisterFunction in RegisterFunctions
// (funcs.go), like goAdd.
func echoBytes(payload []byte) []byte {
	return payload
}

// GetEchoBytes returns the echoBytes function for direct invocation, matching
// the GetGoAdd/GetGoMultiply accessor pattern in funcs.go.
func GetEchoBytes() func([]byte) []byte {
	return echoBytes
}
