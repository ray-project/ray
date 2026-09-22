// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"reflect"
	"testing"
)

func TestDeserializeArgs_ArgCountMismatch(t *testing.T) {
	paramTypes := []reflect.Type{reflect.TypeOf(0), reflect.TypeOf("")}

	// Too few arguments must return a clear error (not panic / not zero-fill).
	_, err := DeserializeArgs(nil, paramTypes)
	if err == nil {
		t.Fatal("expected argument count mismatch error, got nil")
	}
	if got, want := err.Error(), "argument count mismatch: expected 2, got 0"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}

	// Too many arguments must return a clear error (not an index-out-of-range panic).
	args := []FunctionArg{
		NewFunctionArgByValue(nil, nil),
		NewFunctionArgByValue(nil, nil),
		NewFunctionArgByValue(nil, nil),
	}
	_, err = DeserializeArgs(args, paramTypes)
	if err == nil {
		t.Fatal("expected argument count mismatch error, got nil")
	}
	if got, want := err.Error(), "argument count mismatch: expected 2, got 3"; got != want {
		t.Fatalf("unexpected error: got %q, want %q", got, want)
	}
}
