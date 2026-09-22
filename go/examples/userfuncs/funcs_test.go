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

import (
	"testing"
)

// TestGoAdd tests the goAdd function.
func TestGoAdd(t *testing.T) {
	goAddFn := GetGoAdd()
	if got := goAddFn(10, 20); got != 30 {
		t.Errorf("goAdd(10, 20) = %d, want 30", got)
	}
	if got := goAddFn(-5, 5); got != 0 {
		t.Errorf("goAdd(-5, 5) = %d, want 0", got)
	}
	if got := goAddFn(-5, -5); got != -10 {
		t.Errorf("goAdd(-5, -5) = %d, want -10", got)
	}
	if got := goAddFn(50, 50); got != 100 {
		t.Errorf("goAdd(50, 50) = %d, want 100", got)
	}
}

// TestGoMultiply tests the goMultiply function.
func TestGoMultiply(t *testing.T) {
	goMultiplyFn := GetGoMultiply()
	if got := goMultiplyFn(10, 5); got != 50 {
		t.Errorf("goMultiply(10, 5) = %d, want 50", got)
	}
	if got := goMultiplyFn(-5, 5); got != -25 {
		t.Errorf("goMultiply(-5, 5) = %d, want -25", got)
	}
	if got := goMultiplyFn(-5, -5); got != 25 {
		t.Errorf("goMultiply(-5, -5) = %d, want 25", got)
	}
	if got := goMultiplyFn(0, 100); got != 0 {
		t.Errorf("goMultiply(0, 100) = %d, want 0", got)
	}
}

// TestGoConcat tests the goConcat function.
func TestGoConcat(t *testing.T) {
	goConcatFn := GetGoConcat()
	if got := goConcatFn("hello", " world"); got != "hello world" {
		t.Errorf("goConcat(\"hello\", \" world\") = %q, want \"hello world\"", got)
	}
	if got := goConcatFn("", ""); got != "" {
		t.Errorf("goConcat(\"\", \"\") = %q, want \"\"", got)
	}
	if got := goConcatFn("abc", ""); got != "abc" {
		t.Errorf("goConcat(\"abc\", \"\") = %q, want \"abc\"", got)
	}
	if got := goConcatFn("", "xyz"); got != "xyz" {
		t.Errorf("goConcat(\"\", \"xyz\") = %q, want \"xyz\"", got)
	}
}

// TestGoCompute tests the goCompute function.
func TestGoCompute(t *testing.T) {
	goComputeFn := GetGoCompute()
	if got := goComputeFn(10, 5, 2); got != 52 {
		t.Errorf("goCompute(10, 5, 2) = %d, want 52", got)
	}
	if got := goComputeFn(-10, 5, 2); got != -48 {
		t.Errorf("goCompute(-10, 5, 2) = %d, want -48", got)
	}
	if got := goComputeFn(0, 5, 2); got != 2 {
		t.Errorf("goCompute(0, 5, 2) = %d, want 2", got)
	}
	if got := goComputeFn(10, 10, 2); got != 102 {
		t.Errorf("goCompute(10, 10, 2) = %d, want 102", got)
	}
}

// TestGetFunctions tests the helper functions that return function pointers.
func TestGetFunctions(t *testing.T) {
	// Test GetGoAdd
	goAddFn := GetGoAdd()
	if goAddFn == nil {
		t.Fatal("GetGoAdd() returned nil")
	}
	if got := goAddFn(10, 20); got != 30 {
		t.Errorf("GetGoAdd()(10, 20) = %d, want 30", got)
	}

	// Test GetGoMultiply
	goMultiplyFn := GetGoMultiply()
	if goMultiplyFn == nil {
		t.Fatal("GetGoMultiply() returned nil")
	}
	if got := goMultiplyFn(10, 5); got != 50 {
		t.Errorf("GetGoMultiply()(10, 5) = %d, want 50", got)
	}

	// Test GetGoConcat
	goConcatFn := GetGoConcat()
	if goConcatFn == nil {
		t.Fatal("GetGoConcat() returned nil")
	}
	if got := goConcatFn("hello", " world"); got != "hello world" {
		t.Errorf("GetGoConcat()(\"hello\", \" world\") = %q, want \"hello world\"", got)
	}

	// Test GetGoCompute
	goComputeFn := GetGoCompute()
	if goComputeFn == nil {
		t.Fatal("GetGoCompute() returned nil")
	}
	if got := goComputeFn(10, 5, 2); got != 52 {
		t.Errorf("GetGoCompute()(10, 5, 2) = %d, want 52", got)
	}
}
