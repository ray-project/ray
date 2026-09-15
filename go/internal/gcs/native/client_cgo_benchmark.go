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

//go:build cgo

package native

/*
#include <stdlib.h>
#include <string.h>

// Mock CGO functions for benchmark.
// Return fake data without depending on the real C++ library.

static int mock_kv_get(const char* ns, const char* key, void** data_out, size_t* size_out, size_t size_bytes) {
    if (!data_out || !size_out || size_bytes == 0) {
        return -1;
    }

    *size_out = size_bytes;
    *data_out = malloc(*size_out);
    if (!*data_out) {
        return -1;
    }

    // Fill with a fixed pattern to make verification deterministic.
    memset(*data_out, 'A', *size_out);

    return 0;
}

static void mock_free(void* ptr) {
    if (ptr) {
        free(ptr);
    }
}
*/
import "C"

import (
	"testing"
	"unsafe"
)

// Benchmark_Get_1KB benchmarks the Get performance for 1KB payloads.
func Benchmark_Get_1KB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var cData unsafe.Pointer
		var cSize C.size_t

		result := C.mock_kv_get(C.CString("test"), C.CString("key"), &cData, &cSize, 1024)
		if result != 0 {
			b.Fatal("mock_kv_get failed")
		}

		if cData != nil {
			data := C.GoBytes(cData, C.int(cSize))
			if len(data) != 1024 {
				b.Fatalf("expected 1024 bytes, got %d", len(data))
			}
			C.mock_free(cData)
		}
	}
}

// Benchmark_Get_100KB benchmarks the Get performance for 100KB payloads.
func Benchmark_Get_100KB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var cData unsafe.Pointer
		var cSize C.size_t

		result := C.mock_kv_get(C.CString("test"), C.CString("key"), &cData, &cSize, 100*1024)
		if result != 0 {
			b.Fatal("mock_kv_get failed")
		}

		if cData != nil {
			data := C.GoBytes(cData, C.int(cSize))
			if len(data) != 100*1024 {
				b.Fatalf("expected %d bytes, got %d", 100*1024, len(data))
			}
			C.mock_free(cData)
		}
	}
}

// Benchmark_Get_1MB benchmarks the Get performance for 1MB payloads.
func Benchmark_Get_1MB(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var cData unsafe.Pointer
		var cSize C.size_t

		result := C.mock_kv_get(C.CString("test"), C.CString("key"), &cData, &cSize, 1024*1024)
		if result != 0 {
			b.Fatal("mock_kv_get failed")
		}

		if cData != nil {
			data := C.GoBytes(cData, C.int(cSize))
			if len(data) != 1024*1024 {
				b.Fatalf("expected %d bytes, got %d", 1024*1024, len(data))
			}
			C.mock_free(cData)
		}
	}
}
