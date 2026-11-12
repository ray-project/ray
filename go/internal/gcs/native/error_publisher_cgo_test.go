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

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/ids"
)

func TestCgoClient_PublishErrorInfo_NilReceiver(t *testing.T) {
	var pub *cgoClient
	err := pub.PublishErrorInfo(context.Background(), "key", gcs.ErrorInfoPayload{Type: "raylet_died"})
	if err == nil {
		t.Fatal("expected error for nil receiver, got nil")
	}
	if !strings.Contains(err.Error(), "not initialized") {
		t.Errorf("expected 'not initialized' in error, got %q", err)
	}
}

func TestCgoClient_PublishErrorInfo_NullBackingClient(t *testing.T) {
	// A *cgoClient with a nil backing C client is rejected by withPtr's
	// closed-client guard before cgo is entered, so this pins the Go-side
	// fail path of publish rather than the C++ argument guard (which the
	// C++ tests cover). No live GCS cluster is needed. The ctx carries a
	// deadline because the deadline-less rejection fires before withPtr.
	pub := &cgoClient{}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := pub.PublishErrorInfo(ctx, ids.NilJobID().Hex(),
		gcs.ErrorInfoPayload{JobID: ids.NilJobID().Binary(), Type: "raylet_died", Message: "message"})
	if err == nil {
		t.Fatal("expected error from null backing client, got nil")
	}
	if !strings.Contains(err.Error(), "closed") {
		t.Errorf("expected 'closed' in error, got %q", err)
	}
	t.Logf("publish against null backing client failed as expected: %v", err)
}

// TestCgoClient_PublishErrorInfo_RejectsADeadlineLessContext enforces the
// interface contract: a deadline-less ctx maps to timeout -1, which the C++
// retry queue waits on unboundedly while GCS is unreachable, and this
// synchronous cgo call cannot observe ctx cancellation. Deleting the
// deadline check in PublishErrorInfo fails this test — the violation surfaces
// as an immediate error instead of an unabortable hang.
func TestCgoClient_PublishErrorInfo_RejectsADeadlineLessContext(t *testing.T) {
	pub := &cgoClient{}
	err := pub.PublishErrorInfo(context.Background(), ids.NilJobID().Hex(),
		gcs.ErrorInfoPayload{JobID: ids.NilJobID().Binary(), Type: "raylet_died", Message: "message"})
	if err == nil {
		t.Fatal("PublishErrorInfo accepted a deadline-less context; the synchronous cgo call would hang unabortably while GCS is unreachable")
	}
	if !strings.Contains(err.Error(), "deadline") {
		t.Errorf("error %q does not name the missing deadline", err)
	}
}

// TestScrubNULRemovesEmbeddedNULBytes pins the NUL scrub the message goes
// through before C.CString: the C++ protobuf setters read it as a
// NUL-terminated string, so an unsanitized NUL from raw raylet.out tail bytes
// would silently truncate the published diagnostic mid-content. Reverting the
// scrub to an identity function fails this test.
func TestScrubNULRemovesEmbeddedNULBytes(t *testing.T) {
	const embedded = "before\x00after\n    last tail line"
	got := scrubNUL(embedded)
	if want := "beforeafter\n    last tail line"; got != want {
		t.Fatalf("scrubNUL = %q, want %q", got, want)
	}
	if strings.ContainsRune(scrubNUL("\x00tail\x00"), '\x00') {
		t.Fatal("scrubNUL left an embedded NUL byte behind")
	}
	// A clean message must pass through unchanged: the scrub must not be a
	// disguised message rewrite.
	if got := scrubNUL("Raylet is terminated. Termination is unexpected."); got != "Raylet is terminated. Termination is unexpected." {
		t.Fatalf("scrubNUL changed a clean message: %q", got)
	}
}
