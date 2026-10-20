package main

import (
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

type fakeClient struct {
	gcs.Client
}

func (f *fakeClient) Close() error { return nil }

func (f *fakeClient) IsClosed() bool { return false }

type fakePublisher struct {
	gcs.LogBatchPublisher
}

func ConnectClient(opts gcs.ClientOptions) (gcs.Client, error) {
	return &fakeClient{}, nil
}

func NewLogBatchPublisher(client gcs.Client) (gcs.LogBatchPublisher, error) {
	return &fakePublisher{}, nil
}

// nilHandle implements contract.RuntimeHandle with a pointer-receiver Runtime
// that dereferences its receiver, so any call on the typed-nil instance panics
// -- the exact hazard the loader's typed-nil check must reject before any
// method dispatch.
type nilHandle struct {
	runtime contract.Runtime
}

func (h *nilHandle) IsRuntimeHandle() {}

func (h *nilHandle) Runtime() contract.Runtime { return h.runtime }

// Initialize returns a typed-nil *nilHandle wrapped in the non-nil
// contract.RuntimeHandle interface with a nil error: the interface assertion in
// the loader's validator succeeds, so the nil-ness must be caught via reflect.
func Initialize(opts options.InitializeOptions) (contract.RuntimeHandle, error) {
	var h *nilHandle
	return h, nil
}

// Shutdown and RunTaskExecutionLoop are stubbed so the typednil test plugin
// exports the complete symbol surface validated by the startup pre-check,
// mirroring the real export surface (go/cmd/plugin), which takes
// base.RuntimeHandle.
func Shutdown(handle base.RuntimeHandle) error { return nil }

func RunTaskExecutionLoop(handle base.RuntimeHandle) error { return nil }

func main() {}
