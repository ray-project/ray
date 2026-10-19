package main

import (
	"github.com/ray-project/ray/go/internal/runtime/base"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

type fakeClient struct {
	gcs.Client
	addr string
}

func (f *fakeClient) Address() string { return f.addr }

func (f *fakeClient) Close() error { return nil }

func (f *fakeClient) IsClosed() bool { return false }

type fakePublisher struct {
	gcs.LogBatchPublisher
}

func ConnectClient(opts gcs.ClientOptions) (gcs.Client, error) {
	return &fakeClient{addr: opts.Address}, nil
}

func NewLogBatchPublisher(client gcs.Client) (gcs.LogBatchPublisher, error) {
	return &fakePublisher{}, nil
}

func Initialize(opts options.InitializeOptions) (contract.RuntimeHandle, error) {
	return nil, nil
}

// Shutdown and RunTaskExecutionLoop are stubbed so the full test plugin
// exports the complete symbol surface validated by the startup pre-check.
// They mirror the real export surface (go/cmd/plugin), which takes
// base.RuntimeHandle.
func Shutdown(handle base.RuntimeHandle) error { return nil }

func RunTaskExecutionLoop(handle base.RuntimeHandle) error { return nil }

func main() {}
