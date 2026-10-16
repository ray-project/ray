package main

import (
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/pkg/options"
	"github.com/ray-project/ray/go/pkg/runtime/contract"
)

// ConnectClient intentionally returns (string, error): a wrong first return
// type, so callPluginFunc's validator rejects it.
func ConnectClient(opts gcs.ClientOptions) (string, error) {
	return opts.Address, nil
}

func Initialize(opts options.InitializeOptions) (contract.RuntimeHandle, error) {
	return nil, nil
}

func main() {}
