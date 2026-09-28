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

//go:build !cloud

package runtime_env

import (
	"context"
	"fmt"
	"io"
)

// cloudHandlers returns placeholder handlers for the cloud storage protocols
// (GS, S3, Azure Blob Storage and ABFSS) in builds without the "cloud" build
// tag. The real implementations are only compiled with the cloud tag: the
// Azure SDK pulls an azcore dependency graph with a known bazel repository
// visibility conflict in this workspace, and the GCS handler pulls the google
// cloud auth/proto chain that this bazel setup cannot compile. Rebuild with
// bazel --define gotags=cloud (or go build -tags cloud) to enable them.
func cloudHandlers(opts *DownloadOptions) []protocolHandler {
	return []protocolHandler{
		&gsProtocolHandler{},
		&s3ProtocolHandler{},
		&azureProtocolHandler{},
		&abfssProtocolHandler{},
	}
}

// gsProtocolHandler is the GS protocol handler stub for builds without the
// cloud build tag.
type gsProtocolHandler struct{}

func (h *gsProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolGS
}

func (h *gsProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	return fmt.Errorf("gcs protocol support is not compiled in (build without the cloud build tag; rebuild with --define gotags=cloud or go build -tags cloud)")
}

// s3ProtocolHandler is the S3 protocol handler stub for builds without the
// cloud build tag.
type s3ProtocolHandler struct{}

func (h *s3ProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolS3
}

func (h *s3ProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	return fmt.Errorf("s3 protocol support is not compiled in (build without the cloud build tag; rebuild with --define gotags=cloud or go build -tags cloud)")
}

// azureProtocolHandler is the Azure protocol handler stub for builds without
// the cloud build tag.
type azureProtocolHandler struct{}

func (h *azureProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolAzure
}

func (h *azureProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	return fmt.Errorf("azure protocol support is not compiled in (build without the cloud build tag; rebuild with --define gotags=cloud or go build -tags cloud)")
}

// abfssProtocolHandler is the ABFSS protocol handler stub for builds without
// the cloud build tag.
type abfssProtocolHandler struct{}

func (h *abfssProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolABFSS
}

func (h *abfssProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	return fmt.Errorf("abfss protocol support is not compiled in (build without the cloud build tag; rebuild with --define gotags=cloud or go build -tags cloud)")
}

// cloudProtocols returns the protocols whose download handlers are compiled
// into this build: none in the default build, so GetProtocols and
// validation do not advertise downloads that can only fail.
func cloudProtocols() []Protocol {
	return nil
}
