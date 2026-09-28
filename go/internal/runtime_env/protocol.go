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

package runtime_env

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Protocol is a runtime env protocol type.
type Protocol string

const (
	// ProtocolGCS is the GCS protocol (Ray internal storage).
	ProtocolGCS Protocol = "gcs"
	// ProtocolConda is the conda environment protocol.
	ProtocolConda Protocol = "conda"
	// ProtocolPip is the pip package management protocol.
	ProtocolPip Protocol = "pip"
	// ProtocolUv is the uv package management protocol.
	ProtocolUv Protocol = "uv"
	// ProtocolHTTPS is the HTTPS remote download protocol.
	ProtocolHTTPS Protocol = "https"
	// ProtocolS3 is the AWS S3 storage protocol.
	ProtocolS3 Protocol = "s3"
	// ProtocolGS is the Google Cloud Storage protocol.
	ProtocolGS Protocol = "gs"
	// ProtocolAzure is the Azure Blob storage protocol.
	ProtocolAzure Protocol = "azure"
	// ProtocolABFSS is the Azure Data Lake Storage Gen2 protocol.
	ProtocolABFSS Protocol = "abfss"
	// ProtocolFile is the local file system protocol.
	ProtocolFile Protocol = "file"
)

// ProtocolsProvider provides the supported protocols.
type ProtocolsProvider struct{}

// GetProtocols returns all supported protocols. Cloud storage protocols
// (s3/gs/azure/abfss) are only advertised when their handlers are compiled
// in (the gotags=cloud build); advertising a protocol whose downloads always
// fail would make validation accept URIs that cannot be fetched.
func GetProtocols() []Protocol {
	protocols := []Protocol{
		// For packages dynamically uploaded and managed by the GCS.
		ProtocolGCS,
		// For conda environments installed locally on each node.
		ProtocolConda,
		// For pip environments installed locally on each node.
		ProtocolPip,
		// For uv environments install locally on each node.
		ProtocolUv,
		// Remote https path, assumes everything packed in one zip file.
		ProtocolHTTPS,
		// File storage path, assumes everything packed in one zip file.
		ProtocolFile,
	}
	return append(protocols, cloudProtocols()...)
}

// GetRemoteProtocols returns all remote storage protocols.
// These protocols should only be used with paths that end in ".zip" or ".whl"
func GetRemoteProtocols() []Protocol {
	protocols := []Protocol{
		ProtocolHTTPS,
		ProtocolFile,
	}
	return append(protocols, cloudProtocols()...)
}

// IsRemoteProtocol reports whether the protocol is a remote protocol.
func IsRemoteProtocol(protocol Protocol) bool {
	remoteProtocols := GetRemoteProtocols()
	for _, p := range remoteProtocols {
		if p == protocol {
			return true
		}
	}
	return false
}

// isValidProtocol reports whether the protocol is valid.
func isValidProtocol(protocol Protocol) bool {
	for _, p := range GetProtocols() {
		if p == protocol {
			return true
		}
	}
	return false
}

// DownloadOptions holds the download options.
type DownloadOptions struct {
	HTTPTimeout    time.Duration // HTTP request timeout.
	BufferSize     int           // IO buffer size in bytes.
	UseAtomicWrite bool          // Whether to write atomically.
}

// DefaultDownloadOptions holds the default download options.
var DefaultDownloadOptions = &DownloadOptions{
	HTTPTimeout:    300 * time.Second,
	BufferSize:     32 * 1024, // 32KB buffer for better I/O performance
	UseAtomicWrite: false,
}

// downloadFunc is the type of download functions.
type downloadFunc func(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error

// protocolHandler is the interface implemented by protocol handlers.
type protocolHandler interface {
	Match(protocol Protocol) bool
	Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error
}

// storageURIParts holds the parsed parts of a storage URI.
type storageURIParts struct {
	Bucket     string // Bucket or container name.
	ObjectName string // Object or file path.
	RawPath    string // Raw path (for special handling).
	Netloc     string // Full network location (host:port etc.).
}

// parseStorageURI is a shared helper for parsing storage URIs.
// It returns the parsed URI parts and an error.
func parseStorageURI(sourceURI string, protocolName string) (*storageURIParts, error) {
	parsedURL, err := url.Parse(sourceURI)
	if err != nil {
		return nil, fmt.Errorf("failed to parse %s URI %q: %w", protocolName, sourceURI, err)
	}

	return &storageURIParts{
		Bucket:     parsedURL.Host,
		ObjectName: strings.TrimPrefix(parsedURL.Path, "/"),
		RawPath:    parsedURL.Path,
		Netloc:     parsedURL.Host,
	}, nil
}

// fileProtocolHandler is the file protocol handler.
type fileProtocolHandler struct{}

func (h *fileProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolFile
}

func (h *fileProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	sourcePath := strings.TrimPrefix(sourceURI, "file://")
	sourcePath, err := url.PathUnescape(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to unescape file path: %w", err)
	}

	src, err := os.Open(sourcePath)
	if err != nil {
		return fmt.Errorf("failed to open source file %q: %w", sourcePath, err)
	}
	defer src.Close()

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, src)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// httpProtocolHandler is the HTTP/HTTPS protocol handler.
type httpProtocolHandler struct{}

func (h *httpProtocolHandler) Match(protocol Protocol) bool {
	// Handle every protocol not matched by a more specific handler (including https).
	return true
}

func (h *httpProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, sourceURI, nil)
	if err != nil {
		return fmt.Errorf("failed to create HTTP request: %w", err)
	}

	client := &http.Client{Timeout: opts.HTTPTimeout}
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("failed to execute HTTP request: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("HTTP request failed with status %d", resp.StatusCode)
	}

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, resp.Body)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// Downloader downloads files over the supported protocols.
type Downloader struct {
	handlers    []protocolHandler
	defaultOpts *DownloadOptions
}

// NewDownloader creates a new Downloader.
func NewDownloader(opts *DownloadOptions) *Downloader {
	if opts == nil {
		opts = DefaultDownloadOptions
	}

	// Cloud protocol handlers (GS, S3, Azure Blob Storage and ABFSS) are only
	// fully implemented in builds with the cloud build tag; the handlers must
	// be registered before the default HTTP handler, which stays last.
	handlers := []protocolHandler{&fileProtocolHandler{}}
	handlers = append(handlers, cloudHandlers(opts)...)
	handlers = append(handlers, &httpProtocolHandler{})

	return &Downloader{
		handlers:    handlers,
		defaultOpts: opts,
	}
}

// Download downloads a file to the given destination.
func (d *Downloader) Download(ctx context.Context, protocol Protocol, sourceURI string, destFile string, opts *DownloadOptions) error {
	if opts == nil {
		opts = d.defaultOpts
	}

	if !IsRemoteProtocol(protocol) {
		return fmt.Errorf("protocol %q is not a remote protocol", protocol)
	}

	// Find the handler matching the protocol.
	var handler protocolHandler
	for _, h := range d.handlers {
		if h.Match(protocol) {
			handler = h
			break
		}
	}

	if handler == nil {
		return fmt.Errorf("no handler found for protocol %q", protocol)
	}

	// Set up the destination writer.
	var destWriter io.Writer
	var tempFile *os.File
	var cleanupNeeded bool

	if opts.UseAtomicWrite {
		dir := filepath.Dir(destFile)
		var tempErr error
		tempFile, tempErr = os.CreateTemp(dir, ".download-*")
		if tempErr != nil {
			return fmt.Errorf("failed to create temporary file: %w", tempErr)
		}

		destWriter = tempFile
		cleanupNeeded = true

		defer func() {
			if cleanupNeeded && tempFile != nil {
				tempFile.Close()
				os.Remove(tempFile.Name())
			}
		}()
	} else {
		f, err := os.Create(destFile)
		if err != nil {
			return fmt.Errorf("failed to create destination file %q: %w", destFile, err)
		}
		destWriter = f
		cleanupNeeded = true

		defer func() {
			if cleanupNeeded {
				f.Close()
			}
		}()
	}

	// Perform the download.
	err := handler.Download(ctx, sourceURI, destWriter, opts)
	if err != nil {
		if closer, ok := destWriter.(io.Closer); ok {
			closer.Close()
		}
		if opts.UseAtomicWrite && tempFile != nil {
			os.Remove(tempFile.Name())
		}
		return err
	}

	// Post-download handling.
	if opts.UseAtomicWrite && tempFile != nil {
		// Close the temporary file first.
		tempFileName := tempFile.Name() // Keep the name for later operations.
		if err := tempFile.Close(); err != nil {
			os.Remove(tempFileName)
			return fmt.Errorf("failed to close temporary file: %w", err)
		}

		// Rename the temporary file to the destination.
		if err := os.Rename(tempFileName, destFile); err != nil {
			os.Remove(tempFileName)
			return fmt.Errorf("failed to rename temporary file to destination: %w", err)
		}
		cleanupNeeded = false // No cleanup needed once the rename succeeds.
		tempFile = nil        // Drop the reference so the defer does not run again.
	} else {
		// In non-atomic mode (or when tempFile is nil) the destination file must be closed explicitly.
		if closer, ok := destWriter.(*os.File); ok {
			if err := closer.Close(); err != nil {
				return fmt.Errorf("failed to close destination file: %w", err)
			}
		}
	}

	return nil
}

// DownloadRemoteURI downloads a file from a remote URI to a local path.
// It is a wrapper kept for backward compatibility.
func DownloadRemoteURI(ctx context.Context, protocol Protocol, sourceURI string, destFile string, opts *DownloadOptions) error {
	downloader := NewDownloader(opts)
	return downloader.Download(ctx, protocol, sourceURI, destFile, opts)
}
