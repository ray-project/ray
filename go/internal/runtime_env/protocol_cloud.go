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

//go:build cloud

package runtime_env

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azdatalake/service"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"

	"github.com/ray-project/ray/go/pkg/log"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/option"
	"google.golang.org/api/storage/v1"
)

// cloudHandlers returns the protocol handlers for the cloud storage protocols
// (GS, S3, Azure Blob Storage and ABFSS). They are only compiled with the
// "cloud" build tag: the Azure SDK pulls an azcore dependency graph with a
// known bazel repository visibility conflict in this workspace, and the GCS
// handler pulls the google cloud auth/proto chain that this bazel setup
// cannot compile.
func cloudHandlers(opts *DownloadOptions) []protocolHandler {
	return []protocolHandler{
		&gsProtocolHandler{},
		&s3ProtocolHandler{clientCache: newS3ClientCache(opts.HTTPTimeout)},
		&azureProtocolHandler{},
		&abfssProtocolHandler{},
	}
}

// gsProtocolHandler is the GS protocol handler.
type gsProtocolHandler struct{}

func (h *gsProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolGS
}

func (h *gsProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "GS")
	if err != nil {
		return err
	}

	bucket := parts.Bucket
	objectName := parts.ObjectName

	if bucket == "" || objectName == "" {
		return fmt.Errorf("invalid GS URI format: %s", sourceURI)
	}

	creds, err := google.FindDefaultCredentials(ctx, storage.DevstorageFullControlScope)
	var svc *storage.Service
	if err != nil {
		log.Log.Info("No GCS credentials found, attempting public access")
		svc, err = storage.NewService(ctx, option.WithoutAuthentication())
	} else {
		svc, err = storage.NewService(ctx, option.WithCredentials(creds))
	}
	if err != nil {
		return fmt.Errorf("failed to create GCS service: %w", err)
	}

	resp, err := svc.Objects.Get(bucket, objectName).Download()
	if err != nil {
		return fmt.Errorf("failed to download GCS object %q from bucket %q: %w", objectName, bucket, err)
	}
	defer resp.Body.Close()

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, resp.Body)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// s3ProtocolHandler is the S3 protocol handler.
type s3ProtocolHandler struct {
	clientCache *s3ClientCache
}

type s3ClientCache struct {
	authenticatedClient   *s3.Client
	unauthenticatedClient *s3.Client
	httpTimeout           time.Duration
	initAuthClientOnce    sync.Once
	initUnauthClientOnce  sync.Once
	authClientErr         error
	unauthClientErr       error
	hasCredentials        bool
	checkCredentialsOnce  sync.Once
}

func newS3ClientCache(timeout time.Duration) *s3ClientCache {
	return &s3ClientCache{
		httpTimeout: timeout,
	}
}

func (c *s3ClientCache) hasAWSCredentials(ctx context.Context) bool {
	c.checkCredentialsOnce.Do(func() {
		_, err := config.LoadDefaultConfig(ctx)
		c.hasCredentials = (err == nil)
	})
	return c.hasCredentials
}

func (c *s3ClientCache) getAuthenticatedClient(ctx context.Context) (*s3.Client, error) {
	c.initAuthClientOnce.Do(func() {
		cfg, err := config.LoadDefaultConfig(ctx)
		if err != nil {
			c.authClientErr = err
			return
		}

		c.authenticatedClient = s3.NewFromConfig(cfg)
	})

	if c.authClientErr != nil {
		return nil, c.authClientErr
	}
	return c.authenticatedClient, nil
}

func (c *s3ClientCache) getUnauthenticatedClient() *s3.Client {
	c.initUnauthClientOnce.Do(func() {
		httpClient := &http.Client{
			Timeout: c.httpTimeout,
		}

		c.unauthenticatedClient = s3.New(s3.Options{
			HTTPClient: httpClient,
		})
	})

	if c.unauthClientErr != nil {
		return nil // This shouldn't happen as initialization can't fail
	}
	return c.unauthenticatedClient
}

func (h *s3ProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolS3
}

func (h *s3ProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "S3")
	if err != nil {
		return err
	}

	bucket := parts.Bucket
	key := parts.ObjectName

	if bucket == "" || key == "" {
		return fmt.Errorf("invalid S3 URI format: %s", sourceURI)
	}

	var s3Client *s3.Client
	if !h.clientCache.hasAWSCredentials(ctx) {
		log.Log.Info("No AWS credentials found, using unsigned client for public buckets")
		s3Client = h.clientCache.getUnauthenticatedClient()
	} else {
		s3Client, err = h.clientCache.getAuthenticatedClient(ctx)
		if err != nil {
			return fmt.Errorf("failed to create authenticated S3 client: %w", err)
		}
	}

	result, err := s3Client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return fmt.Errorf("failed to get S3 object %q from bucket %q: %w", key, bucket, err)
	}
	defer result.Body.Close()

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, result.Body)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// azureProtocolHandler is the Azure protocol handler.
type azureProtocolHandler struct{}

func (h *azureProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolAzure
}

func (h *azureProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "Azure")
	if err != nil {
		return err
	}

	container := parts.Bucket
	blobName := parts.ObjectName

	if container == "" || blobName == "" {
		return fmt.Errorf("invalid Azure URI format: %s", sourceURI)
	}

	azureStorageAccount := os.Getenv("AZURE_STORAGE_ACCOUNT")
	if azureStorageAccount == "" {
		return fmt.Errorf("Azure Blob Storage authentication requires AZURE_STORAGE_ACCOUNT environment variable to be set")
	}

	accountURL := fmt.Sprintf("https://%s.blob.core.windows.net/", azureStorageAccount)

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential: %w", err)
	}

	client, err := azblob.NewClient(accountURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure Blob Service client: %w", err)
	}

	downloadResponse, err := client.DownloadStream(ctx, container, blobName, nil)
	if err != nil {
		return fmt.Errorf("failed to download Azure blob %q from container %q: %w", blobName, container, err)
	}
	defer downloadResponse.Body.Close()

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, downloadResponse.Body)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// abfssProtocolHandler is the ABFSS protocol handler.
type abfssProtocolHandler struct{}

func (h *abfssProtocolHandler) Match(protocol Protocol) bool {
	return protocol == ProtocolABFSS
}

func (h *abfssProtocolHandler) Download(ctx context.Context, sourceURI string, destWriter io.Writer, opts *DownloadOptions) error {
	parts, err := parseStorageURI(sourceURI, "ABFSS")
	if err != nil {
		return err
	}

	netloc := parts.Netloc
	path := parts.RawPath

	if netloc == "" || !strings.Contains(netloc, "@") {
		return fmt.Errorf("invalid ABFSS URI format - missing container@account: %s", sourceURI)
	}

	atIndex := strings.Index(netloc, "@")
	containerPart := netloc[:atIndex]
	hostnamePart := netloc[atIndex+1:]

	if containerPart == "" {
		return fmt.Errorf("invalid ABFSS URI format - empty container name: %s", sourceURI)
	}

	if hostnamePart == "" || !strings.HasSuffix(hostnamePart, ".dfs.core.windows.net") {
		return fmt.Errorf("invalid ABFSS URI format - invalid hostname (must end with .dfs.core.windows.net): %s", sourceURI)
	}

	accountNameParts := strings.Split(hostnamePart, ".")
	if len(accountNameParts) == 0 || accountNameParts[0] == "" {
		return fmt.Errorf("invalid ABFSS URI format - empty account name: %s", sourceURI)
	}
	azureStorageAccount := accountNameParts[0]

	blobName := strings.TrimPrefix(path, "/")
	if blobName == "" {
		return fmt.Errorf("invalid ABFSS URI format - empty path: %s", sourceURI)
	}

	serviceURL := fmt.Sprintf("https://%s.dfs.core.windows.net", azureStorageAccount)

	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure credential: %w", err)
	}

	client, err := service.NewClient(serviceURL, cred, nil)
	if err != nil {
		return fmt.Errorf("failed to create Azure Data Lake Storage client: %w", err)
	}

	fsClient := client.NewFileSystemClient(containerPart)
	fileClient := fsClient.NewFileClient(blobName)

	downloadResponse, err := fileClient.DownloadStream(ctx, nil)
	if err != nil {
		return fmt.Errorf("failed to download ADLS file %q from container %q: %w", blobName, containerPart, err)
	}
	defer downloadResponse.Body.Close()

	bufferedWriter := bufio.NewWriterSize(destWriter, opts.BufferSize)
	_, err = io.Copy(bufferedWriter, downloadResponse.Body)
	if err != nil {
		return err
	}
	return bufferedWriter.Flush()
}

// cloudProtocols returns the protocols whose download handlers are compiled
// into this build. Only reachable under the cloud build tag.
func cloudProtocols() []Protocol {
	return []Protocol{ProtocolS3, ProtocolGS, ProtocolAzure, ProtocolABFSS}
}
