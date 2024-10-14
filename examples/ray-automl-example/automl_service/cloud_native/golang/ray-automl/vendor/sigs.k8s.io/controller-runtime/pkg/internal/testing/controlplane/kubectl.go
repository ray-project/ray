/*
Copyright 2021 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controlplane

import (
	"bytes"
	"fmt"
	"io"
	"net/url"
	"os/exec"

	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	kcapi "k8s.io/client-go/tools/clientcmd/api"

	"sigs.k8s.io/controller-runtime/pkg/internal/testing/process"
)

const (
	envtestName = "envtest"
)

// KubeConfigFromREST reverse-engineers a kubeconfig file from a rest.Config.
// The options are tailored towards the rest.Configs we generate, so they're
// not broadly applicable.
//
// This is not intended to be exposed beyond internal for the above reasons.
func KubeConfigFromREST(cfg *rest.Config) ([]byte, error) {
	kubeConfig := kcapi.NewConfig()
	protocol := "https"
	if !rest.IsConfigTransportTLS(*cfg) {
		protocol = "http"
	}

	// cfg.Host is a URL, so we need to parse it so we can properly append the API path
	baseURL, err := url.Parse(cfg.Host)
	if err != nil {
		return nil, fmt.Errorf("unable to interpret config's host value as a URL: %w", err)
	}

	kubeConfig.Clusters[envtestName] = &kcapi.Cluster{
		// TODO(directxman12): if client-go ever decides to expose defaultServerUrlFor(config),
		// we can just use that.  Note that this is not the same as the public DefaultServerURL,
		// which requires us to pass a bunch of stuff in manually.
		Server:                   (&url.URL{Scheme: protocol, Host: baseURL.Host, Path: cfg.APIPath}).String(),
		CertificateAuthorityData: cfg.CAData,
	}
	kubeConfig.AuthInfos[envtestName] = &kcapi.AuthInfo{
		// try to cover all auth strategies that aren't plugins
		ClientCertificateData: cfg.CertData,
		ClientKeyData:         cfg.KeyData,
		Token:                 cfg.BearerToken,
		Username:              cfg.Username,
		Password:              cfg.Password,
	}
	kcCtx := kcapi.NewContext()
	kcCtx.Cluster = envtestName
	kcCtx.AuthInfo = envtestName
	kubeConfig.Contexts[envtestName] = kcCtx
	kubeConfig.CurrentContext = envtestName

	contents, err := clientcmd.Write(*kubeConfig)
	if err != nil {
		return nil, fmt.Errorf("unable to serialize kubeconfig file: %w", err)
	}
	return contents, nil
}

// KubeCtl is a wrapper around the kubectl binary.
type KubeCtl struct {
	// Path where the kubectl binary can be found.
	//
	// If this is left empty, we will attempt to locate a binary, by checking for
	// the TEST_ASSET_KUBECTL environment variable, and the default test assets
	// directory. See the "Binaries" section above (in doc.go) for details.
	Path string

	// Opts can be used to configure additional flags which will be used each
	// time the wrapped binary is called.
	//
	// For example, you might want to use this to set the URL of the APIServer to
	// connect to.
	Opts []string
}

// Run executes the wrapped binary with some preconfigured options and the
// arguments given to this method. It returns Readers for the stdout and
// stderr.
func (k *KubeCtl) Run(args ...string) (stdout, stderr io.Reader, err error) {
	if k.Path == "" {
		k.Path = process.BinPathFinder("kubectl", "")
	}

	stdoutBuffer := &bytes.Buffer{}
	stderrBuffer := &bytes.Buffer{}
	allArgs := append(k.Opts, args...)

	cmd := exec.Command(k.Path, allArgs...)
	cmd.Stdout = stdoutBuffer
	cmd.Stderr = stderrBuffer

	err = cmd.Run()

	return stdoutBuffer, stderrBuffer, err
}
