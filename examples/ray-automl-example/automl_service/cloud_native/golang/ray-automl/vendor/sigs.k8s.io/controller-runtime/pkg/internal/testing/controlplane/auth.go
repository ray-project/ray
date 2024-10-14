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
	"fmt"
	"os"
	"path/filepath"

	"k8s.io/client-go/rest"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/certs"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/process"
)

// User represents a Kubernetes user.
type User struct {
	// Name is the user's Name.
	Name string
	// Groups are the groups to which the user belongs.
	Groups []string
}

// Authn knows how to configure an API server for a particular type of authentication,
// and provision users under that authentication scheme.
//
// The methods must be called in the following order (as presented below in the interface
// for a mnemonic):
//
// 1. Configure
// 2. Start
// 3. AddUsers (0+ calls)
// 4. Stop.
type Authn interface {
	// Configure provides the working directory to this authenticator,
	// and configures the given API server arguments to make use of this authenticator.
	//
	// Should be called first.
	Configure(workDir string, args *process.Arguments) error
	// Start runs this authenticator.  Will be called just before API server start.
	//
	// Must be called after Configure.
	Start() error
	// AddUser provisions a user, returning a copy of the given base rest.Config
	// configured to authenticate as that users.
	//
	// May only be called while the authenticator is "running".
	AddUser(user User, baseCfg *rest.Config) (*rest.Config, error)
	// Stop shuts down this authenticator.
	Stop() error
}

// CertAuthn is an authenticator (Authn) that makes use of client certificate authn.
type CertAuthn struct {
	// ca is the CA used to sign the client certs
	ca *certs.TinyCA
	// certDir is the directory used to write the CA crt file
	// so that the API server can read it.
	certDir string
}

// NewCertAuthn creates a new client-cert-based Authn with a new CA.
func NewCertAuthn() (*CertAuthn, error) {
	ca, err := certs.NewTinyCA()
	if err != nil {
		return nil, fmt.Errorf("unable to provision client certificate auth CA: %w", err)
	}
	return &CertAuthn{
		ca: ca,
	}, nil
}

// AddUser provisions a new user that's authenticated via certificates, with
// the given uesrname and groups embedded in the certificate as expected by the
// API server.
func (c *CertAuthn) AddUser(user User, baseCfg *rest.Config) (*rest.Config, error) {
	certs, err := c.ca.NewClientCert(certs.ClientInfo{
		Name:   user.Name,
		Groups: user.Groups,
	})
	if err != nil {
		return nil, fmt.Errorf("unable to create client certificates for %s: %w", user.Name, err)
	}

	crt, key, err := certs.AsBytes()
	if err != nil {
		return nil, fmt.Errorf("unable to serialize client certificates for %s: %w", user.Name, err)
	}

	cfg := rest.CopyConfig(baseCfg)
	cfg.CertData = crt
	cfg.KeyData = key

	return cfg, nil
}

// caCrtPath returns the path to the on-disk client-cert CA crt file.
func (c *CertAuthn) caCrtPath() string {
	return filepath.Join(c.certDir, "client-cert-auth-ca.crt")
}

// Configure provides the working directory to this authenticator,
// and configures the given API server arguments to make use of this authenticator.
func (c *CertAuthn) Configure(workDir string, args *process.Arguments) error {
	c.certDir = workDir
	args.Set("client-ca-file", c.caCrtPath())
	return nil
}

// Start runs this authenticator.  Will be called just before API server start.
//
// Must be called after Configure.
func (c *CertAuthn) Start() error {
	if len(c.certDir) == 0 {
		return fmt.Errorf("start called before configure")
	}
	caCrt := c.ca.CA.CertBytes()
	if err := os.WriteFile(c.caCrtPath(), caCrt, 0640); err != nil { //nolint:gosec
		return fmt.Errorf("unable to save the client certificate CA to %s: %w", c.caCrtPath(), err)
	}

	return nil
}

// Stop shuts down this authenticator.
func (c *CertAuthn) Stop() error {
	// no-op -- our workdir is cleaned up for us automatically
	return nil
}
