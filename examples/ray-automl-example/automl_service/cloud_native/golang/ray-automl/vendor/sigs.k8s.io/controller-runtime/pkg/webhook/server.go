/*
Copyright 2018 The Kubernetes Authors.

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

package webhook

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	kscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/certwatcher"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/internal/httpserver"
	"sigs.k8s.io/controller-runtime/pkg/runtime/inject"
	"sigs.k8s.io/controller-runtime/pkg/webhook/internal/metrics"
)

// DefaultPort is the default port that the webhook server serves.
var DefaultPort = 9443

// Server is an admission webhook server that can serve traffic and
// generates related k8s resources for deploying.
//
// TLS is required for a webhook to be accessed by kubernetes, so
// you must provide a CertName and KeyName or have valid cert/key
// at the default locations (tls.crt and tls.key). If you do not
// want to configure TLS (i.e for testing purposes) run an
// admission.StandaloneWebhook in your own server.
type Server struct {
	// Host is the address that the server will listen on.
	// Defaults to "" - all addresses.
	Host string

	// Port is the port number that the server will serve.
	// It will be defaulted to 9443 if unspecified.
	Port int

	// CertDir is the directory that contains the server key and certificate. The
	// server key and certificate.
	CertDir string

	// CertName is the server certificate name. Defaults to tls.crt.
	CertName string

	// KeyName is the server key name. Defaults to tls.key.
	KeyName string

	// ClientCAName is the CA certificate name which server used to verify remote(client)'s certificate.
	// Defaults to "", which means server does not verify client's certificate.
	ClientCAName string

	// TLSVersion is the minimum version of TLS supported. Accepts
	// "", "1.0", "1.1", "1.2" and "1.3" only ("" is equivalent to "1.0" for backwards compatibility)
	TLSMinVersion string

	// TLSOpts is used to allow configuring the TLS config used for the server
	TLSOpts []func(*tls.Config)

	// WebhookMux is the multiplexer that handles different webhooks.
	WebhookMux *http.ServeMux

	// webhooks keep track of all registered webhooks for dependency injection,
	// and to provide better panic messages on duplicate webhook registration.
	webhooks map[string]http.Handler

	// setFields allows injecting dependencies from an external source
	setFields inject.Func

	// defaultingOnce ensures that the default fields are only ever set once.
	defaultingOnce sync.Once

	// started is set to true immediately before the server is started
	// and thus can be used to check if the server has been started
	started bool

	// mu protects access to the webhook map & setFields for Start, Register, etc
	mu sync.Mutex
}

// setDefaults does defaulting for the Server.
func (s *Server) setDefaults() {
	s.webhooks = map[string]http.Handler{}
	if s.WebhookMux == nil {
		s.WebhookMux = http.NewServeMux()
	}

	if s.Port <= 0 {
		s.Port = DefaultPort
	}

	if len(s.CertDir) == 0 {
		s.CertDir = filepath.Join(os.TempDir(), "k8s-webhook-server", "serving-certs")
	}

	if len(s.CertName) == 0 {
		s.CertName = "tls.crt"
	}

	if len(s.KeyName) == 0 {
		s.KeyName = "tls.key"
	}
}

// NeedLeaderElection implements the LeaderElectionRunnable interface, which indicates
// the webhook server doesn't need leader election.
func (*Server) NeedLeaderElection() bool {
	return false
}

// Register marks the given webhook as being served at the given path.
// It panics if two hooks are registered on the same path.
func (s *Server) Register(path string, hook http.Handler) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.defaultingOnce.Do(s.setDefaults)
	if _, found := s.webhooks[path]; found {
		panic(fmt.Errorf("can't register duplicate path: %v", path))
	}
	// TODO(directxman12): call setfields if we've already started the server
	s.webhooks[path] = hook
	s.WebhookMux.Handle(path, metrics.InstrumentedHook(path, hook))

	regLog := log.WithValues("path", path)
	regLog.Info("Registering webhook")

	// we've already been "started", inject dependencies here.
	// Otherwise, InjectFunc will do this for us later.
	if s.setFields != nil {
		if err := s.setFields(hook); err != nil {
			// TODO(directxman12): swallowing this error isn't great, but we'd have to
			// change the signature to fix that
			regLog.Error(err, "unable to inject fields into webhook during registration")
		}

		baseHookLog := log.WithName("webhooks")

		// NB(directxman12): we don't propagate this further by wrapping setFields because it's
		// unclear if this is how we want to deal with log propagation.  In this specific instance,
		// we want to be able to pass a logger to webhooks because they don't know their own path.
		if _, err := inject.LoggerInto(baseHookLog.WithValues("webhook", path), hook); err != nil {
			regLog.Error(err, "unable to logger into webhook during registration")
		}
	}
}

// StartStandalone runs a webhook server without
// a controller manager.
func (s *Server) StartStandalone(ctx context.Context, scheme *runtime.Scheme) error {
	// Use the Kubernetes client-go scheme if none is specified
	if scheme == nil {
		scheme = kscheme.Scheme
	}

	if err := s.InjectFunc(func(i interface{}) error {
		if _, err := inject.SchemeInto(scheme, i); err != nil {
			return err
		}
		return nil
	}); err != nil {
		return err
	}

	return s.Start(ctx)
}

// tlsVersion converts from human-readable TLS version (for example "1.1")
// to the values accepted by tls.Config (for example 0x301).
func tlsVersion(version string) (uint16, error) {
	switch version {
	// default is previous behaviour
	case "":
		return tls.VersionTLS10, nil
	case "1.0":
		return tls.VersionTLS10, nil
	case "1.1":
		return tls.VersionTLS11, nil
	case "1.2":
		return tls.VersionTLS12, nil
	case "1.3":
		return tls.VersionTLS13, nil
	default:
		return 0, fmt.Errorf("invalid TLSMinVersion %v: expects 1.0, 1.1, 1.2, 1.3 or empty", version)
	}
}

// Start runs the server.
// It will install the webhook related resources depend on the server configuration.
func (s *Server) Start(ctx context.Context) error {
	s.defaultingOnce.Do(s.setDefaults)

	baseHookLog := log.WithName("webhooks")
	baseHookLog.Info("Starting webhook server")

	certPath := filepath.Join(s.CertDir, s.CertName)
	keyPath := filepath.Join(s.CertDir, s.KeyName)

	certWatcher, err := certwatcher.New(certPath, keyPath)
	if err != nil {
		return err
	}

	go func() {
		if err := certWatcher.Start(ctx); err != nil {
			log.Error(err, "certificate watcher error")
		}
	}()

	tlsMinVersion, err := tlsVersion(s.TLSMinVersion)
	if err != nil {
		return err
	}

	cfg := &tls.Config{ //nolint:gosec
		NextProtos:     []string{"h2"},
		GetCertificate: certWatcher.GetCertificate,
		MinVersion:     tlsMinVersion,
	}

	// load CA to verify client certificate
	if s.ClientCAName != "" {
		certPool := x509.NewCertPool()
		clientCABytes, err := os.ReadFile(filepath.Join(s.CertDir, s.ClientCAName))
		if err != nil {
			return fmt.Errorf("failed to read client CA cert: %w", err)
		}

		ok := certPool.AppendCertsFromPEM(clientCABytes)
		if !ok {
			return fmt.Errorf("failed to append client CA cert to CA pool")
		}

		cfg.ClientCAs = certPool
		cfg.ClientAuth = tls.RequireAndVerifyClientCert
	}

	// fallback TLS config ready, will now mutate if passer wants full control over it
	for _, op := range s.TLSOpts {
		op(cfg)
	}

	listener, err := tls.Listen("tcp", net.JoinHostPort(s.Host, strconv.Itoa(s.Port)), cfg)
	if err != nil {
		return err
	}

	log.Info("Serving webhook server", "host", s.Host, "port", s.Port)

	srv := httpserver.New(s.WebhookMux)

	idleConnsClosed := make(chan struct{})
	go func() {
		<-ctx.Done()
		log.Info("shutting down webhook server")

		// TODO: use a context with reasonable timeout
		if err := srv.Shutdown(context.Background()); err != nil {
			// Error from closing listeners, or context timeout
			log.Error(err, "error shutting down the HTTP server")
		}
		close(idleConnsClosed)
	}()

	s.mu.Lock()
	s.started = true
	s.mu.Unlock()
	if err := srv.Serve(listener); err != nil && err != http.ErrServerClosed {
		return err
	}

	<-idleConnsClosed
	return nil
}

// StartedChecker returns an healthz.Checker which is healthy after the
// server has been started.
func (s *Server) StartedChecker() healthz.Checker {
	config := &tls.Config{
		InsecureSkipVerify: true, //nolint:gosec // config is used to connect to our own webhook port.
	}
	return func(req *http.Request) error {
		s.mu.Lock()
		defer s.mu.Unlock()

		if !s.started {
			return fmt.Errorf("webhook server has not been started yet")
		}

		d := &net.Dialer{Timeout: 10 * time.Second}
		conn, err := tls.DialWithDialer(d, "tcp", net.JoinHostPort(s.Host, strconv.Itoa(s.Port)), config)
		if err != nil {
			return fmt.Errorf("webhook server is not reachable: %w", err)
		}

		if err := conn.Close(); err != nil {
			return fmt.Errorf("webhook server is not reachable: closing connection: %w", err)
		}

		return nil
	}
}

// InjectFunc injects the field setter into the server.
func (s *Server) InjectFunc(f inject.Func) error {
	s.setFields = f

	// inject fields here that weren't injected in Register because we didn't have setFields yet.
	baseHookLog := log.WithName("webhooks")
	for hookPath, webhook := range s.webhooks {
		if err := s.setFields(webhook); err != nil {
			return err
		}

		// NB(directxman12): we don't propagate this further by wrapping setFields because it's
		// unclear if this is how we want to deal with log propagation.  In this specific instance,
		// we want to be able to pass a logger to webhooks because they don't know their own path.
		if _, err := inject.LoggerInto(baseHookLog.WithValues("webhook", hookPath), webhook); err != nil {
			return err
		}
	}
	return nil
}
