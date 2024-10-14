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
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/internal/testing/addr"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/certs"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/process"
)

const (
	// saKeyFile is the name of the service account signing private key file.
	saKeyFile = "sa-signer.key"
	// saKeyFile is the name of the service account signing public key (cert) file.
	saCertFile = "sa-signer.crt"
)

// SecureServing provides/configures how the API server serves on the secure port.
type SecureServing struct {
	// ListenAddr contains the host & port to serve on.
	//
	// Configurable.  If unset, it will be defaulted.
	process.ListenAddr
	// CA contains the CA that signed the API server's serving certificates.
	//
	// Read-only.
	CA []byte
	// Authn can be used to provision users, and override what type of
	// authentication is used to provision users.
	//
	// Configurable.  If unset, it will be defaulted.
	Authn
}

// APIServer knows how to run a kubernetes apiserver.
type APIServer struct {
	// URL is the address the ApiServer should listen on for client
	// connections.
	//
	// If set, this will configure the *insecure* serving details.
	// If unset, it will contain the insecure port if insecure serving is enabled,
	// and otherwise will contain the secure port.
	//
	// If this is not specified, we default to a random free port on localhost.
	//
	// Deprecated: use InsecureServing (for the insecure URL) or SecureServing, ideally.
	URL *url.URL

	// SecurePort is the additional secure port that the APIServer should listen on.
	//
	// If set, this will override SecureServing.Port.
	//
	// Deprecated: use SecureServing.
	SecurePort int

	// SecureServing indicates how the API server will serve on the secure port.
	//
	// Some parts are configurable.  Will be defaulted if unset.
	SecureServing

	// InsecureServing indicates how the API server will serve on the insecure port.
	//
	// If unset, the insecure port will be disabled.  Set to an empty struct to get
	// default values.
	//
	// Deprecated: does not work with Kubernetes versions 1.20 and above.  Use secure
	// serving instead.
	InsecureServing *process.ListenAddr

	// Path is the path to the apiserver binary.
	//
	// If this is left as the empty string, we will attempt to locate a binary,
	// by checking for the TEST_ASSET_KUBE_APISERVER environment variable, and
	// the default test assets directory. See the "Binaries" section above (in
	// doc.go) for details.
	Path string

	// Args is a list of arguments which will passed to the APIServer binary.
	// Before they are passed on, they will be evaluated as go-template strings.
	// This means you can use fields which are defined and exported on this
	// APIServer struct (e.g. "--cert-dir={{ .Dir }}").
	// Those templates will be evaluated after the defaulting of the APIServer's
	// fields has already happened and just before the binary actually gets
	// started. Thus you have access to calculated fields like `URL` and others.
	//
	// If not specified, the minimal set of arguments to run the APIServer will
	// be used.
	//
	// They will be loaded into the same argument set as Configure.  Each flag
	// will be Append-ed to the configured arguments just before launch.
	//
	// Deprecated: use Configure instead.
	Args []string

	// CertDir is a path to a directory containing whatever certificates the
	// APIServer will need.
	//
	// If left unspecified, then the Start() method will create a fresh temporary
	// directory, and the Stop() method will clean it up.
	CertDir string

	// EtcdURL is the URL of the Etcd the APIServer should use.
	//
	// If this is not specified, the Start() method will return an error.
	EtcdURL *url.URL

	// StartTimeout, StopTimeout specify the time the APIServer is allowed to
	// take when starting and stoppping before an error is emitted.
	//
	// If not specified, these default to 20 seconds.
	StartTimeout time.Duration
	StopTimeout  time.Duration

	// Out, Err specify where APIServer should write its StdOut, StdErr to.
	//
	// If not specified, the output will be discarded.
	Out io.Writer
	Err io.Writer

	processState *process.State

	// args contains the structured arguments to use for running the API server
	// Lazily initialized by .Configure(), Defaulted eventually with .defaultArgs()
	args *process.Arguments
}

// Configure returns Arguments that may be used to customize the
// flags used to launch the API server.  A set of defaults will
// be applied underneath.
func (s *APIServer) Configure() *process.Arguments {
	if s.args == nil {
		s.args = process.EmptyArguments()
	}
	return s.args
}

// Start starts the apiserver, waits for it to come up, and returns an error,
// if occurred.
func (s *APIServer) Start() error {
	if err := s.prepare(); err != nil {
		return err
	}
	return s.processState.Start(s.Out, s.Err)
}

func (s *APIServer) prepare() error {
	if err := s.setProcessState(); err != nil {
		return err
	}
	return s.Authn.Start()
}

// configurePorts configures the serving ports for this API server.
//
// Most of this method currently deals with making the deprecated fields
// take precedence over the new fields.
func (s *APIServer) configurePorts() error {
	// prefer the old fields to the new fields if a user set one,
	// otherwise, default the new fields and populate the old ones.

	// Insecure: URL, InsecureServing
	if s.URL != nil {
		s.InsecureServing = &process.ListenAddr{
			Address: s.URL.Hostname(),
			Port:    s.URL.Port(),
		}
	} else if insec := s.InsecureServing; insec != nil {
		if insec.Port == "" || insec.Address == "" {
			port, host, err := addr.Suggest("")
			if err != nil {
				return fmt.Errorf("unable to provision unused insecure port: %w", err)
			}
			s.InsecureServing.Port = strconv.Itoa(port)
			s.InsecureServing.Address = host
		}
		s.URL = s.InsecureServing.URL("http", "")
	}

	// Secure: SecurePort, SecureServing
	if s.SecurePort != 0 {
		s.SecureServing.Port = strconv.Itoa(s.SecurePort)
		// if we don't have an address, try the insecure address, and otherwise
		// default to loopback.
		if s.SecureServing.Address == "" {
			if s.InsecureServing != nil {
				s.SecureServing.Address = s.InsecureServing.Address
			} else {
				s.SecureServing.Address = "127.0.0.1"
			}
		}
	} else if s.SecureServing.Port == "" || s.SecureServing.Address == "" {
		port, host, err := addr.Suggest("")
		if err != nil {
			return fmt.Errorf("unable to provision unused secure port: %w", err)
		}
		s.SecureServing.Port = strconv.Itoa(port)
		s.SecureServing.Address = host
		s.SecurePort = port
	}

	return nil
}

func (s *APIServer) setProcessState() error {
	if s.EtcdURL == nil {
		return fmt.Errorf("expected EtcdURL to be configured")
	}

	var err error

	// unconditionally re-set this so we can successfully restart
	// TODO(directxman12): we supported this in the past, but do we actually
	// want to support re-using an API server object to restart?  The loss
	// of provisioned users is surprising to say the least.
	s.processState = &process.State{
		Dir:          s.CertDir,
		Path:         s.Path,
		StartTimeout: s.StartTimeout,
		StopTimeout:  s.StopTimeout,
	}
	if err := s.processState.Init("kube-apiserver"); err != nil {
		return err
	}

	if err := s.configurePorts(); err != nil {
		return err
	}

	// the secure port will always be on, so use that
	s.processState.HealthCheck.URL = *s.SecureServing.URL("https", "/healthz")

	s.CertDir = s.processState.Dir
	s.Path = s.processState.Path
	s.StartTimeout = s.processState.StartTimeout
	s.StopTimeout = s.processState.StopTimeout

	if err := s.populateAPIServerCerts(); err != nil {
		return err
	}

	if s.SecureServing.Authn == nil {
		authn, err := NewCertAuthn()
		if err != nil {
			return err
		}
		s.SecureServing.Authn = authn
	}

	if err := s.Authn.Configure(s.CertDir, s.Configure()); err != nil {
		return err
	}

	// NB(directxman12): insecure port is a mess:
	// - 1.19 and below have the `--insecure-port` flag, and require it to be set to zero to
	//   disable it, otherwise the default will be used and we'll conflict.
	// - 1.20 requires the flag to be unset or set to zero, and yells at you if you configure it
	// - 1.24 won't have the flag at all...
	//
	// In an effort to automatically do the right thing during this mess, we do feature discovery
	// on the flags, and hope that we've "parsed" them properly.
	//
	// TODO(directxman12): once we support 1.20 as the min version (might be when 1.24 comes out,
	// might be around 1.25 or 1.26), remove this logic and the corresponding line in API server's
	// default args.
	if err := s.discoverFlags(); err != nil {
		return err
	}

	s.processState.Args, s.Args, err = process.TemplateAndArguments(s.Args, s.Configure(), process.TemplateDefaults{ //nolint:staticcheck
		Data:     s,
		Defaults: s.defaultArgs(),
		MinimalDefaults: map[string][]string{
			// as per kubernetes-sigs/controller-runtime#641, we need this (we
			// probably need other stuff too, but this is the only thing that was
			// previously considered a "minimal default")
			"service-cluster-ip-range": {"10.0.0.0/24"},

			// we need *some* authorization mode for health checks on the secure port,
			// so default to RBAC unless the user set something else (in which case
			// this'll be ignored due to SliceToArguments using AppendNoDefaults).
			"authorization-mode": {"RBAC"},
		},
	})
	if err != nil {
		return err
	}

	return nil
}

// discoverFlags checks for certain flags that *must* be set in certain
// versions, and *must not* be set in others.
func (s *APIServer) discoverFlags() error {
	// Present: <1.24, Absent: >= 1.24
	present, err := s.processState.CheckFlag("insecure-port")
	if err != nil {
		return err
	}

	if !present {
		s.Configure().Disable("insecure-port")
	}

	return nil
}

func (s *APIServer) defaultArgs() map[string][]string {
	args := map[string][]string{
		"service-cluster-ip-range": {"10.0.0.0/24"},
		"allow-privileged":         {"true"},
		// we're keeping this disabled because if enabled, default SA is
		// missing which would force all tests to create one in normal
		// apiserver operation this SA is created by controller, but that is
		// not run in integration environment
		"disable-admission-plugins": {"ServiceAccount"},
		"cert-dir":                  {s.CertDir},
		"authorization-mode":        {"RBAC"},
		"secure-port":               {s.SecureServing.Port},
		// NB(directxman12): previously we didn't set the bind address for the secure
		// port.  It *shouldn't* make a difference unless people are doing something really
		// funky, but if you start to get bug reports look here ;-)
		"bind-address": {s.SecureServing.Address},

		// required on 1.20+, fine to leave on for <1.20
		"service-account-issuer":           {s.SecureServing.URL("https", "/").String()},
		"service-account-key-file":         {filepath.Join(s.CertDir, saCertFile)},
		"service-account-signing-key-file": {filepath.Join(s.CertDir, saKeyFile)},
	}
	if s.EtcdURL != nil {
		args["etcd-servers"] = []string{s.EtcdURL.String()}
	}
	if s.URL != nil {
		args["insecure-port"] = []string{s.URL.Port()}
		args["insecure-bind-address"] = []string{s.URL.Hostname()}
	} else {
		// TODO(directxman12): remove this once 1.21 is the lowest version we support
		// (this might be a while, but this line'll break as of 1.24, so see the comment
		// in Start
		args["insecure-port"] = []string{"0"}
	}
	return args
}

func (s *APIServer) populateAPIServerCerts() error {
	_, statErr := os.Stat(filepath.Join(s.CertDir, "apiserver.crt"))
	if !os.IsNotExist(statErr) {
		return statErr
	}

	ca, err := certs.NewTinyCA()
	if err != nil {
		return err
	}

	servingCerts, err := ca.NewServingCert()
	if err != nil {
		return err
	}

	certData, keyData, err := servingCerts.AsBytes()
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(s.CertDir, "apiserver.crt"), certData, 0640); err != nil { //nolint:gosec
		return err
	}
	if err := os.WriteFile(filepath.Join(s.CertDir, "apiserver.key"), keyData, 0640); err != nil { //nolint:gosec
		return err
	}

	s.SecureServing.CA = ca.CA.CertBytes()

	// service account signing files too
	saCA, err := certs.NewTinyCA()
	if err != nil {
		return err
	}

	saCert, saKey, err := saCA.CA.AsBytes()
	if err != nil {
		return err
	}

	if err := os.WriteFile(filepath.Join(s.CertDir, saCertFile), saCert, 0640); err != nil { //nolint:gosec
		return err
	}
	return os.WriteFile(filepath.Join(s.CertDir, saKeyFile), saKey, 0640) //nolint:gosec
}

// Stop stops this process gracefully, waits for its termination, and cleans up
// the CertDir if necessary.
func (s *APIServer) Stop() error {
	if s.processState != nil {
		if s.processState.DirNeedsCleaning {
			s.CertDir = "" // reset the directory if it was randomly allocated, so that we can safely restart
		}
		if err := s.processState.Stop(); err != nil {
			return err
		}
	}
	return s.Authn.Stop()
}

// APIServerDefaultArgs exposes the default args for the APIServer so that you
// can use those to append your own additional arguments.
//
// Note that these arguments don't handle newer API servers well to due the more
// complex feature detection neeeded.  It's recommended that you switch to .Configure
// as you upgrade API server versions.
//
// Deprecated: use APIServer.Configure().
var APIServerDefaultArgs = []string{
	"--advertise-address=127.0.0.1",
	"--etcd-servers={{ if .EtcdURL }}{{ .EtcdURL.String }}{{ end }}",
	"--cert-dir={{ .CertDir }}",
	"--insecure-port={{ if .URL }}{{ .URL.Port }}{{else}}0{{ end }}",
	"{{ if .URL }}--insecure-bind-address={{ .URL.Hostname }}{{ end }}",
	"--secure-port={{ if .SecurePort }}{{ .SecurePort }}{{ end }}",
	// we're keeping this disabled because if enabled, default SA is missing which would force all tests to create one
	// in normal apiserver operation this SA is created by controller, but that is not run in integration environment
	"--disable-admission-plugins=ServiceAccount",
	"--service-cluster-ip-range=10.0.0.0/24",
	"--allow-privileged=true",
	// NB(directxman12): we also enable RBAC if nothing else was enabled
}

// PrepareAPIServer is an internal-only (NEVER SHOULD BE EXPOSED)
// function that sets up the API server just before starting it,
// without actually starting it.  This saves time on tests.
//
// NB(directxman12): do not expose this outside of internal -- it's unsafe to
// use, because things like port allocation could race even more than they
// currently do if you later call start!
func PrepareAPIServer(s *APIServer) error {
	return s.prepare()
}

// APIServerArguments is an internal-only (NEVER SHOULD BE EXPOSED)
// function that sets up the API server just before starting it,
// without actually starting it.  It's public to make testing easier.
//
// NB(directxman12): do not expose this outside of internal.
func APIServerArguments(s *APIServer) []string {
	return s.processState.Args
}
