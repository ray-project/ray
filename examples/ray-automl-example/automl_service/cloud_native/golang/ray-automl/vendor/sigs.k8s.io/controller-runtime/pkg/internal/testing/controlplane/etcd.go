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
	"io"
	"net"
	"net/url"
	"strconv"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/internal/testing/addr"
	"sigs.k8s.io/controller-runtime/pkg/internal/testing/process"
)

// Etcd knows how to run an etcd server.
type Etcd struct {
	// URL is the address the Etcd should listen on for client connections.
	//
	// If this is not specified, we default to a random free port on localhost.
	URL *url.URL

	// Path is the path to the etcd binary.
	//
	// If this is left as the empty string, we will attempt to locate a binary,
	// by checking for the TEST_ASSET_ETCD environment variable, and the default
	// test assets directory. See the "Binaries" section above (in doc.go) for
	// details.
	Path string

	// Args is a list of arguments which will passed to the Etcd binary. Before
	// they are passed on, the`y will be evaluated as go-template strings. This
	// means you can use fields which are defined and exported on this Etcd
	// struct (e.g. "--data-dir={{ .Dir }}").
	// Those templates will be evaluated after the defaulting of the Etcd's
	// fields has already happened and just before the binary actually gets
	// started. Thus you have access to calculated fields like `URL` and others.
	//
	// If not specified, the minimal set of arguments to run the Etcd will be
	// used.
	//
	// They will be loaded into the same argument set as Configure.  Each flag
	// will be Append-ed to the configured arguments just before launch.
	//
	// Deprecated: use Configure instead.
	Args []string

	// DataDir is a path to a directory in which etcd can store its state.
	//
	// If left unspecified, then the Start() method will create a fresh temporary
	// directory, and the Stop() method will clean it up.
	DataDir string

	// StartTimeout, StopTimeout specify the time the Etcd is allowed to
	// take when starting and stopping before an error is emitted.
	//
	// If not specified, these default to 20 seconds.
	StartTimeout time.Duration
	StopTimeout  time.Duration

	// Out, Err specify where Etcd should write its StdOut, StdErr to.
	//
	// If not specified, the output will be discarded.
	Out io.Writer
	Err io.Writer

	// processState contains the actual details about this running process
	processState *process.State

	// args contains the structured arguments to use for running etcd.
	// Lazily initialized by .Configure(), Defaulted eventually with .defaultArgs()
	args *process.Arguments

	// listenPeerURL is the address the Etcd should listen on for peer connections.
	// It's automatically generated and a random port is picked during execution.
	listenPeerURL *url.URL
}

// Start starts the etcd, waits for it to come up, and returns an error, if one
// occurred.
func (e *Etcd) Start() error {
	if err := e.setProcessState(); err != nil {
		return err
	}
	return e.processState.Start(e.Out, e.Err)
}

func (e *Etcd) setProcessState() error {
	e.processState = &process.State{
		Dir:          e.DataDir,
		Path:         e.Path,
		StartTimeout: e.StartTimeout,
		StopTimeout:  e.StopTimeout,
	}

	// unconditionally re-set this so we can successfully restart
	// TODO(directxman12): we supported this in the past, but do we actually
	// want to support re-using an API server object to restart?  The loss
	// of provisioned users is surprising to say the least.
	if err := e.processState.Init("etcd"); err != nil {
		return err
	}

	// Set the listen url.
	if e.URL == nil {
		port, host, err := addr.Suggest("")
		if err != nil {
			return err
		}
		e.URL = &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		}
	}

	// Set the listen peer URL.
	{
		port, host, err := addr.Suggest("")
		if err != nil {
			return err
		}
		e.listenPeerURL = &url.URL{
			Scheme: "http",
			Host:   net.JoinHostPort(host, strconv.Itoa(port)),
		}
	}

	// can use /health as of etcd 3.3.0
	e.processState.HealthCheck.URL = *e.URL
	e.processState.HealthCheck.Path = "/health"

	e.DataDir = e.processState.Dir
	e.Path = e.processState.Path
	e.StartTimeout = e.processState.StartTimeout
	e.StopTimeout = e.processState.StopTimeout

	var err error
	e.processState.Args, e.Args, err = process.TemplateAndArguments(e.Args, e.Configure(), process.TemplateDefaults{ //nolint:staticcheck
		Data:     e,
		Defaults: e.defaultArgs(),
	})
	return err
}

// Stop stops this process gracefully, waits for its termination, and cleans up
// the DataDir if necessary.
func (e *Etcd) Stop() error {
	if e.processState.DirNeedsCleaning {
		e.DataDir = "" // reset the directory if it was randomly allocated, so that we can safely restart
	}
	return e.processState.Stop()
}

func (e *Etcd) defaultArgs() map[string][]string {
	args := map[string][]string{
		"listen-peer-urls": {e.listenPeerURL.String()},
		"data-dir":         {e.DataDir},
	}
	if e.URL != nil {
		args["advertise-client-urls"] = []string{e.URL.String()}
		args["listen-client-urls"] = []string{e.URL.String()}
	}

	// Add unsafe no fsync, available from etcd 3.5
	if ok, _ := e.processState.CheckFlag("unsafe-no-fsync"); ok {
		args["unsafe-no-fsync"] = []string{"true"}
	}
	return args
}

// Configure returns Arguments that may be used to customize the
// flags used to launch etcd.  A set of defaults will
// be applied underneath.
func (e *Etcd) Configure() *process.Arguments {
	if e.args == nil {
		e.args = process.EmptyArguments()
	}
	return e.args
}

// EtcdDefaultArgs exposes the default args for Etcd so that you
// can use those to append your own additional arguments.
var EtcdDefaultArgs = []string{
	"--listen-peer-urls=http://localhost:0",
	"--advertise-client-urls={{ if .URL }}{{ .URL.String }}{{ end }}",
	"--listen-client-urls={{ if .URL }}{{ .URL.String }}{{ end }}",
	"--data-dir={{ .DataDir }}",
}
