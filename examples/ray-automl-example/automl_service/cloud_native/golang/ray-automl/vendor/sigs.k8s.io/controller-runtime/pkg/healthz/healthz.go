/*
Copyright 2014 The Kubernetes Authors.

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

package healthz

import (
	"fmt"
	"net/http"
	"path"
	"sort"
	"strings"

	"k8s.io/apimachinery/pkg/util/sets"
)

// Handler is an http.Handler that aggregates the results of the given
// checkers to the root path, and supports calling individual checkers on
// subpaths of the name of the checker.
//
// Adding checks on the fly is *not* threadsafe -- use a wrapper.
type Handler struct {
	Checks map[string]Checker
}

// checkStatus holds the output of a particular check.
type checkStatus struct {
	name     string
	healthy  bool
	excluded bool
}

func (h *Handler) serveAggregated(resp http.ResponseWriter, req *http.Request) {
	failed := false
	excluded := getExcludedChecks(req)

	parts := make([]checkStatus, 0, len(h.Checks))

	// calculate the results...
	for checkName, check := range h.Checks {
		// no-op the check if we've specified we want to exclude the check
		if excluded.Has(checkName) {
			excluded.Delete(checkName)
			parts = append(parts, checkStatus{name: checkName, healthy: true, excluded: true})
			continue
		}
		if err := check(req); err != nil {
			log.V(1).Info("healthz check failed", "checker", checkName, "error", err)
			parts = append(parts, checkStatus{name: checkName, healthy: false})
			failed = true
		} else {
			parts = append(parts, checkStatus{name: checkName, healthy: true})
		}
	}

	// ...default a check if none is present...
	if len(h.Checks) == 0 {
		parts = append(parts, checkStatus{name: "ping", healthy: true})
	}

	for _, c := range excluded.List() {
		log.V(1).Info("cannot exclude health check, no matches for it", "checker", c)
	}

	// ...sort to be consistent...
	sort.Slice(parts, func(i, j int) bool { return parts[i].name < parts[j].name })

	// ...and write out the result
	// TODO(directxman12): this should also accept a request for JSON content (via a accept header)
	_, forceVerbose := req.URL.Query()["verbose"]
	writeStatusesAsText(resp, parts, excluded, failed, forceVerbose)
}

// writeStatusAsText writes out the given check statuses in some semi-arbitrary
// bespoke text format that we copied from Kubernetes.  unknownExcludes lists
// any checks that the user requested to have excluded, but weren't actually
// known checks.  writeStatusAsText is always verbose on failure, and can be
// forced to be verbose on success using the given argument.
func writeStatusesAsText(resp http.ResponseWriter, parts []checkStatus, unknownExcludes sets.String, failed, forceVerbose bool) {
	resp.Header().Set("Content-Type", "text/plain; charset=utf-8")
	resp.Header().Set("X-Content-Type-Options", "nosniff")

	// always write status code first
	if failed {
		resp.WriteHeader(http.StatusInternalServerError)
	} else {
		resp.WriteHeader(http.StatusOK)
	}

	// shortcut for easy non-verbose success
	if !failed && !forceVerbose {
		fmt.Fprint(resp, "ok")
		return
	}

	// we're always verbose on failure, so from this point on we're guaranteed to be verbose

	for _, checkOut := range parts {
		switch {
		case checkOut.excluded:
			fmt.Fprintf(resp, "[+]%s excluded: ok\n", checkOut.name)
		case checkOut.healthy:
			fmt.Fprintf(resp, "[+]%s ok\n", checkOut.name)
		default:
			// don't include the error since this endpoint is public.  If someone wants more detail
			// they should have explicit permission to the detailed checks.
			fmt.Fprintf(resp, "[-]%s failed: reason withheld\n", checkOut.name)
		}
	}

	if unknownExcludes.Len() > 0 {
		fmt.Fprintf(resp, "warn: some health checks cannot be excluded: no matches for %s\n", formatQuoted(unknownExcludes.List()...))
	}

	if failed {
		log.Info("healthz check failed", "statuses", parts)
		fmt.Fprintf(resp, "healthz check failed\n")
	} else {
		fmt.Fprint(resp, "healthz check passed\n")
	}
}

func (h *Handler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	// clean up the request (duplicating the internal logic of http.ServeMux a bit)
	// clean up the path a bit
	reqPath := req.URL.Path
	if reqPath == "" || reqPath[0] != '/' {
		reqPath = "/" + reqPath
	}
	// path.Clean removes the trailing slash except for root for us
	// (which is fine, since we're only serving one layer of sub-paths)
	reqPath = path.Clean(reqPath)

	// either serve the root endpoint...
	if reqPath == "/" {
		h.serveAggregated(resp, req)
		return
	}

	// ...the default check (if nothing else is present)...
	if len(h.Checks) == 0 && reqPath[1:] == "ping" {
		CheckHandler{Checker: Ping}.ServeHTTP(resp, req)
		return
	}

	// ...or an individual checker
	checkName := reqPath[1:] // ignore the leading slash
	checker, known := h.Checks[checkName]
	if !known {
		http.NotFoundHandler().ServeHTTP(resp, req)
		return
	}

	CheckHandler{Checker: checker}.ServeHTTP(resp, req)
}

// CheckHandler is an http.Handler that serves a health check endpoint at the root path,
// based on its checker.
type CheckHandler struct {
	Checker
}

func (h CheckHandler) ServeHTTP(resp http.ResponseWriter, req *http.Request) {
	if err := h.Checker(req); err != nil {
		http.Error(resp, fmt.Sprintf("internal server error: %v", err), http.StatusInternalServerError)
	} else {
		fmt.Fprint(resp, "ok")
	}
}

// Checker knows how to perform a health check.
type Checker func(req *http.Request) error

// Ping returns true automatically when checked.
var Ping Checker = func(_ *http.Request) error { return nil }

// getExcludedChecks extracts the health check names to be excluded from the query param.
func getExcludedChecks(r *http.Request) sets.String {
	checks, found := r.URL.Query()["exclude"]
	if found {
		return sets.NewString(checks...)
	}
	return sets.NewString()
}

// formatQuoted returns a formatted string of the health check names,
// preserving the order passed in.
func formatQuoted(names ...string) string {
	quoted := make([]string, 0, len(names))
	for _, name := range names {
		quoted = append(quoted, fmt.Sprintf("%q", name))
	}
	return strings.Join(quoted, ",")
}
