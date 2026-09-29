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

package head

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"runtime/debug"
	"strconv"
	"strings"
	"time"

	"github.com/go-logr/logr"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/pkg/version"
)

// dashboardClientMaxSize is the maximum request body size, aligned with
// ray_constants.DASHBOARD_CLIENT_MAX_SIZE (100 MiB). working_dir uploads for
// job submission can be up to 100MiB.
const dashboardClientMaxSize = 100 * 1024 * 1024

// envVarStaticDir overrides the default frontend static build directory. The
// Python dashboard resolves it relative to the dashboard package; Go uses this
// env var so the head process can point at the wheel/source checkout.
const envVarStaticDir = "RAY_DASHBOARD_GO_STATIC_DIR"

// envVarFollowSymlinks enables following symlinks under /static when set to
// "1", aligned with RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS in
// python/ray/dashboard/http_server_head.py (FOLLOW_SYMLINKS_ENABLED).
const envVarFollowSymlinks = "RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS"

// HeadConfig is the configuration for the dashboard head process.
type HeadConfig struct {
	HTTPHost          string
	HTTPPort          int
	HTTPPortRetries   int
	MetricsExportPort int
	GCSAddress        string
	ClusterIDHex      string
	NodeIPAddress     string
	SessionDir        string
	SessionName       string
	Minimal           bool
	ServeFrontend     bool
	LogDir            string
	TempDir           string
	StaticDir         string
	// CodeSearchPath is the cross-language code_search_path so Python workers
	// load wrapper actor classes locally instead of from GCS (Go cannot
	// export_actor_class). It is read from RAY_DASHBOARD_CODE_SEARCH_PATH,
	// mirroring the Python dashboard's SubprocessModuleConfig.code_search_path.
	CodeSearchPath []string
	// Logging options aligned with the Python dashboard.py argparse flags.
	// LoggingLevel/Format/Filename mirror the logging setup; rotate bytes and
	// backup count are accepted for CLI compatibility (the Go logger does not
	// rotate).
	LoggingLevel        string
	LoggingFormat       string
	LoggingFilename     string
	LoggingRotateBytes  int
	LoggingRotateBackup int
	// ModulesToLoad is the set of module names to load; nil/empty loads all
	// modules, aligned with the Python modules_to_load=None default.
	ModulesToLoad []string
	// StdoutFilepath / StderrFilepath redirect the head's stdout/stderr to
	// files, aligned with the Python logging_utils.redirect_stdout_stderr_if_needed.
	StdoutFilepath string
	StderrFilepath string
}

// DefaultHeadConfig returns the default configuration with the Python-compatible
// ports: HTTP 8265 and metrics export 44227 (aligned with
// dashboard_consts.DASHBOARD_METRIC_PORT). The metrics port is overridable via
// the DASHBOARD_METRIC_PORT env var, matching the Python env_integer default.
func DefaultHeadConfig() *HeadConfig {
	return &HeadConfig{
		HTTPHost:          "127.0.0.1",
		HTTPPort:          8265,
		HTTPPortRetries:   0,
		MetricsExportPort: envIntOrDefault("DASHBOARD_METRIC_PORT", 44227),
		// Serve the HTTP dashboard by default, aligned with the Python
		// dashboard (which only disables it with --disable-frontend).
		ServeFrontend: true,
	}
}

// envIntOrDefault reads an integer environment variable, falling back to def
// when unset or unparsable (aligned with Python's env_integer).
func envIntOrDefault(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

// HeadModule is the interface every dashboard head module implements. Modules
// register their HTTP routes on the shared mux before the middleware chain is
// applied.
type HeadModule interface {
	Name() string
	Start(ctx context.Context) error
	RegisterHTTP(mux *http.ServeMux) error
	Healthy() bool
}

// HTTPServer wraps the head's HTTP server and its middleware chain.
type HTTPServer struct {
	cfg     *HeadConfig
	mods    []HeadModule
	metrics *MetricsRegistry
	server  *http.Server
}

// NewHTTPServer builds the HTTP server: it registers every module's routes
// plus the built-in dashboard routes, then wraps the mux in the 5-layer
// middleware chain aligned with the Python aiohttp middleware order.
func NewHTTPServer(cfg *HeadConfig, mods []HeadModule, metrics *MetricsRegistry) (*HTTPServer, error) {
	// Normalize "localhost" to 127.0.0.1 so the listener binds to the loopback
	// address (aligned with dashboard/head.py: http_host = "127.0.0.1" if
	// http_host == "localhost"). This runs before head.go publishes the
	// dashboard address, so the published host is consistent.
	if cfg.HTTPHost == "localhost" {
		cfg.HTTPHost = "127.0.0.1"
	}
	mux := http.NewServeMux()
	for _, m := range mods {
		if err := m.RegisterHTTP(mux); err != nil {
			return nil, fmt.Errorf("register %s routes: %w", m.Name(), err)
		}
	}
	// Built-in routes aligned with http_server_head.py. The method prefixes
	// make non-matching methods return 405 with an Allow header, aligned with
	// aiohttp; /static/ is GET-only so it does not conflict with "GET /" (Go
	// 1.22 ServeMux rejects a methodless pattern that overlaps a method-qualified
	// one).
	mux.HandleFunc("GET /", indexHandler(cfg))
	mux.HandleFunc("GET /favicon.ico", faviconHandler(cfg))
	mux.HandleFunc("GET /static/", staticHandler(cfg))
	mux.HandleFunc("GET /timezone", timezoneHandler)
	mux.HandleFunc("GET /api/authentication_mode", authenticationModeHandler)
	mux.HandleFunc("POST /api/authenticate", authenticateHandler)
	// aiohttp returns 405 for a non-POST request to the POST-only authenticate
	// route; without this explicit GET pattern it would fall through to the
	// "GET /" catch-all.
	mux.HandleFunc("GET /api/authenticate", methodNotAllowedHandler)

	// Middleware chain (aligned with the Python order plus an access log
	// mirroring the aiohttp access_log). The withMaxBodySize layer sits first
	// so every request body is capped at DASHBOARD_CLIENT_MAX_SIZE.
	var h http.Handler = mux
	h = withMethodNotAllowedText(h)
	h = withExactSubtreeCheck(h)
	h = withMaxBodySize(h)
	h = withMetrics(cfg, metrics, h)
	h = withTokenAuth(PublicExactPaths, PublicPathPrefixes, h)
	h = withPathClean(h)
	h = withBrowserPostPutBlock(BrowserPostPutAllowedPaths, h)
	h = withStaticCache(h)
	h = withRecover(log.WithName("dashboard_head"), h)
	h = withAccessLog(h)
	// Outermost layer: strip the nosniff header http.Error adds so HTTP error
	// responses match aiohttp (which never sends X-Content-Type-Options).
	h = withStripNosniff(h)
	if os.Getenv("RAY_DASHBOARD_DEV") == "1" {
		h = withCORS(h)
	}

	return &HTTPServer{
		cfg:     cfg,
		mods:    mods,
		metrics: metrics,
		server: &http.Server{
			Addr:    fmt.Sprintf("%s:%d", cfg.HTTPHost, cfg.HTTPPort),
			Handler: h,
		},
	}, nil
}

// Run starts the HTTP listener, retrying on a new port when the configured
// port is taken (aligned with http_port_retries), and serves until ctx is
// cancelled.
func (s *HTTPServer) Run(ctx context.Context) error {
	ln, err := net.Listen("tcp", s.server.Addr)
	// The initial net.Listen above is attempt #1; the loop performs the
	// remaining HTTPPortRetries attempts, for 1+retries total — matching
	// Python's for i in range(1 + retries). With retries=0 a taken port is a
	// hard failure, as the Python head does.
	for i := 0; err != nil && i < s.cfg.HTTPPortRetries; i++ {
		s.cfg.HTTPPort++
		s.server.Addr = fmt.Sprintf("%s:%d", s.cfg.HTTPHost, s.cfg.HTTPPort)
		ln, err = net.Listen("tcp", s.server.Addr)
	}
	if err != nil {
		return fmt.Errorf("failed to find valid port after %d retries: %w", s.cfg.HTTPPortRetries, err)
	}
	go func() {
		<-ctx.Done()
		_ = s.server.Shutdown(context.Background())
	}()
	if err := s.server.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// Address returns the actual bound host:port.
func (s *HTTPServer) Address() (string, int) { return s.cfg.HTTPHost, s.cfg.HTTPPort }

// resolveStaticDir returns the directory of the built frontend. It prefers an
// explicit StaticDir, then the RAY_DASHBOARD_GO_STATIC_DIR env var, then the
// source checkout derived from the raygo executable location (the binary lives
// at <root>/python/ray/go/cmd/raygo, so the frontend is
// <root>/python/ray/dashboard/client/build, mirroring the Python dashboard's
// os.path.dirname(__file__)/client/build), and finally the source checkout
// relative to the working directory.
func resolveStaticDir(cfg *HeadConfig) string {
	if cfg.StaticDir != "" {
		return cfg.StaticDir
	}
	if dir := os.Getenv(envVarStaticDir); dir != "" {
		return dir
	}
	if exe, err := os.Executable(); err == nil {
		cmdDir := filepath.Dir(exe)
		if filepath.Base(cmdDir) == "cmd" {
			root := filepath.Dir(filepath.Dir(filepath.Dir(cmdDir)))
			if dir := filepath.Join(root, "ray", "dashboard", "client", "build"); dirExists(dir) {
				return dir
			}
		}
	}
	return filepath.Join("python", "ray", "dashboard", "client", "build")
}

// dirExists reports whether path exists and is a directory.
func dirExists(path string) bool {
	fi, err := os.Stat(path)
	return err == nil && fi.IsDir()
}

// indexHandler serves the frontend index.html for "/" and returns 404 for
// other paths so module routes are not shadowed. The response is marked
// no-store so a logged-in dashboard session is not served from cache (aligned
// with http_server_head.get_index).
func indexHandler(cfg *HeadConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			notFound(w, r)
			return
		}
		w.Header().Set("Cache-Control", "no-store")
		http.ServeFile(w, r, filepath.Join(resolveStaticDir(cfg), "index.html"))
	}
}

// methodNotAllowedHandler returns 405, used for the GET /api/authenticate
// pattern so a non-POST request to the POST-only authenticate route matches
// aiohttp's 405 instead of falling through to the "GET /" catch-all. The body
// is "405: Method Not Allowed", aligned with aiohttp's HTTPException text
// format.
func methodNotAllowedHandler(w http.ResponseWriter, r *http.Request) {
	// The POST-only authenticate route advertises POST, matching the Allow
	// header aiohttp's HTTPMethodNotAllowed would emit for it.
	w.Header().Set("Allow", "POST")
	http.Error(w, "405: Method Not Allowed", http.StatusMethodNotAllowed)
}

// notFound writes a 404 whose body matches aiohttp's "404: Not Found" text
// instead of Go's default "404 page not found".
func notFound(w http.ResponseWriter, r *http.Request) {
	http.Error(w, "404: Not Found", http.StatusNotFound)
}

// faviconHandler serves the dashboard favicon.
func faviconHandler(cfg *HeadConfig) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, filepath.Join(resolveStaticDir(cfg), "favicon.ico"))
	}
}

// staticHandler serves /static/ assets with a stripped prefix. Directory
// requests return 403, aligned with the Python routes.static("/static", ...)
// which forbids directory listing (http.FileServer would return an HTML
// directory index instead). Symlinked paths are rejected with 404 unless
// RAY_DASHBOARD_BUILD_FOLLOW_SYMLINKS=1, aligned with the Python
// routes.static(follow_symlinks=FOLLOW_SYMLINKS_ENABLED) which defaults to
// not following symlinks (aiohttp resolves the path and raises HTTPNotFound
// when it escapes the static root).
func staticHandler(cfg *HeadConfig) http.HandlerFunc {
	root := filepath.Join(resolveStaticDir(cfg), "static")
	fs := http.FileServer(http.Dir(root))
	// Resolved once at setup, mirroring the module-level FOLLOW_SYMLINKS_ENABLED
	// in http_server_head.py (os.environ.get(...) == "1").
	followSymlinks := os.Getenv(envVarFollowSymlinks) == "1"
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// FileServer sees the path with the /static/ prefix stripped, so the
		// stat check mirrors that. withPathClean has already rejected traversal
		// attempts, so joining the stripped path cannot escape root.
		name := filepath.Join(root, strings.TrimPrefix(r.URL.Path, "/static/"))
		if !followSymlinks && staticPathHasSymlink(root, strings.TrimPrefix(r.URL.Path, "/static/")) {
			// The target (or one of its path segments) is a symlink: reject
			// with 404, matching aiohttp's HTTPNotFound for a path that
			// resolves outside the static root when symlinks are disabled.
			http.Error(w, "404: Not Found", http.StatusNotFound)
			return
		}
		if fi, err := os.Stat(name); err == nil && fi.IsDir() {
			// A path resolving to a directory (including the subtree root "/")
			// is rejected before FileServer renders the directory listing.
			http.Error(w, "403: Forbidden", http.StatusForbidden)
			return
		}
		http.StripPrefix("/static/", fs).ServeHTTP(w, r)
	})
}

// staticPathHasSymlink reports whether any path segment of rel under root is a
// symbolic link, checked with os.Lstat (which does not follow links). The root
// itself is the deployment directory and is not checked, matching aiohttp which
// resolves the request filename against the already-resolved static directory.
func staticPathHasSymlink(root, rel string) bool {
	p := root
	for _, seg := range strings.Split(rel, "/") {
		if seg == "" {
			continue
		}
		p = filepath.Join(p, seg)
		if fi, err := os.Lstat(p); err == nil && fi.Mode()&os.ModeSymlink != 0 {
			return true
		}
	}
	return false
}

// timezones mirrors the 33-entry timezone table in
// python/ray/dashboard/timezone_utils.py. It maps a current UTC offset to the
// representative zone name returned to the frontend.
var timezones = []struct {
	offset string
	value  string
}{
	{"-12:00", "Etc/+12"},
	{"-11:00", "Pacific/Pago_Pago"},
	{"-10:00", "Pacific/Honolulu"},
	{"-09:00", "America/Anchorage"},
	{"-08:00", "America/Los_Angeles"},
	{"-07:00", "America/Phoenix"},
	{"-06:00", "America/Guatemala"},
	{"-05:00", "America/Bogota"},
	{"-04:00", "America/Halifax"},
	{"-03:30", "America/St_Johns"},
	{"-03:00", "America/Sao_Paulo"},
	{"-02:00", "America/Godthab"},
	{"-01:00", "Atlantic/Azores"},
	{"+00:00", "Europe/London"},
	{"+01:00", "Europe/Amsterdam"},
	{"+02:00", "Asia/Amman"},
	{"+03:00", "Asia/Baghdad"},
	{"+03:30", "Asia/Tehran"},
	{"+04:00", "Asia/Dubai"},
	{"+04:30", "Asia/Kabul"},
	{"+05:00", "Asia/Karachi"},
	{"+05:30", "Asia/Kolkata"},
	{"+05:45", "Asia/Kathmandu"},
	{"+06:00", "Asia/Almaty"},
	{"+06:30", "Asia/Yangon"},
	{"+07:00", "Asia/Bangkok"},
	{"+08:00", "Asia/Shanghai"},
	{"+09:00", "Asia/Irkutsk"},
	{"+09:30", "Australia/Adelaide"},
	{"+10:00", "Australia/Brisbane"},
	{"+11:00", "Asia/Magadan"},
	{"+12:00", "Pacific/Auckland"},
	{"+13:00", "Pacific/Tongatapu"},
}

// timezoneHandler returns the current timezone offset and the mapped zone
// value (aligned with timezone_utils.get_current_timezone_info). The offset
// sign and magnitude use abs(minutes), matching the Python divmod handling for
// negative offsets such as -03:30.
func timezoneHandler(w http.ResponseWriter, r *http.Request) {
	_, offsetSec := time.Now().Zone()
	hours := offsetSec / 3600
	minutes := (offsetSec % 3600) / 60
	sign := "+"
	if hours < 0 {
		sign = "-"
		hours = -hours
	}
	if minutes < 0 {
		minutes = -minutes
	}
	offset := fmt.Sprintf("%s%02d:%02d", sign, hours, minutes)
	value := interface{}(nil)
	for _, tz := range timezones {
		if tz.offset == offset {
			value = tz.value
			break
		}
	}
	writeJSON(w, map[string]interface{}{
		"offset": offset,
		"value":  value,
	})
}

// authenticationModeHandler reports the current authentication mode. When
// authentication is disabled it also clears any existing authentication
// cookie, aligned with http_server_head.get_authentication_mode.
func authenticationModeHandler(w http.ResponseWriter, r *http.Request) {
	mode := getAuthenticationModeName()
	if mode == "disabled" {
		http.SetCookie(w, &http.Cookie{
			Name:     authenticationTokenCookie,
			Value:    "",
			MaxAge:   0,
			Path:     "/",
			HttpOnly: true,
		})
	}
	writeJSON(w, map[string]interface{}{
		"authentication_mode": mode,
	})
}

// authenticateHandler validates a bearer token, sets an HttpOnly session
// cookie and returns a simple JSON status. Aligned with the Python
// authenticate handler in http_server_head.py: on success it strips the
// "Bearer " prefix and stores the token in the authentication cookie so
// subsequent dashboard requests authenticate through the cookie branch of
// withTokenAuth.
func authenticateHandler(w http.ResponseWriter, r *http.Request) {
	if !tokenAuthEnabled() {
		http.Error(w, "Unauthorized: Token authentication is not enabled", http.StatusUnauthorized)
		return
	}
	authHeader := r.Header.Get(authorizationHeaderName)
	if authHeader == "" {
		http.Error(w, "Unauthorized: Missing authentication token", http.StatusUnauthorized)
		return
	}
	if !validateRequestToken(authHeader) {
		http.Error(w, "Forbidden: Invalid authentication token", http.StatusForbidden)
		return
	}
	token := authHeader
	if len(token) >= len(bearerPrefix) && token[:len(bearerPrefix)] == bearerPrefix {
		token = token[len(bearerPrefix):]
	}
	http.SetCookie(w, &http.Cookie{
		Name:     authenticationTokenCookie,
		Value:    token,
		MaxAge:   authenticationTokenCookieMaxAge,
		Path:     "/",
		HttpOnly: true,
		SameSite: http.SameSiteStrictMode,
		Secure:   r.TLS != nil,
	})
	writeJSON(w, map[string]interface{}{
		"status":  "authenticated",
		"message": "Token is valid",
	})
}

// withRecover catches handler panics, logs them and returns 500 so one
// failing module cannot take down the whole head process. The body matches
// aiohttp's handle_error for an unhandled exception
// ("500 Internal Server Error\n\nAn unexpected error has occurred.") and is
// written manually to avoid http.Error's trailing newline and nosniff header.
func withRecover(logger logr.Logger, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if rec := recover(); rec != nil {
				logger.Error(fmt.Errorf("%v", rec), "panic in handler", "path", r.URL.Path, "stack", string(debug.Stack()))
				w.Header().Set("Content-Type", "text/plain; charset=utf-8")
				w.WriteHeader(http.StatusInternalServerError)
				_, _ = w.Write([]byte("500 Internal Server Error\n\nAn unexpected error has occurred."))
			}
		}()
		next.ServeHTTP(w, r)
	})
}

// exactSubtreeRoutes lists every "trailing-slash subtree" pattern registered by
// the modules, paired with the set of paths the subtree legitimately contains
// and the set of methods each of those paths accepts. Go's ServeMux treats a
// trailing-slash pattern ("GET /api/jobs/") as a catch-all for the whole
// subtree, so "GET /api/jobs/abc/yyy" falls through to the list handler and
// "GET /api/jobs/abc/stop" dispatches to whatever method-first pattern matches.
// Python's aiohttp router matches paths exactly: an unregistered deeper path is
// 404 and a method mismatch on a registered path is 405. This middleware
// reproduces that by rejecting requests whose path is inside a known subtree but
// matches no registered sub-path (404) or uses a method the registered sub-path
// does not accept (405). Paths the subtree legitimately contains are passed
// through untouched so the mux dispatches them normally. Keep this table in sync
// with the modules' RegisterHTTP registrations.
var exactSubtreeRoutes = []struct {
	prefix string
	// exact maps a path inside the subtree (suffix after the prefix) to the set
	// of methods it is registered with. An empty string key matches the subtree
	// root itself (the list/create route). A nil entry means no further sub-path
	// exists: any deeper request is 404.
	exact map[string][]string
}{
	{
		prefix: "/api/jobs/",
		exact: map[string][]string{
			"":               {"POST", "GET"},
			"{id}":           {"GET", "DELETE"},
			"{id}/stop":      {"POST"},
			"{id}/logs":      {"GET"},
			"{id}/logs/tail": {"GET"},
		},
	},
	{
		prefix: "/api/flow/jobs/",
		exact: map[string][]string{
			"":               {"POST", "GET"},
			"{id}":           {"GET", "DELETE"},
			"{id}/stop":      {"POST"},
			"{id}/logs":      {"GET"},
			"{id}/logs/tail": {"GET"},
		},
	},
	{
		prefix: "/api/flow/plugins/",
		exact: map[string][]string{
			"":     {"POST", "GET"},
			"{id}": {"GET", "DELETE"},
		},
	},
	{
		prefix: "/api/serve/applications/",
		exact: map[string][]string{
			"": {"GET", "PUT", "DELETE"},
		},
	},
}

// withExactSubtreeCheck rejects requests that fall through Go ServeMux trailing-
// slash subtree patterns into a handler they should not reach, aligning the
// status codes with the Python aiohttp router (unregistered deeper path -> 404,
// method mismatch on a registered path -> 405). Requests not inside a known
// subtree, or matching a registered sub-path, pass through unchanged.
func withExactSubtreeCheck(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		path := r.URL.Path
		for _, sub := range exactSubtreeRoutes {
			if !strings.HasPrefix(path, sub.prefix) {
				continue
			}
			suffix := strings.TrimPrefix(path, sub.prefix)
			if suffix == "" {
				// The subtree root is always registered; the mux dispatches it.
				next.ServeHTTP(w, r)
				return
			}
			segs := strings.Split(suffix, "/")
			var allowed []string
			switch len(segs) {
			case 1:
				allowed = sub.exact["{id}"]
			case 2:
				if segs[1] == "stop" {
					allowed = sub.exact["{id}/stop"]
				} else if segs[1] == "logs" {
					allowed = sub.exact["{id}/logs"]
				}
			case 3:
				if segs[1] == "logs" && segs[2] == "tail" {
					allowed = sub.exact["{id}/logs/tail"]
				}
			}
			if allowed == nil {
				// A deeper path that no module route matches: 404, aligned with
				// the aiohttp router for an unregistered path.
				notFound(w, r)
				return
			}
			for _, m := range allowed {
				if m == r.Method {
					next.ServeHTTP(w, r)
					return
				}
			}
			// The path is registered but not for this method: 405 with an Allow
			// header, aligned with aiohttp's method not allowed behavior (the
			// header is comma-separated without spaces).
			w.Header().Set("Allow", strings.Join(allowed, ","))
			http.Error(w, "405: Method Not Allowed", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// withMaxBodySize caps the request body at dashboardClientMaxSize, aligned with
// the aiohttp Application(client_max_size=DASHBOARD_CLIENT_MAX_SIZE). The body
// is wrapped in a MaxBytesReader so reading a body over the limit errors with
// 413 Request Entity Too Large, matching aiohttp which only enforces the limit
// when the handler reads the body (there is no fail-fast on the declared
// Content-Length).
func withMaxBodySize(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Body != nil {
			r.Body = http.MaxBytesReader(w, r.Body, dashboardClientMaxSize)
		}
		next.ServeHTTP(w, r)
	})
}

// statusRecorder captures the response status code for the access log.
type statusRecorder struct {
	http.ResponseWriter
	status   int
	bytes    int64
	hijacked bool
}

// methodNotAllowedRecorder wraps a ResponseWriter and rewrites 405 responses so
// the body and Allow header match aiohttp's HTTPMethodNotAllowed. Go ServeMux
// and http.Error write the standard "Method Not Allowed" text and a
// ", "-separated Allow list; aiohttp writes "405: Method Not Allowed" and a
// comma-separated Allow header (web_exceptions.HTTPMethodNotAllowed joins with
// ","). WriteHeader records the status and normalizes the Allow header;
// Write replaces the first body write with the aiohttp text so the 405 body is
// identical regardless of which handler produced it.
type methodNotAllowedRecorder struct {
	http.ResponseWriter
	status   int
	hijacked bool
	bodySent bool
}

func (r *methodNotAllowedRecorder) WriteHeader(code int) {
	r.status = code
	if r.hijacked {
		// The connection has been taken over by a WebSocket upgrade; forward
		// without touching the response so the upgrade is unaffected.
		r.ResponseWriter.WriteHeader(code)
		return
	}
	if code == http.StatusMethodNotAllowed {
		// Normalize the Allow header to aiohttp's ", "-less comma-separated
		// format ("GET, HEAD" -> "GET,HEAD").
		if allow := r.Header().Get("Allow"); allow != "" {
			r.Header().Set("Allow", strings.ReplaceAll(allow, ", ", ","))
		}
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *methodNotAllowedRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if r.hijacked {
		// Same as statusRecorder: the WebSocket owns the connection after the
		// upgrade, so do not forward writes to the hijacked writer.
		return len(b), nil
	}
	if r.status == http.StatusMethodNotAllowed && !r.bodySent {
		r.bodySent = true
		return r.ResponseWriter.Write([]byte("405: Method Not Allowed"))
	}
	return r.ResponseWriter.Write(b)
}

// Hijack implements http.Hijacker by forwarding to the wrapped
// http.ResponseWriter, so WebSocket upgrades (websocket.Accept in the job/flow
// modules) can take over the connection through this middleware. The status is
// recorded before hijacking and subsequent writes are skipped.
func (r *methodNotAllowedRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if r.status == 0 {
		r.status = http.StatusSwitchingProtocols
	}
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	r.hijacked = true
	return hj.Hijack()
}

// withMethodNotAllowedText wraps the mux so every 405 response body and Allow
// header matches aiohttp. It sits immediately outside the mux (inside
// withExactSubtreeCheck, outside withMaxBodySize) so both the ServeMux method
// mismatch 405 and the withExactSubtreeCheck 405 are normalized.
func withMethodNotAllowedText(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&methodNotAllowedRecorder{ResponseWriter: w}, r)
	})
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	if r.hijacked {
		// The connection has been taken over by a WebSocket upgrade; writing
		// to the hijacked ResponseWriter would emit a superfluous
		// WriteHeader warning from net/http. Skip the call, the status has
		// already been recorded for the access log.
		return
	}
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	if r.hijacked {
		// Same as WriteHeader: the WebSocket owns the connection after the
		// upgrade, so do not forward writes to the hijacked writer.
		return len(b), nil
	}
	n, err := r.ResponseWriter.Write(b)
	r.bytes += int64(n)
	return n, err
}

// Hijack implements http.Hijacker by forwarding to the wrapped
// http.ResponseWriter, so WebSocket upgrades (websocket.Accept in the job/flow
// modules) can take over the connection through the withAccessLog middleware.
// The status is recorded before hijacking so the access log reports 101.
func (r *statusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if r.status == 0 {
		r.status = http.StatusSwitchingProtocols
	}
	hj, ok := r.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	r.hijacked = true
	return hj.Hijack()
}

// nosniffStrippingWriter deletes the X-Content-Type-Options: nosniff header
// that http.Error sets. aiohttp does not emit nosniff for HTTP error responses,
// so the middleware strips it from every response regardless of which handler
// produced it.
type nosniffStrippingWriter struct {
	http.ResponseWriter
}

func (w *nosniffStrippingWriter) WriteHeader(code int) {
	w.Header().Del("X-Content-Type-Options")
	w.ResponseWriter.WriteHeader(code)
}

// Hijack implements http.Hijacker by forwarding to the wrapped
// http.ResponseWriter so WebSocket upgrades still work through this outermost
// middleware.
func (w *nosniffStrippingWriter) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	hj, ok := w.ResponseWriter.(http.Hijacker)
	if !ok {
		return nil, nil, fmt.Errorf("underlying ResponseWriter does not implement http.Hijacker")
	}
	return hj.Hijack()
}

// withStripNosniff removes the X-Content-Type-Options header from every
// response. It is the outermost middleware (outside withAccessLog) so it also
// covers requests rejected upstream; being outside withAccessLog does not
// affect the access log, which reads the status from its own recorder.
func withStripNosniff(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(&nosniffStrippingWriter{ResponseWriter: w}, r)
	})
}

// withAccessLog logs every request (method/path/query/status/bytes/duration/
// client ip/referer/user-agent), mirroring the aiohttp access_log on the
// dashboard head server. It is
// intentionally the outermost middleware so even requests rejected upstream are
// recorded. The Prometheus /metrics export server is a separate listener and is
// not affected.
func withAccessLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if rec.status == 0 {
			rec.status = http.StatusOK
		}
		log.Log.Info("access",
			"method", r.Method,
			"path", r.URL.Path,
			"query", r.URL.RawQuery,
			"status", rec.status,
			"bytes", rec.bytes,
			"duration_us", time.Since(start).Microseconds(),
			"remote", clientIP(r),
			"referer", r.Header.Get("Referer"),
			"user_agent", r.Header.Get("User-Agent"))
	})
}

// clientIP returns the request's remote IP without the port (aiohttp %a).
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

// withPathClean rejects path traversal attempts under /static and /logs
// (aligned with path_clean_middleware). It also maps two ServeMux behaviors to
// the Python aiohttp router:
//   - A request to a trailing-slash subtree root without the trailing slash
//     (e.g. GET /api/jobs) would otherwise get an automatic 301 to the slashed
//     URL. aiohttp returns 404 for the API subtree roots, so this middleware
//     short-circuits those requests before ServeMux redirects them.
//   - The bare /static path returns 403 like any /static directory request
//     (Python routes.static forbids directory listing). This must be
//     intercepted here because ServeMux would 301 /static -> /static/ before
//     staticHandler can reject it; the slashed and deeper directory forms are
//     handled by staticHandler's own directory check.
func withPathClean(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p := r.URL.Path
		if strings.HasPrefix(p, "/static") || strings.HasPrefix(p, "/logs") {
			base := "/static"
			if strings.HasPrefix(p, "/logs") {
				base = "/logs"
			}
			clean := filepath.Clean(p)
			if clean != base && !strings.HasPrefix(clean, base+"/") {
				http.Error(w, "403: Forbidden", http.StatusForbidden)
				return
			}
		}
		// The bare /static path: ServeMux would 301 it to /static/, aiohttp
		// returns 403 (directory listing forbidden).
		if p == "/static" {
			http.Error(w, "403: Forbidden", http.StatusForbidden)
			return
		}
		// Trailing-slash subtree routes without the trailing slash: ServeMux
		// would 301 to the slashed URL, aiohttp returns 404.
		if !strings.HasSuffix(p, "/") {
			for _, sub := range exactSubtreeRoutes {
				if p == strings.TrimSuffix(sub.prefix, "/") {
					notFound(w, r)
					return
				}
			}
		}
		next.ServeHTTP(w, r)
	})
}

// withBrowserPostPutBlock blocks POST/PUT requests from browsers except for
// whitelisted paths and the flow insight endpoints (aligned with
// browsers_no_post_put_middleware).
func withBrowserPostPutBlock(whitelist map[string]bool, next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if whitelist[r.URL.Path] {
			next.ServeHTTP(w, r)
			return
		}
		if r.Method == http.MethodPost && strings.HasPrefix(r.URL.Path, "/insight/") {
			next.ServeHTTP(w, r)
			return
		}
		if (r.Method == http.MethodPost || r.Method == http.MethodPut) && isBrowserRequest(r) {
			http.Error(w, "Method Not Allowed for browser traffic.", http.StatusMethodNotAllowed)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// endpointNames maps a (method, URL pattern) to the Python handler function
// name so the metrics endpoint label matches the Python metrics_middleware
// output (http_server_head.py uses handler.__name__). The outer key is the Go
// ServeMux pattern without a method prefix and the inner map is keyed by HTTP
// method; routes whose handler name differs per method (e.g. job submit/list,
// package get/upload, insight GET/POST proxy) carry one entry per method,
// single-method routes share a single entry. The "/api/v0/logs/{media_type}"
// route is handled by StateHead.get_logs in Python. Unmatched (method, path)
// pairs fall back to the URL path.
var endpointNames = map[string]map[string]string{
	// http_server_head.py
	"/": {
		"GET": "get_index",
	},
	"/favicon.ico": {
		"GET": "get_favicon",
	},
	"/timezone": {
		"GET": "get_timezone",
	},
	"/api/authentication_mode": {
		"GET": "get_authentication_mode",
	},
	"/api/authenticate": {
		"GET":  "authenticate",
		"POST": "authenticate",
	},
	// job_head.py
	"/api/version": {
		"GET": "get_version",
	},
	"/api/packages/{protocol}/{package_name}": {
		"GET": "get_package",
		"PUT": "upload_package",
	},
	"/api/jobs/": {
		"GET":  "list_jobs",
		"POST": "submit_job",
	},
	"/api/jobs/{job_or_submission_id}": {
		"GET":    "get_job_info",
		"DELETE": "delete_job",
	},
	"/api/jobs/{job_or_submission_id}/stop": {
		"POST": "stop_job",
	},
	"/api/jobs/{job_or_submission_id}/logs": {
		"GET": "get_job_logs",
	},
	"/api/jobs/{job_or_submission_id}/logs/tail": {
		"GET": "tail_job_logs",
	},
	"/api/component_activities": {
		"GET": "get_component_activities",
	},
	// flow_head.py (handler names mirror job_head.py for the job routes)
	"/api/flow/jobs/": {
		"GET":  "list_jobs",
		"POST": "submit_job",
	},
	"/api/flow/jobs/{job_or_submission_id}": {
		"GET":    "get_job_info",
		"DELETE": "delete_job",
	},
	"/api/flow/jobs/{job_or_submission_id}/stop": {
		"POST": "stop_job",
	},
	"/api/flow/jobs/{job_or_submission_id}/logs": {
		"GET": "get_job_logs",
	},
	"/api/flow/jobs/{job_or_submission_id}/logs/tail": {
		"GET": "tail_job_logs",
	},
	"/api/flow/plugins/": {
		"GET":  "list_plugins",
		"POST": "add_plugin",
	},
	"/api/flow/plugins/{plugin_id}": {
		"GET":    "get_plugin_info",
		"DELETE": "delete_plugin",
	},
	// serve_head.py
	"/api/ray/version": {
		"GET": "get_version",
	},
	"/api/serve/applications/": {
		"GET":    "get_serve_instance_details",
		"DELETE": "delete_serve_applications",
		"PUT":    "put_all_applications",
	},
	"/api/v1/applications/{application_name}/deployments/{deployment_name}/scale": {
		"POST": "scale_deployment",
	},
	// state_head.py
	"/api/v0/actors": {
		"GET": "list_actors",
	},
	"/api/v0/jobs": {
		"GET": "list_jobs",
	},
	"/api/v0/nodes": {
		"GET": "list_nodes",
	},
	"/api/v0/placement_groups": {
		"GET": "list_placement_groups",
	},
	"/api/v0/workers": {
		"GET": "list_workers",
	},
	"/api/v0/tasks": {
		"GET": "list_tasks",
	},
	"/api/v0/objects": {
		"GET": "list_objects",
	},
	"/api/v0/runtime_envs": {
		"GET": "list_runtime_envs",
	},
	"/api/v0/logs": {
		"GET": "list_logs",
	},
	"/api/v0/logs/{media_type}": {
		"GET": "get_logs",
	},
	"/api/v0/tasks/summarize": {
		"GET": "summarize_tasks",
	},
	"/api/v0/actors/summarize": {
		"GET": "summarize_actors",
	},
	"/api/v0/objects/summarize": {
		"GET": "summarize_objects",
	},
	"/api/v0/tasks/timeline": {
		"GET": "tasks_timeline",
	},
	"/api/v0/delay/{delay_s}": {
		"GET": "delayed_response",
	},
	// node_head.py — the Python handlers are decorated with aiohttp_cache, so
	// the endpoint label carries the "[cache ttl=2, max_size=128]" suffix.
	"/nodes": {
		"GET": "get_all_nodes[cache ttl=2, max_size=128]",
	},
	"/nodes/{node_id}": {
		"GET": "get_node[cache ttl=2, max_size=128]",
	},
	"/logical/actors": {
		"GET": "get_all_actors[cache ttl=2, max_size=128]",
	},
	"/logical/actors/{actor_id}": {
		"GET": "get_actor[cache ttl=2, max_size=128]",
	},
	"/test/dump": {
		"GET": "dump",
	},
	// reporter_head.py
	"/api/v0/cluster_metadata": {
		"GET": "get_cluster_metadata",
	},
	"/api/cluster_status": {
		"GET": "get_cluster_status",
	},
	"/task/traceback": {
		"GET": "get_task_traceback",
	},
	"/task/cpu_profile": {
		"GET": "get_task_cpu_profile",
	},
	"/worker/traceback": {
		"GET": "get_traceback",
	},
	"/worker/cpu_profile": {
		"GET": "cpu_profile",
	},
	"/worker/gpu_profile": {
		"GET": "gpu_profile",
	},
	"/memory_profile": {
		"GET": "memory_profile",
	},
	"/api/gcs_healthz": {
		"GET": "health_check",
	},
	"/api/prometheus/sd": {
		"GET": "prometheus_service_discovery",
	},
	// event_head.py
	"/report_events": {
		"POST": "report_events",
	},
	"/events": {
		"GET": "get_event[cache ttl=2, max_size=128]",
	},
	"/api/v0/cluster_events": {
		"GET": "list_cluster_events",
	},
	// metrics_head.py
	"/api/grafana_health": {
		"GET": "grafana_health",
	},
	"/api/prometheus_health": {
		"GET": "prometheus_health",
	},
	// train_head.py
	"/api/train/v2/runs": {
		"GET": "get_train_runs",
	},
	"/api/train/v2/runs/v1": {
		"GET": "get_train_v2_runs",
	},
	// data_head.py
	"/api/data/datasets/{job_id}": {
		"GET": "get_datasets",
	},
	// insight_head.py
	"/insight/{path...}": {
		"GET":  "proxy_get_request",
		"POST": "proxy_post_request",
	},
	// usage_stats_head.py
	"/usage_stats_enabled": {
		"GET": "get_usage_stats_enabled",
	},
	"/cluster_id": {
		"GET": "get_cluster_id",
	},
}

// endpointNameFor returns the Python handler name for the request method and
// path. It first tries the exact URL path, then falls back to pattern-matching
// the path against the route patterns (for requests with a concrete id in the
// path).
func endpointNameFor(method, path string) string {
	if names, ok := endpointNames[path]; ok {
		if name, ok := names[method]; ok {
			return name
		}
		return path
	}
	// The Go ServeMux routes use {placeholders} in patterns; match the request
	// path against each pattern by splitting into segments.
	for pattern, names := range endpointNames {
		if patternPathMatches(path, pattern) {
			if name, ok := names[method]; ok {
				return name
			}
			return path
		}
	}
	return path
}

// patternPathMatches reports whether path matches the route pattern. Pattern
// segments in {braces} match any single path segment; a trailing {path...}
// wildcard (the /insight proxy route) matches the remainder of the path.
func patternPathMatches(path, pattern string) bool {
	ps := strings.Split(path, "/")
	rs := strings.Split(pattern, "/")
	for i := range rs {
		if strings.HasPrefix(rs[i], "{") && strings.HasSuffix(rs[i], "}") {
			if strings.HasSuffix(rs[i], "...}") {
				// A {path...} wildcard consumes the rest of the path.
				return i >= len(ps) || ps[i] != ""
			}
			if i >= len(ps) {
				return false
			}
			continue
		}
		if i >= len(ps) || ps[i] != rs[i] {
			return false
		}
	}
	return len(ps) == len(rs)
}

// withMetrics records request timing/counts on the dashboard metrics, aligned
// with the Python metrics_middleware (http_server_head.py). It observes the
// ray_dashboard_api_requests_duration_seconds histogram and increments the
// ray_dashboard_api_requests_count_requests_total counter with the request
// method, endpoint (the Python handler name), status-class (e.g. "2xx"),
// Version and SessionName labels. A nil metrics registry degrades to a
// pass-through so unit tests that build a server without a registry keep
// working.
func withMetrics(cfg *HeadConfig, metrics *MetricsRegistry, next http.Handler) http.Handler {
	dm := (*DashboardMetrics)(nil)
	if metrics != nil {
		dm = metrics.DashboardMetrics()
	}
	sessionName := ""
	if cfg != nil {
		sessionName = cfg.SessionName
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		rec := &statusRecorder{ResponseWriter: w}
		next.ServeHTTP(rec, r)
		if dm == nil {
			return
		}
		status := rec.status
		if status == 0 {
			status = http.StatusOK
		}
		statusTag := fmt.Sprintf("%dxx", status/100)
		duration := time.Since(start).Seconds()
		endpoint := endpointNameFor(r.Method, r.URL.Path)
		dm.requestDuration.WithLabelValues(
			endpoint, statusTag, version.RayVersion, sessionName, "dashboard",
		).Observe(duration)
		dm.requestCount.WithLabelValues(
			r.Method, endpoint, statusTag, version.RayVersion, sessionName, "dashboard",
		).Inc()
	})
}

// withCORS adds the Access-Control-Allow-Origin: * header to every response in
// dev mode, aligned with routes.py rest_response which sets it when
// RAY_DASHBOARD_DEV=1 so a dev frontend on a different port can consume the
// API.
func withCORS(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		next.ServeHTTP(w, r)
	})
}

// withStaticCache adds a one-year cache header to /static assets (aligned with
// cache_control_static_middleware).
func withStaticCache(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.HasPrefix(r.URL.Path, "/static") {
			w.Header().Set("Cache-Control", "max-age=31536000")
		}
		next.ServeHTTP(w, r)
	})
}

// isBrowserRequest guesses whether a request came from a browser, aligned
// with dashboard_optional_utils.is_browser_request: the User-Agent starts
// with "Mozilla", any Sec-Fetch-* header is present, or any CORS header
// (Referer/Origin/Access-Control-Request-*) is present.
func isBrowserRequest(r *http.Request) bool {
	ua := r.Header.Get("User-Agent")
	if strings.HasPrefix(ua, "Mozilla") {
		return true
	}
	for _, h := range []string{
		"Referer",
		"Origin",
		"Sec-Fetch-Mode",
		"Sec-Fetch-Dest",
		"Sec-Fetch-Site",
		"Sec-Fetch-User",
		"Access-Control-Request-Method",
		"Access-Control-Request-Headers",
	} {
		if r.Header.Get(h) != "" {
			return true
		}
	}
	return false
}
