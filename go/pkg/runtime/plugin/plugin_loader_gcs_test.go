package plugin

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ray-project/ray/go/pkg/gcs"
)

// pluginPaths reads the test plugin .so paths injected via $(location) args.
// rules_go runs tests from the package dir inside the runfiles tree, so the
// injected paths are resolved against the runfiles root (TEST_SRCDIR/TEST_WORKSPACE).
func pluginPaths(t *testing.T) (good, bad, incomplete string) {
	t.Helper()
	if len(os.Args) < 4 {
		t.Skip("test plugin paths not injected via args (not running under bazel)")
	}
	root := os.Getenv("TEST_SRCDIR")
	workspace := os.Getenv("TEST_WORKSPACE")
	if root == "" || workspace == "" {
		t.Fatal("TEST_SRCDIR or TEST_WORKSPACE is empty")
	}
	abs := func(p string) string {
		a, err := filepath.Abs(filepath.Join(root, workspace, p))
		if err != nil {
			t.Fatalf("abs %q: %v", p, err)
		}
		return a
	}
	return abs(os.Args[1]), abs(os.Args[2]), abs(os.Args[3])
}

// setPluginPath points FindPluginPath at the given .so via RAY_GO_RUNTIME_PATH.
func setPluginPath(t *testing.T, path string) {
	t.Helper()
	orig, had := os.LookupEnv("RAY_GO_RUNTIME_PATH")
	if err := os.Setenv("RAY_GO_RUNTIME_PATH", path); err != nil {
		t.Fatalf("setenv: %v", err)
	}
	t.Cleanup(func() {
		if had {
			_ = os.Setenv("RAY_GO_RUNTIME_PATH", orig)
		} else {
			_ = os.Unsetenv("RAY_GO_RUNTIME_PATH")
		}
	})
}

func TestLoadGCSClient(t *testing.T) {
	good, _, _ := pluginPaths(t)
	setPluginPath(t, good)

	client, err := LoadGCSClient(gcs.ClientOptions{Address: "127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("LoadGCSClient() error = %v", err)
	}
	if client == nil {
		t.Fatal("LoadGCSClient() = nil client")
	}
	if got := client.Address(); got != "127.0.0.1:6379" {
		t.Fatalf("client.Address() = %q, want 127.0.0.1:6379", got)
	}
}

func TestLoadGCSClientBadSignature(t *testing.T) {
	_, bad, _ := pluginPaths(t)
	setPluginPath(t, bad)

	_, err := LoadGCSClient(gcs.ClientOptions{Address: "127.0.0.1:6379"})
	if err == nil {
		t.Fatal("LoadGCSClient() = nil error, want error for bad ConnectClient signature")
	}
	if !strings.Contains(err.Error(), "must return") {
		t.Fatalf("error %q does not mention return count", err)
	}
}

func TestLoadGCSClientMissingPlugin(t *testing.T) {
	setPluginPath(t, "/nonexistent/go_runtime.so")
	if _, err := LoadGCSClient(gcs.ClientOptions{}); err == nil {
		t.Fatal("LoadGCSClient() = nil error, want error when plugin is missing")
	}
}

func TestLoadLogBatchPublisher(t *testing.T) {
	good, _, _ := pluginPaths(t)
	setPluginPath(t, good)

	pub, err := LoadLogBatchPublisher(&fakeClient{addr: "127.0.0.1:6379"})
	if err != nil {
		t.Fatalf("LoadLogBatchPublisher() error = %v", err)
	}
	if pub == nil {
		t.Fatal("LoadLogBatchPublisher() = nil publisher")
	}
}

func TestLoadLogBatchPublisherMissingSymbol(t *testing.T) {
	_, bad, _ := pluginPaths(t)
	setPluginPath(t, bad)

	_, err := LoadLogBatchPublisher(&fakeClient{})
	if err == nil {
		t.Fatal("LoadLogBatchPublisher() = nil error, want error for missing NewLogBatchPublisher symbol")
	}
	if !strings.Contains(err.Error(), "NewLogBatchPublisher") {
		t.Fatalf("error %q does not mention the missing symbol", err)
	}
}

func TestValidatePluginLoadable(t *testing.T) {
	good, _, incomplete := pluginPaths(t)

	t.Run("success", func(t *testing.T) {
		setPluginPath(t, good)
		if err := ValidatePluginLoadable(); err != nil {
			t.Fatalf("ValidatePluginLoadable() error = %v", err)
		}
	})
	t.Run("missing worker and publisher symbols", func(t *testing.T) {
		setPluginPath(t, incomplete)
		err := ValidatePluginLoadable()
		if err == nil {
			t.Fatal("ValidatePluginLoadable() = nil error, want error for missing Shutdown")
		}
		if !strings.Contains(err.Error(), "Shutdown") {
			t.Fatalf("error %q does not mention the first missing symbol Shutdown", err)
		}
	})
	t.Run("missing publisher symbol with driver symbols present", func(t *testing.T) {
		_, bad, _ := pluginPaths(t)
		setPluginPath(t, bad)
		err := ValidatePluginLoadable()
		if err == nil {
			t.Fatal("ValidatePluginLoadable() = nil error, want error for missing Shutdown on a plugin that exports only Initialize/ConnectClient")
		}
		if !strings.Contains(err.Error(), "Shutdown") {
			t.Fatalf("error %q does not mention Shutdown", err)
		}
	})
	t.Run("missing plugin file", func(t *testing.T) {
		setPluginPath(t, "/nonexistent/go_runtime.so")
		if err := ValidatePluginLoadable(); err == nil {
			t.Fatal("ValidatePluginLoadable() = nil error, want error for missing plugin")
		}
	})
}

func TestResolvePluginPathRetriesAfterFailure(t *testing.T) {
	t.Setenv("RAY_GO_RUNTIME_PATH", "")
	orig := findPluginPathFn
	t.Cleanup(func() { findPluginPathFn = orig })

	calls := 0
	findPluginPathFn = func() (string, error) {
		calls++
		if calls == 1 {
			return "", fmt.Errorf("transient discovery failure")
		}
		return "/fake/go_runtime.so", nil
	}
	if _, err := resolvePluginPath(); err == nil {
		t.Fatal("first call should fail")
	}
	path, err := resolvePluginPath()
	if err != nil {
		t.Fatalf("second call should succeed after transient failure, got %v", err)
	}
	if path != "/fake/go_runtime.so" {
		t.Fatalf("path = %q, want /fake/go_runtime.so", path)
	}
	// Success stays cached: a later discovery failure must not evict it.
	findPluginPathFn = func() (string, error) { return "", fmt.Errorf("later failure") }
	path, err = resolvePluginPath()
	if err != nil || path != "/fake/go_runtime.so" {
		t.Fatalf("cached path evicted: path=%q err=%v", path, err)
	}
}

func TestLoadLogBatchPublisherNilClient(t *testing.T) {
	good, _, _ := pluginPaths(t)
	setPluginPath(t, good)
	_, err := LoadLogBatchPublisher(nil)
	if err == nil {
		t.Fatal("LoadLogBatchPublisher(nil) = nil error, want explicit nil-client error")
	}
	if !strings.Contains(err.Error(), "nil") {
		t.Fatalf("error %q should state the nil client", err)
	}
}

// fakeClient satisfies gcs.Client by embedding the interface.
type fakeClient struct {
	gcs.Client
	addr string
}

func (f *fakeClient) Address() string { return f.addr }

func (f *fakeClient) Close() error { return nil }

func (f *fakeClient) IsClosed() bool { return false }

var _ gcs.Client = (*fakeClient)(nil)
