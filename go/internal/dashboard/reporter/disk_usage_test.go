package reporter

import (
	"math"
	"os"
	"sort"
	"syscall"
	"testing"

	"github.com/ray-project/ray/go/internal/dashboard/agent"
)

// stubStatfs pins the statfs view diskUsageOf sees. The map is keyed by path;
// an unlisted path reports ENOENT so a caller can assert the error path.
func stubStatfs(t *testing.T, byPath map[string]syscall.Statfs_t) {
	t.Helper()

	orig := statfs
	statfs = func(path string) (syscall.Statfs_t, error) {
		st, ok := byPath[path]
		if !ok {
			return syscall.Statfs_t{}, os.ErrNotExist
		}
		return st, nil
	}
	t.Cleanup(func() { statfs = orig })
}

// stubUserTempDir pins userTempDir so the diskUsage key does not depend on the
// environment the test runs under.
func stubUserTempDir(t *testing.T, dir string) {
	t.Helper()

	orig := userTempDir
	userTempDir = func() string { return dir }
	t.Cleanup(func() { userTempDir = orig })
}

// TestDiskPercentUsesNonRootPool pins the psutil disk_usage percent formula.
//
// psutil derives `used` from f_bfree but `free` from f_bavail, so the
// root-reserved blocks sit outside both, and percent is used/(used+free). Using
// total as the denominator instead counts the reserve and understates usage —
// on an ext4 /home with a 5% reserve that is a multi-point error in the payload
// the head node renders.
//
// Layout: 2000 blocks total, 500 of them free to root (f_bfree), but only 400
// free to a non-root process (f_bavail) after a 100-block reserve.
//
//	used = (2000-500)  = 1500 blocks
//	free = 400         =  400 blocks
//	psutil percent = 1500/(1500+400)*100 = 78.947... -> 78.9
//	used/total     = 1500/2000*100       = 75.0      (the old, wrong answer)
func TestDiskPercentUsesNonRootPool(t *testing.T) {
	const blockSize = 4096
	stubStatfs(t, map[string]syscall.Statfs_t{
		"/pinned": {
			Bsize:  blockSize,
			Frsize: blockSize,
			Blocks: 2000,
			Bfree:  500, // 100 of these are reserved for root
			Bavail: 400,
		},
	})

	got, err := diskUsageOf("/pinned")
	if err != nil {
		t.Fatalf("diskUsageOf: %v", err)
	}

	wantUsed := int64(1500 * blockSize)
	wantFree := int64(400 * blockSize)
	if got.Used != wantUsed {
		t.Errorf("used = %d, want %d (blocks-total minus f_bfree)", got.Used, wantUsed)
	}
	if got.Free != wantFree {
		t.Errorf("free = %d, want %d (f_bavail, the non-root pool)", got.Free, wantFree)
	}
	if want := 78.9; math.Abs(got.Percent-want) > 1e-9 {
		t.Errorf("percent = %v, want %v (used/(used+free), not used/total); used/total would give 75.0", got.Percent, want)
	}
}

// TestDiskPercentEqualsPinnedFormula verifies the percent for several layouts,
// including the tie-breaking cases where Python's round(x, 1) differs from
// int(x*10+0.5)/10: 12.25 -> 12.2 (round-half-to-even) and 12.35 -> 12.3.
func TestDiskPercentEqualsPinnedFormula(t *testing.T) {
	cases := []struct {
		name                  string
		blocks, bfree, bavail uint64
		want                  float64
	}{
		// used=1500 free=400 -> 78.9473684... -> 78.9
		{"root reserve", 2000, 500, 400, 78.9},
		// used=800 free=200 -> 80.0 exactly, no rounding involved
		{"exact", 2000, 1200, 200, 80.0},
		// used=0 free=100 -> 0
		{"empty", 2000, 2000, 100, 0.0},
		// used=100 free=0 -> 100
		{"full non-root pool", 2000, 1900, 0, 100.0},
		// used=49 free=351 -> 12.25 -> Python round(12.25, 1) = 12.2
		{"tie down to even", 400, 351, 351, 12.2},
		// used=37 free=263 -> 12.333... -> 12.3
		{"round nearest", 400, 363, 263, 12.3},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			stubStatfs(t, map[string]syscall.Statfs_t{
				"/pinned": {Bsize: 1, Frsize: 1, Blocks: c.blocks, Bfree: c.bfree, Bavail: c.bavail},
			})

			got, err := diskUsageOf("/pinned")
			if err != nil {
				t.Fatalf("diskUsageOf: %v", err)
			}
			if math.Abs(got.Percent-c.want) > 1e-9 {
				t.Errorf("percent = %v, want %v", got.Percent, c.want)
			}
		})
	}
}

// TestDiskPercentZeroDenominator verifies a degenerate statfs result (all zeros,
// which a broken network mount can report) yields 0 rather than NaN.
func TestDiskPercentZeroDenominator(t *testing.T) {
	stubStatfs(t, map[string]syscall.Statfs_t{
		"/pinned": {Bsize: 0, Frsize: 0, Blocks: 0, Bfree: 0, Bavail: 0},
	})

	got, err := diskUsageOf("/pinned")
	if err != nil {
		t.Fatalf("diskUsageOf: %v", err)
	}
	if got.Percent != 0 {
		t.Errorf("percent = %v, want 0 when used+free is 0", got.Percent)
	}
	if math.IsNaN(got.Percent) {
		t.Error("percent is NaN, want 0")
	}
}

// TestDiskBlockSizeFallsBackToBsize verifies a filesystem reporting f_frsize 0
// still yields byte counts rather than zeros, mirroring psutil's choice of the
// fundamental block size.
func TestDiskBlockSizeFallsBackToBsize(t *testing.T) {
	stubStatfs(t, map[string]syscall.Statfs_t{
		"/pinned": {Bsize: 1024, Frsize: 0, Blocks: 1000, Bfree: 250, Bavail: 200},
	})

	got, err := diskUsageOf("/pinned")
	if err != nil {
		t.Fatalf("diskUsageOf: %v", err)
	}
	if want := int64(1000 * 1024); got.Total != want {
		t.Errorf("total = %d, want %d (f_bsize when f_frsize is 0)", got.Total, want)
	}
	if want := int64(750 * 1024); got.Used != want {
		t.Errorf("used = %d, want %d", got.Used, want)
	}
}

// TestDiskUsageOfPropagatesStatfsError verifies an unreadable path reports an
// error so diskUsage can skip that mount point, mirroring the Python
// psutil.disk_usage raising.
func TestDiskUsageOfPropagatesStatfsError(t *testing.T) {
	stubStatfs(t, nil)

	if _, err := diskUsageOf("/gone"); err == nil {
		t.Error("diskUsageOf on an unreadable path returned no error")
	}
}

// TestDiskUsageSkipsUnreadableMount verifies diskUsage drops a mount point whose
// statfs fails instead of publishing a zeroed entry, and still reports the
// readable one.
func TestDiskUsageSkipsUnreadableMount(t *testing.T) {
	// TempDir is unreadable; "/" is readable.
	stubStatfs(t, map[string]syscall.Statfs_t{
		"/": {Bsize: 1, Frsize: 1, Blocks: 100, Bfree: 60, Bavail: 40},
	})
	s := &systemSource{cfg: agent.Config{TempDir: "/unreadable"}}

	got, err := s.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage: %v", err)
	}
	if _, ok := got["/unreadable"]; ok {
		t.Errorf("diskUsage published the unreadable mount: %v", got)
	}
	if _, ok := got["/"]; !ok {
		t.Errorf("diskUsage dropped the readable mount: %v", got)
	}
}

// TestDiskUsageTempKeyUsesUserTempDir pins the second disk entry's key.
//
// Python keys it on get_user_temp_dir(): RAY_TMPDIR, else TMPDIR, else /tmp.
// Keying it on the agent's --temp-dir would be wrong on two counts — that value
// is the session dir (<user temp dir>/ray), so both the key and the measured
// mount differ from Python's.
func TestDiskUsageTempKeyUsesUserTempDir(t *testing.T) {
	stubStatfs(t, map[string]syscall.Statfs_t{
		"/":        {Bsize: 1, Frsize: 1, Blocks: 100, Bfree: 60, Bavail: 40},
		"/var/tmp": {Bsize: 1, Frsize: 1, Blocks: 200, Bfree: 100, Bavail: 100},
	})
	stubUserTempDir(t, "/var/tmp")

	// A session temp dir that must NOT appear as a key.
	s := &systemSource{cfg: agent.Config{TempDir: "/var/tmp/ray"}}

	got, err := s.diskUsage()
	if err != nil {
		t.Fatalf("diskUsage: %v", err)
	}
	if _, ok := got["/var/tmp"]; !ok {
		t.Errorf("diskUsage keys = %v, want a %q entry (get_user_temp_dir)", keysOf(got), "/var/tmp")
	}
	if _, ok := got["/var/tmp/ray"]; ok {
		t.Errorf("diskUsage keys = %v, must not key the temp entry on --temp-dir", keysOf(got))
	}
}

// TestUserTempDirPrecedence pins get_user_temp_dir's resolution order:
// RAY_TMPDIR beats TMPDIR beats the /tmp fallback.
func TestUserTempDirPrecedence(t *testing.T) {
	for _, c := range []struct {
		name     string
		ray, tmp string
		want     string
	}{
		{"RAY_TMPDIR wins", "/data/raytmp", "/var/tmp", "/data/raytmp"},
		{"TMPDIR fallback", "", "/var/tmp", "/var/tmp"},
		{"default", "", "", "/tmp"},
	} {
		t.Run(c.name, func(t *testing.T) {
			t.Setenv("RAY_TMPDIR", c.ray)
			t.Setenv("TMPDIR", c.tmp)

			if got := userTempDir(); got != c.want {
				t.Errorf("userTempDir() = %q, want %q", got, c.want)
			}
		})
	}
}

// keysOf returns the sorted keys of a DiskInfo map for stable failure messages.
func keysOf(m map[string]DiskInfo) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TestDiskUsagePercentMatchesPsutilOnLiveMount compares the production code path
// against a live mount rather than a pinned layout, so a future divergence in the
// statfs field mapping is caught. The assertion is structural: used+free must
// reconstruct a subrange of total, and percent must agree with used/(used+free).
// It deliberately does not read psutil, keeping the test hermetic.
func TestDiskUsagePercentMatchesPsutilOnLiveMount(t *testing.T) {
	got, err := diskUsageOf("/")
	if err != nil {
		t.Fatalf("diskUsageOf(/): %v", err)
	}
	if got.Total <= 0 {
		t.Fatalf("total = %d, want a positive size for a live mount", got.Total)
	}
	if got.Used < 0 || got.Free < 0 {
		t.Fatalf("used = %d free = %d, want non-negative", got.Used, got.Free)
	}
	if got.Used+got.Free > got.Total {
		t.Errorf("used+free = %d exceeds total = %d", got.Used+got.Free, got.Total)
	}
	want := math.RoundToEven(float64(got.Used)/float64(got.Used+got.Free)*1000) / 10
	if math.Abs(got.Percent-want) > 1e-9 {
		t.Errorf("percent = %v, want %v from used/(used+free)", got.Percent, want)
	}
}
