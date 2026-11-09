package reporter

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// stubDiskstats pins the /proc/diskstats and /sys/block views diskIO sees, so
// the aggregate counter can be asserted against a known block-device layout
// instead of whatever devices the test host happens to expose.
//
// devices are the names that get a /sys/block/<name> directory, i.e. the whole
// block devices psutil's is_storage_device would accept.
func stubDiskstats(t *testing.T, diskstats string, devices ...string) {
	t.Helper()

	dir := t.TempDir()
	for _, name := range devices {
		path := filepath.Join(dir, strings.ReplaceAll(name, "/", "!"))
		if err := os.MkdirAll(path, 0o755); err != nil {
			t.Fatalf("create fake sysfs entry %q: %v", path, err)
		}
	}

	statsPath := filepath.Join(dir, "diskstats")
	if err := os.WriteFile(statsPath, []byte(diskstats), 0o600); err != nil {
		t.Fatalf("write fake /proc/diskstats: %v", err)
	}

	origPath, origDir := diskstatsPath, sysBlockDir
	diskstatsPath = statsPath
	sysBlockDir = dir
	t.Cleanup(func() {
		diskstatsPath, sysBlockDir = origPath, origDir
	})
}

// diskstatsLine renders one /proc/diskstats row in the 18-field layout modern
// kernels emit. Only the counters diskIO reads are meaningful; the rest are
// filler, matching the real file's field positions.
func diskstatsLine(name string, reads, sectorsRead, writes, sectorsWrite int64) string {
	return fmt.Sprintf(
		"   8       0 %s %d 0 %d 0 %d 0 %d 0 0 0 0 0 0 0 0 0",
		name, reads, sectorsRead, writes, sectorsWrite,
	)
}

// TestDiskIOSkipsPartitions pins the disk-IO aggregation fix: aggregate disk IO must count
// whole devices only.
//
// Python calls psutil.disk_io_counters() with perdisk=False, which drops any
// row whose name has no /sys/block/<name> entry — i.e. every partition. Go used
// to sum every row in /proc/diskstats, so each partition's counters were added
// on top of the whole device that already contained them. On a real host that
// inflated read_bytes by 36.9% and write_bytes by 42.7%.
//
// Layout: vda carries all the traffic, vda1 + vda2 are its partitions (their
// counters sum to vda's, as the kernel reports them), and dm-0 is an
// independent whole device. Extra whitespace variants of the fields are covered
// by the split on strings.Fields.
func TestDiskIOSkipsPartitions(t *testing.T) {
	stats := strings.Join([]string{
		diskstatsLine("vda", 100, 1000, 200, 2000),
		diskstatsLine("vda1", 40, 400, 80, 800),
		diskstatsLine("vda2", 60, 600, 120, 1200),
		diskstatsLine("dm-0", 10, 100, 20, 200),
	}, "\n") + "\n"

	// Only whole devices exist under /sys/block.
	stubDiskstats(t, stats, "vda", "dm-0")

	s := &systemSource{}
	got, err := s.diskIO()
	if err != nil {
		t.Fatalf("diskIO: %v", err)
	}

	const sectorSize = 512
	want := DiskIOMetrics{
		ReadCount:  110,                       // vda 100 + dm-0 10
		WriteCount: 220,                       // vda 200 + dm-0 20
		ReadBytes:  (1000 + 100) * sectorSize, // sectors are 512-byte units
		WriteBytes: (2000 + 200) * sectorSize,
	}
	if got != want {
		t.Errorf("diskIO = %+v, want %+v (partitions vda1/vda2 must not be added)", got, want)
	}

	// Control: summing every row — the pre-fix behaviour — yields a different
	// answer, so this test genuinely guards the filter rather than passing on
	// an accidental equality.
	unfiltered := DiskIOMetrics{
		ReadCount:  210,
		WriteCount: 420,
		ReadBytes:  (1000 + 400 + 600 + 100) * sectorSize,
		WriteBytes: (2000 + 800 + 1200 + 200) * sectorSize,
	}
	if got == unfiltered {
		t.Fatalf("diskIO returned the unfiltered sum %+v, partitions were not skipped", got)
	}
}

// TestDiskIOSkipsUnknownDevice verifies a row whose device has no /sys/block
// entry contributes nothing, even when it is not obviously a partition. This
// mirrors psutil, which trusts /sys/block alone rather than the device name.
func TestDiskIOSkipsUnknownDevice(t *testing.T) {
	stats := diskstatsLine("vda", 100, 1000, 200, 2000) + "\n" +
		diskstatsLine("ram3", 999, 999, 999, 999) + "\n"

	stubDiskstats(t, stats, "vda")

	got, err := (&systemSource{}).diskIO()
	if err != nil {
		t.Fatalf("diskIO: %v", err)
	}
	if want := (DiskIOMetrics{
		ReadCount:  100,
		WriteCount: 200,
		ReadBytes:  1000 * 512,
		WriteBytes: 2000 * 512,
	}); got != want {
		t.Errorf("diskIO = %+v, want %+v (ram3 has no /sys/block entry)", got, want)
	}
}

// TestDiskIOSkipsShortLines verifies rows with fewer than 14 fields are ignored
// instead of being read at fixed offsets past their end. Partition rows on old
// kernels are the realistic short case.
func TestDiskIOSkipsShortLines(t *testing.T) {
	stats := "   8       0 vda 100 0 1000 0 200 0 2000\n" +
		diskstatsLine("dm-0", 10, 100, 20, 200) + "\n"

	stubDiskstats(t, stats, "vda", "dm-0")

	got, err := (&systemSource{}).diskIO()
	if err != nil {
		t.Fatalf("diskIO: %v", err)
	}
	if want := (DiskIOMetrics{
		ReadCount:  10,
		WriteCount: 20,
		ReadBytes:  100 * 512,
		WriteBytes: 200 * 512,
	}); got != want {
		t.Errorf("diskIO = %+v, want %+v (the 11-field row must be skipped)", got, want)
	}
}

// TestDiskIOMissingDiskstats verifies an absent /proc/diskstats surfaces an
// error so the caller can publish zeros rather than a stale sample.
func TestDiskIOMissingDiskstats(t *testing.T) {
	orig := diskstatsPath
	diskstatsPath = filepath.Join(t.TempDir(), "absent")
	t.Cleanup(func() { diskstatsPath = orig })

	if _, err := (&systemSource{}).diskIO(); err == nil {
		t.Error("diskIO on a missing /proc/diskstats returned no error")
	}
}

// TestIsStorageDeviceMatchesPsutil verifies the /sys/block lookup, including
// psutil's slash substitution: diskstats reports "cciss/c0d0" while sysfs
// names the directory "cciss!c0d0".
func TestIsStorageDeviceMatchesPsutil(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"vda", "cciss!c0d0"} {
		if err := os.MkdirAll(filepath.Join(dir, name), 0o755); err != nil {
			t.Fatalf("create fake sysfs entry %q: %v", name, err)
		}
	}

	orig := sysBlockDir
	sysBlockDir = dir
	t.Cleanup(func() { sysBlockDir = orig })

	for _, c := range []struct {
		name string
		want bool
	}{
		{"vda", true},
		{"cciss/c0d0", true}, // "/" is mapped to "!"
		{"vda1", false},      // partition
		{"cciss/c0d0p1", false},
	} {
		if got := isStorageDevice(c.name); got != c.want {
			t.Errorf("isStorageDevice(%q) = %v, want %v", c.name, got, c.want)
		}
	}
}
