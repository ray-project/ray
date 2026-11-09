package reporter

import (
	"math"
	"os"
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestProcAttrsCPUPercentStartsAtZeroAndAdvances verifies cpu_percent follows the
// psutil Process.cpu_percent(interval=None) contract: the first observation for a
// pid is 0.0, and a later observation after the process has burned CPU is
// positive. This is the behaviour the reporter's component_cpu_percentage gauge
// depends on.
func TestProcAttrsCPUPercentStartsAtZeroAndAdvances(t *testing.T) {
	s := &systemSource{}
	pid := os.Getpid()

	first := s.procAttrs(pid)
	if got, _ := first["cpu_percent"].(float64); got != 0.0 {
		t.Fatalf("first cpu_percent = %v, want 0.0 (psutil interval=None semantics)", got)
	}

	// Burn CPU so the next sample has a strictly positive delta, then wait a
	// little so the elapsed-time divisor is not tiny.
	deadline := time.Now().Add(150 * time.Millisecond)
	x := 0
	for time.Now().Before(deadline) {
		x++
	}
	if x == 0 {
		t.Fatal("busy loop did not run")
	}

	second := s.procAttrs(pid)
	got, ok := second["cpu_percent"].(float64)
	if !ok {
		t.Fatalf("second cpu_percent type = %T, want float64", second["cpu_percent"])
	}
	if got <= 0 {
		t.Errorf("second cpu_percent = %v, want > 0 after burning CPU", got)
	}
	// psutil rounds to one decimal place, so the value must land exactly on a
	// tenth rather than carrying float noise.
	if math.Abs(got*10-math.Round(got*10)) > 1e-9 {
		t.Errorf("cpu_percent = %v is not rounded to one decimal place", got)
	}
}

// TestProcAttrsCPUTimes verifies cpu_times reports the psutil fields in seconds
// with plausible magnitudes for the current process.
func TestProcAttrsCPUTimes(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	ct, ok := info["cpu_times"].(map[string]interface{})
	if !ok {
		t.Fatalf("cpu_times type = %T, want map[string]interface{}", info["cpu_times"])
	}
	for _, key := range []string{"user", "system", "children_user", "children_system"} {
		v, ok := ct[key].(float64)
		if !ok {
			t.Errorf("cpu_times[%q] type = %T, want float64", key, ct[key])
			continue
		}
		if v < 0 {
			t.Errorf("cpu_times[%q] = %v, want >= 0", key, v)
		}
	}
	// Both user and system time are ticks divided by USER_HZ (100). The test
	// binary has certainly run for a measurable amount of CPU, and if the value
	// were left in raw ticks it would be 100x larger; a sane bound catches both
	// a units mistake and a wrong field index in /proc/<pid>/stat.
	user := ct["user"].(float64)
	if user <= 0 {
		t.Errorf("cpu_times user = %v, want > 0 for the running test process", user)
	}
	if user > 24*3600 {
		t.Errorf("cpu_times user = %v seconds, implausibly large; ticks may not have been divided by USER_HZ", user)
	}
}

// TestProcAttrsMemoryInfoUnits verifies memory_info reports rss/vms/shared in
// bytes with rss <= vms, which is the invariant of /proc/<pid>/statm. A page
// count leaking through undivided would produce rss of a few hundred bytes for a
// process that really uses megabytes.
func TestProcAttrsMemoryInfoUnits(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	mem, ok := info["memory_info"].(map[string]interface{})
	if !ok {
		t.Fatalf("memory_info type = %T, want map[string]interface{}", info["memory_info"])
	}
	rss, ok := mem["rss"].(int64)
	if !ok {
		t.Fatalf("memory_info rss type = %T, want int64", mem["rss"])
	}
	vms, ok := mem["vms"].(int64)
	if !ok {
		t.Fatalf("memory_info vms type = %T, want int64", mem["vms"])
	}
	shared, ok := mem["shared"].(int64)
	if !ok {
		t.Fatalf("memory_info shared type = %T, want int64", mem["shared"])
	}

	if rss <= 0 {
		t.Errorf("memory_info rss = %d, want > 0", rss)
	}
	// A Go test binary maps tens of megabytes of vms, so a value below 1 MiB
	// almost certainly means page counts were never multiplied by the page size.
	if rss < 1<<20 {
		t.Errorf("memory_info rss = %d bytes, implausibly small; page count may not have been scaled to bytes", rss)
	}
	if rss > vms {
		t.Errorf("memory_info rss = %d > vms = %d, violates the statm invariant", rss, vms)
	}
	if shared < 0 {
		t.Errorf("memory_info shared = %d, want >= 0", shared)
	}

	// Cross-check rss against the kernel's own statm reading so a wrong field
	// index is caught: field 2 (resident) times the page size must equal rss.
	statm, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		t.Skipf("cannot read /proc/self/statm for cross-check: %v", err)
	}
	fields := strings.Fields(string(statm))
	if len(fields) < 2 {
		t.Skipf("malformed /proc/self/statm: %q", statm)
	}
	var residentPages int64
	for _, r := range fields[1] {
		if r < '0' || r > '9' {
			t.Fatalf("non-numeric resident field %q in /proc/self/statm", fields[1])
		}
		residentPages = residentPages*10 + int64(r-'0')
	}
	// The process may allocate between the two reads, so allow a small margin.
	wantRSS := residentPages * int64(os.Getpagesize())
	if rss < wantRSS-(8<<20) || rss > wantRSS+(8<<20) {
		t.Errorf("memory_info rss = %d, want ~%d (statm resident pages x page size)", rss, wantRSS)
	}
}

// TestProcAttrsFullMemoryInfoUnits verifies memory_full_info adds the uss and pss
// byte totals on top of memory_info, with uss <= pss and pss <= rss holding for a
// real process.
func TestProcAttrsFullMemoryInfoUnits(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	full, ok := info["memory_full_info"].(map[string]interface{})
	if !ok {
		t.Fatalf("memory_full_info type = %T, want map[string]interface{}", info["memory_full_info"])
	}
	uss, ok := full["uss"].(int64)
	if !ok {
		t.Fatalf("memory_full_info uss type = %T, want int64", full["uss"])
	}
	pss, ok := full["pss"].(int64)
	if !ok {
		t.Fatalf("memory_full_info pss type = %T, want int64", full["pss"])
	}
	rss := full["rss"].(int64)

	if uss <= 0 {
		t.Errorf("memory_full_info uss = %d, want > 0", uss)
	}
	if pss <= 0 {
		t.Errorf("memory_full_info pss = %d, want > 0", pss)
	}
	// USS counts only private memory, PSS charges shared pages proportionally and
	// RSS charges every mapped page in full, so uss <= pss <= rss must hold.
	if uss > pss {
		t.Errorf("memory_full_info uss = %d > pss = %d, violates uss <= pss", uss, pss)
	}
	if pss > rss {
		t.Errorf("memory_full_info pss = %d > rss = %d, violates pss <= rss", pss, rss)
	}
}

// TestProcAttrsNumFDs verifies num_fds matches the actual /proc/<pid>/fd entry
// count, which the component_num_fds and node_num_fds gauges report.
func TestProcAttrsNumFDs(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	n, ok := info["num_fds"].(int)
	if !ok {
		t.Fatalf("num_fds type = %T, want int", info["num_fds"])
	}
	if n <= 0 {
		t.Fatalf("num_fds = %d, want > 0 (a process always has stdin/stdout/stderr)", n)
	}

	entries, err := os.ReadDir("/proc/self/fd")
	if err != nil {
		t.Skipf("cannot read /proc/self/fd: %v", err)
	}
	// Reading the directory itself opens an fd, so the observed count can be one
	// higher than the value sampled before the read.
	if n != len(entries) && n+1 != len(entries) {
		t.Errorf("num_fds = %d, want %d or %d (the directory read opens one fd)", n, len(entries)-1, len(entries))
	}
}

// TestProcAttrsCmdline verifies cmdline is the NUL-separated argv of the process,
// re-split on NUL so the first element is the executable path.
func TestProcAttrsCmdline(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(os.Getpid())

	cmdline, ok := info["cmdline"].([]string)
	if !ok {
		t.Fatalf("cmdline type = %T, want []string", info["cmdline"])
	}
	if len(cmdline) == 0 {
		t.Fatal("cmdline is empty, want the test binary argv")
	}
	if !strings.Contains(cmdline[0], "test") && !strings.HasPrefix(cmdline[0], "/") {
		t.Errorf("cmdline[0] = %q, want the test binary path", cmdline[0])
	}
}

// TestReadProcMemoryInfoMatchesStatm verifies the statm parser independently of
// procAttrs, so a regression in readProcMemoryInfo is localized.
func TestReadProcMemoryInfoMatchesStatm(t *testing.T) {
	mem, err := readProcMemoryInfo(os.Getpid())
	if err != nil {
		t.Fatalf("readProcMemoryInfo: %v", err)
	}
	pageSize := int64(os.Getpagesize())
	for _, key := range []string{"rss", "vms", "shared"} {
		v, ok := mem[key].(int64)
		if !ok {
			t.Fatalf("memory info %q type = %T, want int64", key, mem[key])
		}
		if v%pageSize != 0 {
			t.Errorf("memory info %q = %d is not a multiple of the page size %d; pages may not have been scaled", key, v, pageSize)
		}
	}
}

// TestReadProcMemoryInfoMissingProcess verifies a nonexistent pid degrades to an
// error rather than inventing values.
func TestReadProcMemoryInfoMissingProcess(t *testing.T) {
	if _, err := readProcMemoryInfo(1 << 30); err == nil {
		t.Error("readProcMemoryInfo on a nonexistent pid returned no error")
	}
	if _, err := readProcFullMemoryInfo(1 << 30); err == nil {
		t.Error("readProcFullMemoryInfo on a nonexistent pid returned no error")
	}
	if _, err := countProcFDs(1 << 30); err == nil {
		t.Error("countProcFDs on a nonexistent pid returned no error")
	}
	if _, err := procStatFields(1 << 30); err == nil {
		t.Error("procStatFields on a nonexistent pid returned no error")
	}
}

// TestParseSmapsUsage verifies the smaps/smaps_rollup parser sums the private
// lines into USS and the Pss line into PSS, converting kB to bytes and ignoring
// the Pss_Anon/Pss_File/Pss_Shmem breakdown lines.
func TestParseSmapsUsage(t *testing.T) {
	rollup := strings.Join([]string{
		"7f0000000000-7f0000001000 r-xp 00000000 08:01 1234  /usr/lib/libc.so",
		"Size:               1024 kB",
		"KernelPageSize:        4 kB",
		"Rss:                 512 kB",
		"Pss:                 300 kB",
		"Pss_Anon:            100 kB",
		"Pss_File:            200 kB",
		"Pss_Shmem:             0 kB",
		"Shared_Clean:        100 kB",
		"Private_Clean:        50 kB",
		"Private_Dirty:       200 kB",
		"Referenced:          512 kB",
		"Swap:                 64 kB",
	}, "\n")

	uss, pss, swap := parseSmapsUsage(rollup)
	if want := int64((50 + 200) * 1024); uss != want {
		t.Errorf("uss = %d, want %d (Private_Clean + Private_Dirty, kB -> bytes)", uss, want)
	}
	// Only the "Pss:" line counts; the Pss_Anon/File/Shmem breakdown must not be
	// added on top or the total would be wrong.
	if want := int64(300 * 1024); pss != want {
		t.Errorf("pss = %d, want %d (only the Pss line)", pss, want)
	}
	if want := int64(64 * 1024); swap != want {
		t.Errorf("swap = %d, want %d", swap, want)
	}
}

// TestParseSmapsUsageIncludesHugetlb verifies Private_Hugetlb (carried by
// smaps_rollup, unlike plain smaps) is part of USS like psutil's uss.
func TestParseSmapsUsageIncludesHugetlb(t *testing.T) {
	raw := "Private_Clean: 1 kB\nPrivate_Dirty: 2 kB\nPrivate_Hugetlb: 4 kB\nPss: 3 kB\n"
	uss, pss, _ := parseSmapsUsage(raw)
	if want := int64(7 * 1024); uss != want {
		t.Errorf("uss = %d, want %d (Clean + Dirty + Hugetlb)", uss, want)
	}
	if want := int64(3 * 1024); pss != want {
		t.Errorf("pss = %d, want %d", pss, want)
	}
}

// TestParseSmapsUsageEmpty verifies an unparsable document degrades to zeros
// instead of panicking.
func TestParseSmapsUsageEmpty(t *testing.T) {
	uss, pss, swap := parseSmapsUsage("")
	if uss != 0 || pss != 0 || swap != 0 {
		t.Errorf("parseSmapsUsage(\"\") = (%d, %d, %d), want all zeros", uss, pss, swap)
	}
	// A line with no value and a non-numeric value must both contribute zero.
	uss, pss, _ = parseSmapsUsage("Private_Clean:\nPss: not-a-number kB\n")
	if uss != 0 || pss != 0 {
		t.Errorf("parseSmapsUsage(malformed lines) = (%d, %d), want all zeros", uss, pss)
	}
}

// TestProcStatFieldsMatchesKernelStat verifies the post-comm field split lines up
// with the kernel's field numbering: the first returned field is state (field 3).
func TestProcStatFieldsMatchesKernelStat(t *testing.T) {
	fields, err := procStatFields(os.Getpid())
	if err != nil {
		t.Fatalf("procStatFields: %v", err)
	}
	// /proc/<pid>/stat carries 52 fields; field 3 onward are 50 fields. A comm
	// containing parentheses would shift a naive split, which is exactly what
	// splitting after the last ')' must avoid.
	if len(fields) < 50 {
		t.Fatalf("procStatFields returned %d fields, want >= 50: %v", len(fields), fields)
	}
	if len(fields[0]) != 1 {
		t.Errorf("field 3 (state) = %q, want a single character", fields[0])
	}
	// Field 22 overall is starttime, i.e. index 19 here, and must be a positive
	// tick count.
	var startTicks int64
	for _, r := range fields[19] {
		if r < '0' || r > '9' {
			t.Fatalf("starttime field %q is not numeric", fields[19])
		}
		startTicks = startTicks*10 + int64(r-'0')
	}
	if startTicks <= 0 {
		t.Errorf("starttime = %d ticks, want > 0", startTicks)
	}
}

// TestProcessCPUPercentHandlesShortStat verifies cpu_percent degrades to 0 when
// the stat line is too short to carry the CPU-time fields, instead of reading out
// of range.
func TestProcessCPUPercentHandlesShortStat(t *testing.T) {
	s := &systemSource{}
	// Prime the previous sample, then present a truncated times reading: the
	// delta must simply be non-positive rather than index out of range.
	times := processCPUTimes{user: 1, system: 1}
	if got := s.processCPUPercent(os.Getpid(), times); got != 0 {
		t.Errorf("first processCPUPercent = %v, want 0", got)
	}
	if got := s.processCPUPercent(os.Getpid(), times); got != 0 {
		t.Errorf("processCPUPercent with no CPU delta = %v, want 0", got)
	}
	if got := s.processCPUPercent(os.Getpid(), times); got != 0 {
		t.Errorf("processCPUPercent with no elapsed CPU = %v, want 0", got)
	}
}

// TestTicksToSeconds verifies the USER_HZ conversion and its degradation.
func TestTicksToSeconds(t *testing.T) {
	if got := ticksToSeconds("250"); got != 2.5 {
		t.Errorf("ticksToSeconds(250) = %v, want 2.5", got)
	}
	if got := ticksToSeconds("not-a-number"); got != 0 {
		t.Errorf("ticksToSeconds(garbage) = %v, want 0", got)
	}
}

// TestPagesToBytes verifies the page-size scaling and its degradation.
func TestPagesToBytes(t *testing.T) {
	if got := pagesToBytes("3", 4096); got != 12288 {
		t.Errorf("pagesToBytes(3, 4096) = %d, want 12288", got)
	}
	if got := pagesToBytes("garbage", 4096); got != 0 {
		t.Errorf("pagesToBytes(garbage) = %d, want 0", got)
	}
}

// TestProcAttrsUnknownProcessDegrades verifies a nonexistent pid still yields the
// identifying fields instead of panicking, and omits only the unreadable ones.
func TestProcAttrsUnknownProcessDegrades(t *testing.T) {
	s := &systemSource{}
	info := s.procAttrs(1 << 30)
	if info["pid"] != 1<<30 {
		t.Errorf("pid = %v, want %d even for an unreadable process", info["pid"], 1<<30)
	}
	for _, k := range []string{"cpu_times", "memory_info", "memory_full_info", "num_fds", "cmdline"} {
		if _, ok := info[k]; ok {
			t.Errorf("procAttrs unexpectedly reported %q for a nonexistent pid", k)
		}
	}
}

// TestProcessCreateTimeUsesRawBootTimeInContainer verifies create_time stays a
// valid epoch timestamp even when bootTime() diverts to the container path, i.e.
// processCreateTime must not recurse into itself.
func TestProcessCreateTimeUsesRawBootTimeInContainer(t *testing.T) {
	if os.Getpid() == 1 {
		t.Skip("running as pid 1; the container boot-time path is the host path")
	}
	s := &systemSource{}
	ct := s.processCreateTime(os.Getpid())
	raw, err := s.rawBootTime()
	if err != nil {
		t.Fatalf("rawBootTime: %v", err)
	}
	if ct < raw {
		t.Errorf("createTime = %v earlier than the raw boot time %v", ct, raw)
	}
	if delta := float64(time.Now().Unix()) - ct; delta < 0 || delta > 30*24*3600 {
		t.Errorf("createTime = %v is %v seconds from now, outside a plausible window", ct, delta)
	}
}

// TestReadProcessCPUTimesSelf verifies the CPU-time reader reports the running
// process with monotonic, non-negative values.
func TestReadProcessCPUTimesSelf(t *testing.T) {
	before, ok := readProcessCPUTimes(os.Getpid())
	if !ok {
		t.Fatal("readProcessCPUTimes(self) reported no CPU times")
	}
	if before.user < 0 || before.system < 0 {
		t.Errorf("cpu times = %+v, want non-negative", before)
	}

	// Burn CPU and confirm the sampled user/system time only moves forward.
	deadline := time.Now().Add(100 * time.Millisecond)
	n := 0
	for time.Now().Before(deadline) {
		n++
	}
	if n == 0 {
		t.Fatal("busy loop did not run")
	}
	after, ok := readProcessCPUTimes(os.Getpid())
	if !ok {
		t.Fatal("readProcessCPUTimes(self) reported no CPU times after burning CPU")
	}
	if after.user < before.user {
		t.Errorf("user CPU time went backwards: %v -> %v", before.user, after.user)
	}
	if after.user == before.user && after.system == before.system {
		t.Error("CPU times did not advance after burning CPU; wrong /proc/<pid>/stat field indices")
	}
}

// TestCountProcFDsMatchesReadDir verifies the fd counter against a direct count
// of the same directory, allowing for the directory handle used by the read.
func TestCountProcFDsMatchesReadDir(t *testing.T) {
	pid := os.Getpid()
	got, err := countProcFDs(pid)
	if err != nil {
		t.Fatalf("countProcFDs: %v", err)
	}
	// The fixture deliberately opens an fd to give the count something stable to
	// observe; os.OpenFile keeps it open for the duration of the assertion.
	f, err := os.OpenFile(os.DevNull, os.O_RDONLY, 0)
	if err != nil {
		t.Fatalf("open %s: %v", os.DevNull, err)
	}
	defer f.Close()

	after, err := countProcFDs(pid)
	if err != nil {
		t.Fatalf("countProcFDs after opening a file: %v", err)
	}
	if after != got+1 {
		t.Errorf("num_fds went %d -> %d after opening one fd, want exactly +1", got, after)
	}
}

// TestSplitCmdline verifies the cmdline splitter handles the empty and
// NUL-separated cases.
func TestSplitCmdline(t *testing.T) {
	if got := splitCmdline(""); len(got) != 0 {
		t.Errorf("splitCmdline(\"\") = %v, want an empty slice", got)
	}
	got := splitCmdline("/usr/bin/python\x00-m\x00ray\x00")
	if len(got) != 4 || got[0] != "/usr/bin/python" || got[1] != "-m" || got[2] != "ray" {
		t.Errorf("splitCmdline = %q, want the NUL-separated argv", got)
	}
}

// TestProcStatFieldsTrimsPIDNameWithSpaces verifies the field split survives a
// comm value containing spaces and parentheses, which is why the parser cuts
// after the last ')' rather than splitting the whole line.
func TestProcStatFieldsTrimsPIDNameWithSpaces(t *testing.T) {
	// A comm like "(my (weird) proc)" appears inside the parentheses, and the
	// kernel always terminates the comm with the last ')' on the line.
	line := "1234 (my (weird) proc) S 1 1234 1234 0 -1 4194304 100 0 0 0 5 7 0 0 20 0 1 0 999 123456789 500 18446744073709551615"
	rp := strings.LastIndex(line, ")")
	fields := strings.Fields(line[rp+1:])
	if len(fields) < 20 {
		t.Fatalf("split produced %d fields, want >= 20: %v", len(fields), fields)
	}
	if fields[0] != "S" {
		t.Errorf("field 3 = %q, want the state S", fields[0])
	}
	if fields[11] != "5" || fields[12] != "7" {
		t.Errorf("utime/stime = %q/%q, want 5/7", fields[11], fields[12])
	}
	if fields[19] != "999" {
		t.Errorf("starttime = %q, want 999", fields[19])
	}
}

// TestReadProcessCPUTimesFieldIndices verifies the reader's field offsets
// against a real stat line: utime/stime/cutime/cstime are stat fields 14-17,
// i.e. indices 11-14 after the state field, and every reported value is exactly
// its raw tick count divided by USER_HZ.
func TestReadProcessCPUTimesFieldIndices(t *testing.T) {
	// One snapshot only: reading the live process twice would race the tick
	// counter, and a tick landing between the reads would make the comparison
	// fail for reasons that have nothing to do with the field offsets.
	fields, err := procStatFields(os.Getpid())
	if err != nil {
		t.Skipf("cannot read own stat: %v", err)
	}
	times, ok := cpuTimesFromStatFields(fields)
	if !ok {
		t.Skip("stat line too short")
	}
	want := []struct {
		name string
		got  float64
		idx  int
	}{
		{"user", times.user, 11},
		{"system", times.system, 12},
		{"children_user", times.childrenUser, 13},
		{"children_system", times.childrenSystem, 14},
	}
	for _, w := range want {
		if got := ticksToSeconds(fields[w.idx]); got != w.got {
			t.Errorf("%s = %v, want %v from stat field %d (%q)", w.name, w.got, got, w.idx+3, fields[w.idx])
		}
	}
}

// TestReadProcessCPUTimesSelfConsistentWithGetrusage verifies the reported CPU
// times agree with the kernel's own getrusage accounting for this process, which
// catches a wrong field index or a units error of an order of magnitude.
func TestReadProcessCPUTimesSelfConsistentWithGetrusage(t *testing.T) {
	times, ok := readProcessCPUTimes(os.Getpid())
	if !ok {
		t.Skip("cannot read own CPU times")
	}
	var ru syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &ru); err != nil {
		t.Skipf("getrusage: %v", err)
	}
	sec := func(tv syscall.Timeval) float64 {
		return float64(tv.Sec) + float64(tv.Usec)/1e6
	}
	// The stat read and the getrusage call are separated in time, so allow a
	// generous margin; the check is about the value's magnitude and field, not
	// exact equality.
	margin := 1.0
	if got, want := times.user, sec(ru.Utime); got < want-margin || got > want+margin {
		t.Errorf("user CPU time = %v, want ~%v from getrusage", got, want)
	}
	if got, want := times.system, sec(ru.Stime); got < want-margin || got > want+margin {
		t.Errorf("system CPU time = %v, want ~%v from getrusage", got, want)
	}
}
