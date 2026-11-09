package reporter

import (
	"math"
	"syscall"
	"testing"
)

// TestRoundToDecimalsMatchesPythonRound pins roundToDecimals against the values
// Python's round(f, n) produces. The cases are the ones where the previous
// implementations disagreed: exact decimal ties, and values whose decimal
// expansion is a tie but whose binary approximation is one ulp off.
func TestRoundToDecimalsMatchesPythonRound(t *testing.T) {
	for _, c := range []struct {
		in   float64
		n    int
		want float64
	}{
		// Ties broken to even, as round() does. int(f*100+0.5) rounded these up.
		{0.125, 2, 0.12},
		{0.375, 2, 0.38},
		{0.625, 2, 0.62},
		{0.875, 2, 0.88},
		{1.125, 2, 1.12},
		// Not a tie in decimal, but 2.675*100 = 267.49999999999997 in binary, so
		// rounding the scaled value lands on the wrong neighbour either way:
		// Python gives 2.67, int(f*100+0.5) gives 2.68.
		{2.675, 2, 2.67},
		{1.005, 2, 1.0},
		{0.005, 2, 0.01},
		{0.145, 2, 0.14},
		// One decimal place, as the percentage call sites use.
		{0.25, 1, 0.2},
		{0.75, 1, 0.8},
		{1.25, 1, 1.2},
		{2.25, 1, 2.2},
		{12.25, 1, 12.2},
		{12.35, 1, 12.3},
		{33.333333, 1, 33.3},
		// Values with no rounding to do must be returned unchanged.
		{0, 2, 0},
		{0.5, 2, 0.5},
		{-0.5, 1, -0.5},
		{-1.2345, 2, -1.23},
	} {
		if got := roundToDecimals(c.in, c.n); got != c.want {
			t.Errorf("roundToDecimals(%v, %d) = %v, want %v", c.in, c.n, got, c.want)
		}
	}
}

// TestPreviousRoundingFormulasWereWrong is the contrast that justifies the
// helper: it reimplements the two formulas this change replaced and asserts
// they disagree with Python's round on the inputs pinned above. Without it, a
// regression back to either formula would still fail the tables, but the reason
// would be less obvious.
func TestPreviousRoundingFormulasWereWrong(t *testing.T) {
	// The round2 formula: int(f*100+0.5)/100.
	oldRound2 := func(f float64) float64 { return float64(int(f*100+0.5)) / 100 }
	for _, c := range []struct{ in, py float64 }{
		{0.125, 0.12},
		{0.625, 0.62},
		{1.125, 1.12},
		{2.675, 2.67},
	} {
		if oldRound2(c.in) == c.py {
			t.Errorf("oldRound2(%v) = %v but Python gives %v; the contrast no "+
				"longer demonstrates a deviation", c.in, oldRound2(c.in), c.py)
		}
	}

	// The disk formula: math.RoundToEven(x*1000)/10, where x is the ratio.
	oldDisk := func(used, free float64) float64 {
		return math.RoundToEven(used/(used+free)*1000) / 10
	}
	for _, c := range []struct{ used, free, py float64 }{
		{49, 31, 61.3},
		{51, 29, 63.7},
		{23, 57, 28.7},
	} {
		if oldDisk(c.used, c.free) == c.py {
			t.Errorf("oldDisk(%v, %v) = %v but Python gives %v; the contrast no "+
				"longer demonstrates a deviation",
				c.used, c.free, oldDisk(c.used, c.free), c.py)
		}
	}
}

// TestRound2MatchesPythonRoundLoadAvg covers the load-average call site: Python divides
// the load average by the CPU count and rounds to two places. The expected
// values are the literals Python's round() returned for each quotient.
func TestRound2MatchesPythonRoundLoadAvg(t *testing.T) {
	for _, c := range []struct {
		load, cpus, want float64
	}{
		// load/cpus = 0.125 exactly -> round(0.125, 2) = 0.12 (not 0.13).
		{0.5, 4, 0.12},
		{0.25, 2, 0.12},
		// load/cpus = 0.625 exactly -> round(0.625, 2) = 0.62 (not 0.63).
		{1.25, 2, 0.62},
		{0.625, 1, 0.62},
		// load/cpus = 1.125 exactly -> round(1.125, 2) = 1.12 (not 1.13).
		{2.25, 2, 1.12},
		// Non-ties, where both implementations agree.
		{0.75, 2, 0.38},
		{1.5, 4, 0.38},
		{1.5, 3, 0.5},
		{0.15, 3, 0.05},
		{0.5, 3, 0.17},
	} {
		in := c.load / c.cpus
		if got := round2(in); got != c.want {
			t.Errorf("round2(%v / %v = %v) = %v, want %v",
				c.load, c.cpus, in, got, c.want)
		}
	}
}

// TestMemPercentMatchesPython pins memPercent against Python's
// round(used/total, 3) * 100, including the ratio-level ties where the old
// math.Round(ratio*1000)/1000 * 100 disagreed.
func TestMemPercentMatchesPython(t *testing.T) {
	for _, c := range []struct {
		used, total int64
		want        float64
	}{
		// 1/16 = 0.0625 exactly; round(0.0625, 3) = 0.062 -> 6.2, not 6.3.
		{1, 16, 6.2},
		{5, 16, 31.2},
		// 3/80 = 0.0375 exactly; round -> 0.037, scaled by 100.
		{3, 80, 0.037 * 100},
		// Repeating decimals pin the scaling order: the ratio is rounded before
		// multiplying by 100, so 1/3 gives 0.333 * 100, not round(33.33..., 3).
		{1, 3, 0.333 * 100},
		{2, 3, 0.667 * 100},
		{1, 7, 0.143 * 100},
		// Bounds.
		{0, 8, 0},
		{8, 8, 100},
		{1, 1, 100},
		// A non-positive total short-circuits to 0.
		{5, 0, 0},
	} {
		got := memPercent(c.used, c.total)
		if math.Abs(got-c.want) > 1e-12 {
			t.Errorf("memPercent(%d, %d) = %v, want %v", c.used, c.total, got, c.want)
		}
	}
}

// TestDiskPercentTieCasesWhereScalingLosesTheTie covers the remaining rounding edge: for
// these used/free pairs the exact percentage is a .x5 tie, but multiplying by
// 10 first lands one ulp away from it, so rounding the scaled value picked the
// wrong neighbour. Python rounds the already-scaled percentage.
func TestDiskPercentTieCasesWhereScalingLosesTheTie(t *testing.T) {
	for _, c := range []struct {
		used, free uint64
		want       float64
	}{
		// 61.25000000000001 -> 61.3; RoundToEven(x*1000)/10 gave 61.2.
		{49, 31, 61.3},
		{98, 62, 61.3},
		// 63.74999999999999 -> 63.7; RoundToEven(x*1000)/10 gave 63.8.
		{51, 29, 63.7},
		// 28.749999999999996 -> 28.7; RoundToEven(x*1000)/10 gave 28.8.
		{23, 57, 28.7},
		// Cases the old scaling happened to get right must stay right.
		{49, 351, 12.2},
		{37, 263, 12.3},
		{1, 1, 50.0},
		{0, 100, 0.0},
		{100, 0, 100.0},
	} {
		stubStatfs(t, map[string]syscall.Statfs_t{
			"/pinned": {
				Bsize: 1, Frsize: 1,
				Blocks: c.used + c.free, Bfree: c.free, Bavail: c.free,
			},
		})

		got, err := diskUsageOf("/pinned")
		if err != nil {
			t.Fatalf("diskUsageOf: %v", err)
		}
		if math.Abs(got.Percent-c.want) > 1e-9 {
			t.Errorf("used=%d free=%d percent = %v, want %v",
				c.used, c.free, got.Percent, c.want)
		}
	}
}
