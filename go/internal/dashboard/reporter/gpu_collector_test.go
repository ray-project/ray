package reporter

import (
	"errors"
	"testing"
)

// fakeCommand returns a commandRunner backed by a fixed output and error so
// tests never invoke the real nvidia-smi binary.
func fakeCommand(out []byte, err error) commandRunner {
	return func(name string, arg ...string) ([]byte, error) {
		return out, err
	}
}

func TestGPUCollectorParsesCSV(t *testing.T) {
	csvOut := []byte(
		"0, GPU-1234abcd, NVIDIA A100-SXM4-40GB, 45, 1234, 40960\n" +
			"1, GPU-5678efgh, NVIDIA GeForce RTX 2080, 0, 0, 8192\n",
	)
	orig := runCommand
	runCommand = fakeCommand(csvOut, nil)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 2 {
		t.Fatalf("Collect() returned %d GPUs, want 2", len(gpus))
	}

	g0 := gpus[0]
	if g0.Index != 0 {
		t.Errorf("gpu[0].Index = %d, want 0", g0.Index)
	}
	if g0.UUID != "GPU-1234abcd" {
		t.Errorf("gpu[0].UUID = %q, want GPU-1234abcd", g0.UUID)
	}
	if g0.Name != "NVIDIA A100-SXM4-40GB" {
		t.Errorf("gpu[0].Name = %q, want NVIDIA A100-SXM4-40GB", g0.Name)
	}
	if g0.UtilizationGPU != 45 {
		t.Errorf("gpu[0].UtilizationGPU = %d, want 45", g0.UtilizationGPU)
	}
	if g0.MemoryUsed != 1234 {
		t.Errorf("gpu[0].MemoryUsed = %d, want 1234", g0.MemoryUsed)
	}
	if g0.MemoryTotal != 40960 {
		t.Errorf("gpu[0].MemoryTotal = %d, want 40960", g0.MemoryTotal)
	}
	if g0.ProcessesPIDs == nil {
		t.Errorf("gpu[0].ProcessesPIDs = nil, want non-nil empty map")
	}

	g1 := gpus[1]
	if g1.Index != 1 {
		t.Errorf("gpu[1].Index = %d, want 1", g1.Index)
	}
	if g1.UUID != "GPU-5678efgh" {
		t.Errorf("gpu[1].UUID = %q, want GPU-5678efgh", g1.UUID)
	}
	if g1.Name != "NVIDIA GeForce RTX 2080" {
		t.Errorf("gpu[1].Name = %q, want NVIDIA GeForce RTX 2080", g1.Name)
	}
}

func TestGPUCollectorDegradesOnCommandError(t *testing.T) {
	orig := runCommand
	runCommand = fakeCommand(nil, errors.New("nvidia-smi not found"))
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if gpus == nil {
		t.Fatalf("Collect() returned nil, want non-nil empty slice")
	}
	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs on error, want 0", len(gpus))
	}
}

func TestGPUCollectorDegradesOnMalformedCSV(t *testing.T) {
	// An unclosed quote makes the CSV unreadable, which must degrade to empty.
	orig := runCommand
	runCommand = fakeCommand([]byte("\"unclosed, 0, GPU-1, NVIDIA A100, 10, 5, 100\n"), nil)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs on malformed input, want 0", len(gpus))
	}
}

func TestGPUCollectorSkipsShortRecords(t *testing.T) {
	// Rows with fewer than 6 fields are not usable GPU records and are skipped,
	// so a uniform short output degrades to empty rather than producing garbage.
	orig := runCommand
	runCommand = fakeCommand([]byte("0, GPU-1, NVIDIA A100\n1, GPU-2, NVIDIA B200\n"), nil)
	defer func() { runCommand = orig }()

	c := NewGPUCollector()
	gpus := c.Collect()

	if len(gpus) != 0 {
		t.Errorf("Collect() returned %d GPUs, want 0 (short records skipped)", len(gpus))
	}
}
