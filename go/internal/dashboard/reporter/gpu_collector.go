package reporter

import (
	"encoding/csv"
	"os/exec"
	"strconv"
	"strings"
)

// ProcessGPUInfo holds GPU usage for a single process, aligned with the
// Python ProcessGPUInfo TypedDict in gpu_providers.py.
type ProcessGPUInfo struct {
	PID            int
	GPUMemoryUsage int64
	GPUUtilization *int
}

// GpuUtilizationInfo holds GPU utilization for a single GPU device, aligned
// with the Python GpuUtilizationInfo TypedDict in gpu_providers.py. Memory
// fields are in MiB and UtilizationGPU is a percentage (0-100), matching the
// nvidia-smi --query-gpu output.
type GpuUtilizationInfo struct {
	Index          int
	Name           string
	UUID           string
	UtilizationGPU int
	MemoryUsed     int64
	MemoryTotal    int64
	ProcessesPIDs  map[int]ProcessGPUInfo
}

// commandRunner abstracts executing an external command and returning its
// combined stdout+stderr. It is a package-level variable so tests can inject
// a fake and avoid invoking the real nvidia-smi binary.
type commandRunner func(name string, arg ...string) ([]byte, error)

// runCommand is the production commandRunner, wrapping os/exec.
var runCommand commandRunner = func(name string, arg ...string) ([]byte, error) {
	return exec.Command(name, arg...).CombinedOutput()
}

// gpuQueryArgs is the fixed argument list passed to nvidia-smi. Keeping the
// arguments as a constant whitelist prevents any external string from being
// injected into the command line.
var gpuQueryArgs = []string{
	"--query-gpu=index,uuid,name,utilization.gpu,memory.used,memory.total",
	"--format=csv,noheader,nounits",
}

// GPUCollector collects GPU utilization by parsing nvidia-smi output.
type GPUCollector struct{}

// NewGPUCollector builds a GPUCollector using the default nvidia-smi command.
func NewGPUCollector() *GPUCollector {
	return &GPUCollector{}
}

// Collect returns GPU utilization for all devices. It degrades to a non-nil
// empty slice when nvidia-smi is unavailable or the command fails, so callers
// never observe an error.
func (c *GPUCollector) Collect() []GpuUtilizationInfo {
	out, err := runCommand("nvidia-smi", gpuQueryArgs...)
	if err != nil {
		return []GpuUtilizationInfo{}
	}
	return parseGPUCsv(out)
}

// parseGPUCsv parses the nvidia-smi CSV output into GPU utilization records.
// Malformed or short records are skipped; unparseable numeric fields default
// to zero.
func parseGPUCsv(out []byte) []GpuUtilizationInfo {
	r := csv.NewReader(strings.NewReader(string(out)))
	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return []GpuUtilizationInfo{}
	}

	gpus := make([]GpuUtilizationInfo, 0, len(records))
	for _, rec := range records {
		if len(rec) < 6 {
			continue
		}
		index, _ := strconv.Atoi(strings.TrimSpace(rec[0]))
		uuid := strings.TrimSpace(rec[1])
		name := strings.TrimSpace(rec[2])
		util, _ := strconv.Atoi(strings.TrimSpace(rec[3]))
		memUsed, _ := strconv.ParseInt(strings.TrimSpace(rec[4]), 10, 64)
		memTotal, _ := strconv.ParseInt(strings.TrimSpace(rec[5]), 10, 64)
		gpus = append(gpus, GpuUtilizationInfo{
			Index:          index,
			UUID:           uuid,
			Name:           name,
			UtilizationGPU: util,
			MemoryUsed:     memUsed,
			MemoryTotal:    memTotal,
			ProcessesPIDs:  map[int]ProcessGPUInfo{},
		})
	}
	return gpus
}
