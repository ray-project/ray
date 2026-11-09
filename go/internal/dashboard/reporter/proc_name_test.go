package reporter

import (
	"encoding/json"
	"os"
	"testing"
)

// processInfoPayloadKeys is the set of keys the Python ProcessInfo model
// declares (reporter_models.py), which is exactly what json.dumps of the parsed
// model can emit besides the two GPU fields that _get_workers adds.
var processInfoPayloadKeys = map[string]bool{
	"pid":            true,
	"createTime":     true,
	"cpuPercent":     true,
	"cpuTimes":       true,
	"cmdline":        true,
	"memoryInfo":     true,
	"memoryFullInfo": true,
	"numFds":         true,
	"gpuMemoryUsage": true,
	"gpuUtilization": true,
}

// TestProcAttrsOmitsProcessName pins that procAttrs never reports the process
// name. Python collects exactly PSUTIL_PROCESS_ATTRS through
// psutil.Process.as_dict(attrs=...), and that list has no "name" entry, so the
// Python stats dict never carries the key; the ProcessInfo model has no field
// for it either, so pydantic would discard it. Go read /proc/<pid>/comm and put
// it under "name", which toGoogleStyle carried straight into the published
// payload as a key the Python payload never has.
//
// Both a live pid and a nonexistent one are checked, so the assertion holds for
// the degraded path as well, where only the unreadable fields are omitted.
func TestProcAttrsOmitsProcessName(t *testing.T) {
	s := &systemSource{}
	for _, pid := range []int{os.Getpid(), 1 << 30} {
		info := s.procAttrs(pid)
		if name, ok := info["name"]; ok {
			t.Errorf("procAttrs(%d) reported name = %v, want no such key", pid, name)
		}
	}
}

// TestPayloadProcessInfoHasNoUndeclaredKeys pins the observable effect at the
// published payload: every ProcessInfo position carries only keys the Python
// model declares. Checking the whole key set rather than just "name" catches any
// future extra field of the same kind, which is the actual defect here — the Go
// collector emitted a key Python never produces and nothing dropped it.
//
// The entries come from the real procAttrs so the test tracks the collector
// instead of a hand-built fixture. gpus and tpus legitimately carry a "name"
// field of their own, asserted at the end so the check cannot pass vacuously.
func TestPayloadProcessInfoHasNoUndeclaredKeys(t *testing.T) {
	s := &systemSource{}
	self := s.procAttrs(os.Getpid())

	stats := headStats()
	stats["workers"] = []interface{}{self}
	stats["raylet"] = self
	stats["agent"] = self
	stats["gcs"] = self
	stats["additional"] = map[string]interface{}{"log_monitor": []interface{}{self}}

	out, err := NewStatsPayloadComposer(stats, 1700000000.0).Compose()
	if err != nil {
		t.Fatalf("Compose returned error: %v", err)
	}
	var payload map[string]interface{}
	if err := json.Unmarshal([]byte(out), &payload); err != nil {
		t.Fatalf("payload is not valid JSON: %v", err)
	}

	workers, ok := payload["workers"].([]interface{})
	if !ok || len(workers) == 0 {
		t.Fatalf("workers empty or not a list: %v", payload["workers"])
	}
	entries := map[string]map[string]interface{}{}
	for i, w := range workers {
		proc, ok := w.(map[string]interface{})
		if !ok {
			t.Fatalf("workers[%d] is not an object: %v", i, w)
		}
		entries["workers"] = proc
	}
	for _, key := range []string{"raylet", "agent", "gcs"} {
		proc, ok := payload[key].(map[string]interface{})
		if !ok {
			t.Fatalf("%s is not an object: %v", key, payload[key])
		}
		entries[key] = proc
	}

	for position, proc := range entries {
		for k := range proc {
			if !processInfoPayloadKeys[k] {
				t.Errorf("%s carries %q = %v, which the Python ProcessInfo model does not declare",
					position, k, proc[k])
			}
		}
	}

	// The device entries keep their own name field, which Python declares in
	// GpuUtilizationInfo / TpuUtilizationInfo. Asserting it here proves the
	// absence above is scoped to process entries rather than to the payload.
	gpus, ok := payload["gpus"].([]interface{})
	if !ok || len(gpus) == 0 {
		t.Fatalf("gpus empty or not a list: %v", payload["gpus"])
	}
	gpu, ok := gpus[0].(map[string]interface{})
	if !ok {
		t.Fatalf("gpus[0] is not an object: %v", gpus[0])
	}
	if _, ok := gpu["name"]; !ok {
		t.Errorf("gpus[0] lost its name field: %v", gpu)
	}
}
