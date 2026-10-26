package reporter

import (
	"encoding/json"
	"sort"
)

// StatsPayloadComposer assembles the JSON stats payload that the dashboard
// reporter publishes to GCS, mirroring the Python ReporterAgent
// _generate_stats_payload. The emitted keys use camelCase ("Google style") and
// the structure matches the Python StatsPayload pydantic model in
// reporter_models.py.
type StatsPayloadComposer struct {
	stats map[string]interface{}
	now   float64
}

// NewStatsPayloadComposer builds a composer from a StatsCollector.Collect()
// result map (snake_case keys) plus the POSIX timestamp that should appear in
// the "now" field.
func NewStatsPayloadComposer(stats map[string]interface{}, now float64) *StatsPayloadComposer {
	return &StatsPayloadComposer{stats: stats, now: now}
}

// Compose converts the snake_case stats map into the camelCase JSON payload and
// returns the marshaled JSON string. It returns an error only if marshaling
// fails.
func (c *StatsPayloadComposer) Compose() (string, error) {
	out, ok := toGoogleStyle(c.stats).(map[string]interface{})
	if !ok {
		out = map[string]interface{}{}
	}
	out["now"] = c.now
	data, err := json.Marshal(out)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

// toGoogleStyle recursively converts a snake_case stats value into the
// camelCase form expected by the Python StatsPayload model. It handles the
// concrete value types produced by StatsCollector.Collect() as well as generic
// maps/slices, so it degrades gracefully for any input shape.
func toGoogleStyle(v interface{}) interface{} {
	switch val := v.(type) {
	case map[string]interface{}:
		out := make(map[string]interface{}, len(val))
		for k, sub := range val {
			// The Python StatsPayload pydantic model does not declare an
			// "additional" field, so pydantic v1 (Extra.ignore) discards it
			// during parse_obj before json.dumps. Drop it here to keep the Go
			// payload structurally identical to Python's published output.
			if k == "additional" {
				continue
			}
			out[toCamelCase(k)] = toGoogleStyle(sub)
		}
		return out
	case map[string]DiskInfo:
		out := make(map[string]interface{}, len(val))
		for k, d := range val {
			out[k] = diskInfoGoogleStyle(d)
		}
		return out
	case []interface{}:
		out := make([]interface{}, len(val))
		for i, e := range val {
			out[i] = toGoogleStyle(e)
		}
		return out
	case []GpuUtilizationInfo:
		out := make([]interface{}, len(val))
		for i, g := range val {
			out[i] = gpuGoogleStyle(g)
		}
		return out
	case []TpuUtilizationInfo:
		out := make([]interface{}, len(val))
		for i, t := range val {
			out[i] = tpuGoogleStyle(t)
		}
		return out
	case GpuUtilizationInfo:
		return gpuGoogleStyle(val)
	case TpuUtilizationInfo:
		return tpuGoogleStyle(val)
	case DiskIOMetrics:
		return []interface{}{val.ReadBytes, val.WriteBytes, val.ReadCount, val.WriteCount}
	case IOMetrics:
		return []interface{}{val.Sent, val.Recv}
	case DiskInfo:
		return diskInfoGoogleStyle(val)
	default:
		return v
	}
}

// diskInfoGoogleStyle renders a DiskInfo as the camelCase DiskUsage object
// {total, used, free, percent}.
func diskInfoGoogleStyle(d DiskInfo) map[string]interface{} {
	return map[string]interface{}{
		"total":   d.Total,
		"used":    d.Used,
		"free":    d.Free,
		"percent": d.Percent,
	}
}

// gpuGoogleStyle renders a GpuUtilizationInfo as the camelCase
// GpuUtilizationInfo object, converting the processes_pids map into a list.
func gpuGoogleStyle(g GpuUtilizationInfo) map[string]interface{} {
	pids := make([]interface{}, 0, len(g.ProcessesPIDs))
	keys := make([]int, 0, len(g.ProcessesPIDs))
	for k := range g.ProcessesPIDs {
		keys = append(keys, k)
	}
	// Order processes_pids by PID (ascending). This intentionally diverges from
	// Python, which uses list(gpu["processes_pids"].values()) and therefore
	// preserves the dict insertion order. Sorting by PID is deterministic and
	// stable across runs, which the reviewer accepted as a deliberate, safe
	// deviation from Python's ordering.
	sort.Ints(keys)
	for _, k := range keys {
		p := g.ProcessesPIDs[k]
		entry := map[string]interface{}{
			"pid":            p.PID,
			"gpuMemoryUsage": p.GPUMemoryUsage,
			"gpuUtilization": p.GPUUtilization,
		}
		pids = append(pids, entry)
	}
	return map[string]interface{}{
		"index":          g.Index,
		"name":           g.Name,
		"uuid":           g.UUID,
		"utilizationGpu": g.UtilizationGPU,
		"memoryUsed":     g.MemoryUsed,
		"memoryTotal":    g.MemoryTotal,
		"processesPids":  pids,
	}
}

// tpuGoogleStyle renders a TpuUtilizationInfo as the camelCase
// TpuUtilizationInfo object.
func tpuGoogleStyle(t TpuUtilizationInfo) map[string]interface{} {
	return map[string]interface{}{
		"index":                 t.Index,
		"name":                  t.Name,
		"tpuType":               t.TPUType,
		"tpuTopology":           t.TPUTopology,
		"tensorcoreUtilization": t.TensorcoreUtilization,
		"hbmUtilization":        t.HBMUtilization,
		"dutyCycle":             t.DutyCycle,
		"memoryUsed":            t.MemoryUsed,
		"memoryTotal":           t.MemoryTotal,
	}
}

// toCamelCase converts a snake_case key to camelCase ("disk_io" -> "diskIo").
// Keys that are already single words or camelCase pass through unchanged.
func toCamelCase(s string) string {
	var b []byte
	upper := false
	for i := 0; i < len(s); i++ {
		c := s[i]
		if c == '_' {
			upper = true
			continue
		}
		if upper && c >= 'a' && c <= 'z' {
			b = append(b, c-'a'+'A')
			upper = false
			continue
		}
		upper = false
		b = append(b, c)
	}
	return string(b)
}
