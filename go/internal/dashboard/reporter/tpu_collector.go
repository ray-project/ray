package reporter

import (
	"io"
	"net/http"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

// tpuDevicePluginAddrEnv is the environment variable holding the TPU device
// plugin listen address, aligned with the Python TPU_DEVICE_PLUGIN_ADDR.
const tpuDevicePluginAddrEnv = "TPU_DEVICE_PLUGIN_ADDR"

// TpuUtilizationInfo holds utilization for a single TPU accelerator, aligned
// with the Python TpuUtilizationInfo TypedDict in gpu_providers.py. The
// utilization fields (TensorcoreUtilization, HBMUtilization, DutyCycle) are
// percentages (0-100) and the memory fields are in bytes, matching the device
// plugin /metrics output.
type TpuUtilizationInfo struct {
	Index                 int     `json:"index"`
	Name                  string  `json:"name"`
	TPUType               string  `json:"tpu_type"`
	TPUTopology           string  `json:"tpu_topology"`
	TensorcoreUtilization float64 `json:"tensorcore_utilization"`
	HBMUtilization        float64 `json:"hbm_utilization"`
	DutyCycle             float64 `json:"duty_cycle"`
	MemoryUsed            int64   `json:"memory_used"`
	MemoryTotal           int64   `json:"memory_total"`
}

// httpGetter abstracts issuing an HTTP GET and returning the response body.
// It is a package-level variable so tests can inject a fake and avoid making
// real network calls, mirroring the commandRunner injection used by the GPU
// collector.
type httpGetter func(url string) ([]byte, error)

// httpTimeout bounds how long getMetrics waits for the device plugin /metrics
// endpoint, keeping the collection overhead low when the endpoint hangs.
const httpTimeout = 2 * time.Second

// tpuHTTPClient is the HTTP client used by getMetrics. It is a package-level
// variable so tests can inspect the configured timeout.
var tpuHTTPClient = &http.Client{Timeout: httpTimeout}

// getMetrics is the production httpGetter, wrapping net/http. A non-2xx status
// is treated as an error so callers can degrade to empty.
var getMetrics httpGetter = func(url string) ([]byte, error) {
	resp, err := tpuHTTPClient.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, io.ErrUnexpectedEOF
	}
	return io.ReadAll(resp.Body)
}

// TPUCollector collects TPU utilization by parsing the TPU device plugin
// /metrics endpoint (Prometheus text format).
type TPUCollector struct {
	endpoint string
}

// NewTPUCollector builds a TPUCollector using the TPU_DEVICE_PLUGIN_ADDR
// environment variable as the device plugin address, falling back to an empty
// endpoint when the variable is unset.
func NewTPUCollector() *TPUCollector {
	return NewTPUCollectorWithEndpoint(os.Getenv(tpuDevicePluginAddrEnv))
}

// NewTPUCollectorWithEndpoint builds a TPUCollector targeting the given device
// plugin address. An empty address yields an empty collection result.
func NewTPUCollectorWithEndpoint(addr string) *TPUCollector {
	endpoint := ""
	if addr != "" {
		endpoint = "http://" + addr + "/metrics"
	}
	return &TPUCollector{endpoint: endpoint}
}

// Collect returns TPU utilization for all accelerators reported by the device
// plugin. It degrades to a non-nil empty slice when the endpoint is
// unreachable or the metrics cannot be parsed, so callers never observe an
// error.
func (c *TPUCollector) Collect() []TpuUtilizationInfo {
	if c.endpoint == "" {
		return []TpuUtilizationInfo{}
	}
	body, err := getMetrics(c.endpoint)
	if err != nil {
		return []TpuUtilizationInfo{}
	}
	return parseTPUPrometheus(body)
}

// tpuField identifies which utilization field a parsed Prometheus sample set,
// so the merge step can overwrite that single field without summing. This
// prevents double counting when the device plugin emits both a plain metric
// and its "_node" variant for the same accelerator (summing them could push a
// utilization past 100%).
type tpuField int

const (
	tpuFieldNone tpuField = iota
	tpuFieldTensorcore
	tpuFieldHBM
	tpuFieldDutyCycle
	tpuFieldMemoryUsed
	tpuFieldMemoryTotal
)

// parseTPUPrometheus parses the device plugin Prometheus text output into
// merged TPU utilization records indexed by accelerator. Unreadable or
// malformed input degrades to an empty slice.
func parseTPUPrometheus(body []byte) []TpuUtilizationInfo {
	merged := map[int]*TpuUtilizationInfo{}
	for _, line := range strings.Split(string(body), "\n") {
		info, field, ok := parseTPUSample(line)
		if !ok {
			continue
		}
		key := info.Index
		if cur, exists := merged[key]; exists {
			switch field {
			case tpuFieldTensorcore:
				cur.TensorcoreUtilization = info.TensorcoreUtilization
			case tpuFieldHBM:
				cur.HBMUtilization = info.HBMUtilization
			case tpuFieldDutyCycle:
				cur.DutyCycle = info.DutyCycle
			case tpuFieldMemoryUsed:
				cur.MemoryUsed = info.MemoryUsed
			case tpuFieldMemoryTotal:
				cur.MemoryTotal = info.MemoryTotal
			}
			continue
		}
		merged[key] = info
	}

	out := make([]TpuUtilizationInfo, 0, len(merged))
	for _, info := range merged {
		out = append(out, *info)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// parseTPUSample parses a single Prometheus sample line into a TPU
// utilization record carrying only the metric reported by that sample. It
// returns ok=false for comment, blank, or non-TPU lines. The returned field
// identifies which utilization field was populated so callers can merge
// records field-by-field instead of summing.
func parseTPUSample(line string) (*TpuUtilizationInfo, tpuField, bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return nil, tpuFieldNone, false
	}

	name, labels, valueStr, ok := splitSample(line)
	if !ok {
		return nil, tpuFieldNone, false
	}
	acceleratorID, hasAccel := labels["accelerator_id"]
	if !hasAccel {
		return nil, tpuFieldNone, false
	}
	index, ok := acceleratorIndex(acceleratorID)
	if !ok {
		return nil, tpuFieldNone, false
	}

	info := &TpuUtilizationInfo{
		Index:       index,
		Name:        acceleratorID,
		TPUType:     labels["model"],
		TPUTopology: labels["tpu_topology"],
	}

	switch name {
	case "tensorcore_utilization", "tensorcore_utilization_node":
		info.TensorcoreUtilization = parseFloat(valueStr)
		return info, tpuFieldTensorcore, true
	case "memory_bandwidth_utilization", "memory_bandwidth_utilization_node":
		info.HBMUtilization = parseFloat(valueStr)
		return info, tpuFieldHBM, true
	case "duty_cycle", "duty_cycle_node":
		info.DutyCycle = parseFloat(valueStr)
		return info, tpuFieldDutyCycle, true
	case "memory_used", "memory_used_node":
		info.MemoryUsed = parseInt(valueStr)
		return info, tpuFieldMemoryUsed, true
	case "memory_total", "memory_total_node":
		info.MemoryTotal = parseInt(valueStr)
		return info, tpuFieldMemoryTotal, true
	default:
		return nil, tpuFieldNone, false
	}
}

// splitSample splits a Prometheus sample line of the form
// metric_name{label="v",...} value into its name, label map, and value string.
// It returns ok=false for lines without a value token.
func splitSample(line string) (name string, labels map[string]string, value string, ok bool) {
	labels = map[string]string{}
	open := strings.IndexByte(line, '{')
	closeIdx := -1
	if open >= 0 {
		closeIdx = strings.IndexByte(line[open:], '}')
		if closeIdx < 0 {
			return "", nil, "", false
		}
		closeIdx += open
		name = strings.TrimSpace(line[:open])
		labelStr := line[open+1 : closeIdx]
		for _, pair := range strings.Split(labelStr, ",") {
			pair = strings.TrimSpace(pair)
			eq := strings.IndexByte(pair, '=')
			if eq < 0 {
				continue
			}
			k := strings.TrimSpace(pair[:eq])
			v := strings.Trim(strings.TrimSpace(pair[eq+1:]), `"`)
			labels[k] = v
		}
		rest := strings.TrimSpace(line[closeIdx+1:])
		fields := strings.Fields(rest)
		if len(fields) == 0 {
			return "", nil, "", false
		}
		return name, labels, fields[0], true
	}

	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", nil, "", false
	}
	return fields[0], labels, fields[1], true
}

// acceleratorIndex derives the accelerator numeric index from an
// accelerator_id of the form "<chip>-<index>", mirroring Python
// accelerator_id.split("-")[1]. It returns ok=false for ids without a
// parseable index so malformed ids are skipped rather than merged into index 0.
func acceleratorIndex(acceleratorID string) (int, bool) {
	parts := strings.Split(acceleratorID, "-")
	if len(parts) < 2 {
		return 0, false
	}
	idx, err := strconv.Atoi(parts[1])
	if err != nil {
		return 0, false
	}
	return idx, true
}

func parseFloat(s string) float64 {
	v, _ := strconv.ParseFloat(strings.TrimSpace(s), 64)
	return v
}

func parseInt(s string) int64 {
	v, _ := strconv.ParseInt(strings.TrimSpace(s), 10, 64)
	return v
}
