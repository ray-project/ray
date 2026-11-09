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

// tpuNodeSuffix marks the device plugin's per-node variant of a TPU metric. It
// is stripped before matching, so one set of cases covers both namings.
const tpuNodeSuffix = "_node"

// tpuField identifies which utilization field a parsed Prometheus sample set.
// The values form a bitmask so a device can record which fields it has seen
// from the plain naming and which only from the "_node" variant.
type tpuField uint8

const (
	tpuFieldNone        tpuField = 0
	tpuFieldTensorcore  tpuField = 1 << 0
	tpuFieldHBM         tpuField = 1 << 1
	tpuFieldDutyCycle   tpuField = 1 << 2
	tpuFieldMemoryUsed  tpuField = 1 << 3
	tpuFieldMemoryTotal tpuField = 1 << 4
)

// tpuDeviceSamples accumulates the samples of one accelerator. The plain and
// "_node" namings are summed separately, and plainSeen records which fields the
// plain naming supplied, so the merge can prefer plain without depending on the
// order the two namings appear in the /metrics output.
type tpuDeviceSamples struct {
	plain     TpuUtilizationInfo
	node      TpuUtilizationInfo
	plainSeen tpuField
}

// parseTPUPrometheus parses the device plugin Prometheus text output into
// merged TPU utilization records indexed by accelerator. Unreadable or
// malformed input degrades to an empty slice.
//
// The merge reconciles two namings and two aggregation rules:
//
//   - Python matches only the plain names, so a "_node" sample is invisible to
//     it. Go also accepts the "_node" variant, which recovers data when the
//     device plugin emits only that naming (Python would report nothing), but
//     plain must win when both are present or Go would disagree with Python.
//   - Python sums repeated samples of the same field for one index (+=), so
//     duplicates add up rather than the last line winning. Summing is done
//     within each naming, never across them: summing both namings together
//     would double count a device that reports its value twice.
func parseTPUPrometheus(body []byte) []TpuUtilizationInfo {
	merged := map[int]*tpuDeviceSamples{}
	for _, line := range strings.Split(string(body), "\n") {
		info, field, plain, ok := parseTPUSample(line)
		if !ok {
			continue
		}
		acc, exists := merged[info.Index]
		if !exists {
			acc = &tpuDeviceSamples{}
			merged[info.Index] = acc
		}
		if plain {
			acc.plainSeen |= field
			addTPUField(&acc.plain, info, field)
			continue
		}
		addTPUField(&acc.node, info, field)
	}

	out := make([]TpuUtilizationInfo, 0, len(merged))
	for index, acc := range merged {
		out = append(out, acc.merged(index))
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Index < out[j].Index })
	return out
}

// addTPUField adds the single field carried by info into dst, mirroring the
// Python merge loop which does `merged[field] += info[field]` for every field.
// The identity fields are copied rather than summed; every sample of an
// accelerator carries the same ones, so the last copy is equivalent to the
// first. Index is set here so the merged record needs no extra bookkeeping.
func addTPUField(dst, info *TpuUtilizationInfo, field tpuField) {
	dst.Index = info.Index
	dst.Name = info.Name
	dst.TPUType = info.TPUType
	dst.TPUTopology = info.TPUTopology
	if field&tpuFieldTensorcore != 0 {
		dst.TensorcoreUtilization += info.TensorcoreUtilization
	}
	if field&tpuFieldHBM != 0 {
		dst.HBMUtilization += info.HBMUtilization
	}
	if field&tpuFieldDutyCycle != 0 {
		dst.DutyCycle += info.DutyCycle
	}
	if field&tpuFieldMemoryUsed != 0 {
		dst.MemoryUsed += info.MemoryUsed
	}
	if field&tpuFieldMemoryTotal != 0 {
		dst.MemoryTotal += info.MemoryTotal
	}
}

// merged combines the accumulated samples of one accelerator. A field comes
// from the plain naming when that naming supplied it, and from the "_node"
// variant otherwise. Identity fields also follow the plain naming when present,
// so the record describes one consistent source.
func (a *tpuDeviceSamples) merged(index int) TpuUtilizationInfo {
	src := &a.node
	if a.plainSeen != tpuFieldNone {
		src = &a.plain
	}
	return TpuUtilizationInfo{
		Index:                 index,
		Name:                  src.Name,
		TPUType:               src.TPUType,
		TPUTopology:           src.TPUTopology,
		TensorcoreUtilization: a.pickFloat(tpuFieldTensorcore, a.plain.TensorcoreUtilization, a.node.TensorcoreUtilization),
		HBMUtilization:        a.pickFloat(tpuFieldHBM, a.plain.HBMUtilization, a.node.HBMUtilization),
		DutyCycle:             a.pickFloat(tpuFieldDutyCycle, a.plain.DutyCycle, a.node.DutyCycle),
		MemoryUsed:            a.pickInt(tpuFieldMemoryUsed, a.plain.MemoryUsed, a.node.MemoryUsed),
		MemoryTotal:           a.pickInt(tpuFieldMemoryTotal, a.plain.MemoryTotal, a.node.MemoryTotal),
	}
}

// pickFloat returns the plain value of field when the plain naming supplied it,
// and the "_node" value otherwise.
func (a *tpuDeviceSamples) pickFloat(field tpuField, plain, node float64) float64 {
	if a.plainSeen&field != 0 {
		return plain
	}
	return node
}

// pickInt is pickFloat for the byte-valued memory fields.
func (a *tpuDeviceSamples) pickInt(field tpuField, plain, node int64) int64 {
	if a.plainSeen&field != 0 {
		return plain
	}
	return node
}

// parseTPUSample parses a single Prometheus sample line into a TPU
// utilization record carrying only the metric reported by that sample. It
// returns ok=false for comment, blank, or non-TPU lines. The returned field
// identifies which utilization field was populated and plain reports whether
// the sample used the plain naming (matched by Python) or its "_node" variant,
// so the caller can merge the two namings without letting them add up.
func parseTPUSample(line string) (info *TpuUtilizationInfo, field tpuField, plain bool, ok bool) {
	line = strings.TrimSpace(line)
	if line == "" || strings.HasPrefix(line, "#") {
		return nil, tpuFieldNone, false, false
	}

	name, labels, valueStr, ok := splitSample(line)
	if !ok {
		return nil, tpuFieldNone, false, false
	}
	acceleratorID, hasAccel := labels["accelerator_id"]
	if !hasAccel {
		return nil, tpuFieldNone, false, false
	}
	index, ok := acceleratorIndex(acceleratorID)
	if !ok {
		return nil, tpuFieldNone, false, false
	}

	info = &TpuUtilizationInfo{
		Index:       index,
		Name:        acceleratorID,
		TPUType:     labels["model"],
		TPUTopology: labels["tpu_topology"],
	}

	// The "_node" variant of each metric carries the same information under a
	// suffixed name, so the suffix is stripped once instead of doubling every
	// branch below.
	base := name
	plain = true
	if trimmed, hadSuffix := strings.CutSuffix(name, tpuNodeSuffix); hadSuffix {
		base = trimmed
		plain = false
	}

	switch base {
	case "tensorcore_utilization":
		info.TensorcoreUtilization = parseFloat(valueStr)
		return info, tpuFieldTensorcore, plain, true
	case "memory_bandwidth_utilization":
		info.HBMUtilization = parseFloat(valueStr)
		return info, tpuFieldHBM, plain, true
	case "duty_cycle":
		info.DutyCycle = parseFloat(valueStr)
		return info, tpuFieldDutyCycle, plain, true
	case "memory_used":
		info.MemoryUsed = parseInt(valueStr)
		return info, tpuFieldMemoryUsed, plain, true
	case "memory_total":
		info.MemoryTotal = parseInt(valueStr)
		return info, tpuFieldMemoryTotal, plain, true
	default:
		return nil, tpuFieldNone, false, false
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
