// Copyright 2025 The Ray Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package event implements the EventHead dashboard module that collects events
// reported by agents and serves the /events and /api/v0/cluster_events routes,
// aligned with the Python EventHead in
// python/ray/dashboard/modules/event/event_head.py. Events are collected from
// two sources: the /report_events agent reports, and a local scan of the
// session events directory (monitor_events), which mirrors the Python
// EventHead.monitor_events reading event_*.log files written by agents.
package event

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
)

// maxEventsToCache is MAX_EVENTS_TO_CACHE: the per-job event cache limit,
// configured via RAY_DASHBOARD_MAX_EVENTS_TO_CACHE (default 10000).
const maxEventsToCache = 10000

// defaultListLimit mirrors the state API default limit.
const defaultListLimit = 100

// maxListLimit mirrors RAY_MAX_LIMIT_FROM_API_SERVER (10000) from
// python/ray/util/state/common.py.
const maxListLimit = 10000

// maxConcurrency bounds concurrent cluster_events requests, aligned with
// RAY_STATE_SERVER_MAX_HTTP_REQUEST.
const maxConcurrency = 100

// Event monitor constants, aligned with event_consts.py.
const (
	// scanEventDirInterval is the interval between two scans of the events
	// directory.
	scanEventDirInterval = 2 * time.Second
	// scanEventStartOffset is how far back in file mtime we monitor, so events
	// written shortly before the head started are still picked up.
	scanEventStartOffset = -30 * time.Minute
	// eventReadLineCountLimit caps the lines read from a single file per scan.
	eventReadLineCountLimit = 200
)

// eventSourceAll mirrors event_pb2.Event.SourceType.keys(): the file name
// patterns monitored in the events directory (*{source_type}*.log).
var eventSourceAll = []string{
	"COMMON", "CORE_WORKER", "GCS", "RAYLET", "CLUSTER_LIFECYCLE", "AUTOSCALER", "JOBS",
}

// monitorFile records the read position and stat of an event log file so
// incremental reads only return new lines, aligned with the Python MonitorFile
// namedtuple.
type monitorFile struct {
	size     int64
	mtime    int64
	position int64
}

// clusterEventFilterableColumns mirrors the filterable columns of the Python
// ClusterEventState schema in python/ray/util/state/common.py.
var clusterEventFilterableColumns = map[string]bool{
	"severity":    true,
	"source_type": true,
	"event_id":    true,
}

// clusterEventBaseColumns mirrors the base_columns of the Python
// ClusterEventState schema (detail=False output), aligned with filter_fields in
// python/ray/util/state/common.py.
var clusterEventBaseColumns = []string{"severity", "time", "source_type", "message", "event_id"}

// clusterEventDetailColumns mirrors the full column set of the Python
// ClusterEventState schema (detail=True output), i.e. the base columns plus the
// detail-only custom_fields column.
var clusterEventDetailColumns = []string{"severity", "time", "source_type", "message", "event_id", "custom_fields"}

// jobEvents is an insertion-ordered map from event_id to event, mirroring the
// Python OrderedDict used by EventHead.events.
type jobEvents struct {
	order  []string
	events map[string]map[string]interface{}
}

func newJobEvents() *jobEvents {
	return &jobEvents{events: map[string]map[string]interface{}{}}
}

func (j *jobEvents) set(id string, ev map[string]interface{}) {
	if _, ok := j.events[id]; !ok {
		j.order = append(j.order, id)
	}
	j.events[id] = ev
}

func (j *jobEvents) popOldest() {
	if len(j.order) == 0 {
		return
	}
	id := j.order[0]
	j.order = j.order[1:]
	delete(j.events, id)
}

func (j *jobEvents) values() []interface{} {
	out := make([]interface{}, 0, len(j.order))
	for _, id := range j.order {
		out = append(out, j.events[id])
	}
	return out
}

// EventHead collects and queries cluster events.
type EventHead struct {
	cfg *head.HeadConfig
	mu  sync.RWMutex
	// events maps job_id (hex string) to insertion-ordered event_id -> event,
	// aligned with the Python {job_id: OrderedDict(event_id: event)}.
	events map[string]*jobEvents
	sem    chan struct{}
	// monitorMu guards monitorFiles while the scanner goroutine updates it.
	monitorMu    sync.Mutex
	monitorFiles map[string]monitorFile
}

// New creates an EventHead.
func New(cfg *head.HeadConfig, _ *head.GCSClient) *EventHead {
	return &EventHead{
		cfg:          cfg,
		events:       map[string]*jobEvents{},
		sem:          make(chan struct{}, maxConcurrency),
		monitorFiles: map[string]monitorFile{},
	}
}

// Name returns the module name.
func (e *EventHead) Name() string { return "EventHead" }

// Start launches the background scanner that reads event_*.log files from the
// session events directory and feeds them into the cache, aligned with the
// Python EventHead.monitor_events / EventAgent.monitor_events.
func (e *EventHead) Start(ctx context.Context) error {
	if e.cfg.LogDir == "" {
		log.Log.Info("event monitor disabled: no log dir configured")
		return nil
	}
	startMtime := time.Now().Add(scanEventStartOffset).Unix()
	log.Log.Info("monitor events logs modified after", "start_mtime", startMtime, "event_dir", filepath.Join(e.cfg.LogDir, "events"))
	go e.scanEventLoop(ctx, startMtime)
	return nil
}

// Healthy reports the module health.
func (e *EventHead) Healthy() bool { return true }

// RegisterHTTP registers the event routes.
func (e *EventHead) RegisterHTTP(mux *http.ServeMux) error {
	mux.HandleFunc("POST /report_events", e.handleReportEvents)
	mux.HandleFunc("GET /events", e.handleEvents)
	mux.HandleFunc("GET /api/v0/cluster_events", e.handleClusterEvents)
	return nil
}

// parseEventString parses one event line, aligned with _parse_line in
// python/ray/dashboard/modules/event/event_utils.py (JSON decode plus newline
// restoration in the message field). An empty or invalid line yields nil.
func parseEventString(line string) map[string]interface{} {
	if strings.TrimSpace(line) == "" {
		return nil
	}
	var ev map[string]interface{}
	if err := json.Unmarshal([]byte(line), &ev); err != nil {
		log.Log.Error(err, "parse event line failed", "line", line)
		return nil
	}
	if msg, ok := ev["message"].(string); ok {
		ev["message"] = strings.ReplaceAll(strings.ReplaceAll(msg, "\\n", "\n"), "\\r", "\n")
	}
	return ev
}

// parseEventStrings parses a list of event strings, skipping empty and
// unparseable entries, aligned with parse_event_strings.
func parseEventStrings(lines []string) []map[string]interface{} {
	var out []map[string]interface{}
	for _, line := range lines {
		if ev := parseEventString(line); ev != nil {
			out = append(out, ev)
		}
	}
	return out
}

// scanEventLoop periodically scans the events directory until the context is
// cancelled, aligned with the Python _scan_event_log_files async_loop_forever.
func (e *EventHead) scanEventLoop(ctx context.Context, startMtime int64) {
	ticker := time.NewTicker(scanEventDirInterval)
	defer ticker.Stop()
	for {
		e.scanEvents(startMtime)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
	}
}

// scanEvents lists the event_*.log files in the events directory and reads any
// new lines since the last scan, feeding them into the cache. Only files
// modified after startMtime are monitored, and each file is read from its
// recorded position so already-consumed lines are skipped. Aligned with
// _scan_event_log_files / _read_monitor_file in event_utils.py.
func (e *EventHead) scanEvents(startMtime int64) {
	dir := filepath.Join(e.cfg.LogDir, "events")
	entries, err := os.ReadDir(dir)
	if err != nil {
		// The directory may not exist before the first agent writes events.
		if !os.IsNotExist(err) {
			log.Log.V(1).Info("failed to list events directory", "dir", dir, "error", err)
		}
		return
	}
	var files []string
	for _, entry := range entries {
		name := entry.Name()
		if entry.IsDir() || !strings.HasSuffix(name, ".log") {
			continue
		}
		if !matchesEventSource(name) {
			continue
		}
		files = append(files, filepath.Join(dir, name))
	}
	for _, file := range files {
		if lines := e.readMonitorFile(file, startMtime); len(lines) > 0 {
			e.updateEvents(parseEventStrings(lines))
		}
	}
}

// matchesEventSource reports whether the file name matches the
// *{source_type}*.log pattern of one of the event sources (e.g.
// event_JOBS.log), aligned with _get_source_files.
func matchesEventSource(name string) bool {
	for _, src := range eventSourceAll {
		if strings.Contains(name, src) {
			return true
		}
	}
	return false
}

// readMonitorFile reads the new lines appended to an event file since the last
// scan. The first time a file is seen it is read from the beginning; on later
// scans only lines past the recorded position are returned. Aligned with
// _read_monitor_file.
func (e *EventHead) readMonitorFile(file string, startMtime int64) []string {
	stat, err := os.Stat(file)
	if err != nil || stat.Size() <= 0 {
		return nil
	}
	if stat.ModTime().Unix() < startMtime {
		return nil
	}
	e.monitorMu.Lock()
	defer e.monitorMu.Unlock()
	prev, seen := e.monitorFiles[file]
	position := int64(0)
	if seen {
		if prev.position == prev.size && prev.size == stat.Size() && prev.mtime == stat.ModTime().Unix() {
			// No change since the last scan.
			return nil
		}
		position = prev.position
	} else {
		log.Log.Info("found new event log file", "file", file)
	}
	f, err := os.Open(file)
	if err != nil {
		return nil
	}
	defer f.Close()
	if _, err := f.Seek(position, 0); err != nil {
		return nil
	}
	reader := bufio.NewReader(f)
	var lines []string
	for i := 0; i < eventReadLineCountLimit; i++ {
		line, err := reader.ReadString('\n')
		if len(line) > 0 {
			lines = append(lines, strings.TrimRight(line, "\r\n"))
		}
		if err != nil {
			break
		}
	}
	// The fd position includes bytes buffered but not yet consumed by the
	// reader; subtract the buffered amount to get the true consumed position.
	fdPos, _ := f.Seek(0, 1)
	newPos := fdPos - int64(reader.Buffered())
	e.monitorFiles[file] = monitorFile{size: stat.Size(), mtime: stat.ModTime().Unix(), position: newPos}
	return lines
}

// updateEvents merges the parsed events into the cache keyed by event_id with
// per-job FIFO eviction, aligned with EventHead._update_events.
func (e *EventHead) updateEvents(events []map[string]interface{}) {
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, ev := range events {
		eventID := asString(ev["event_id"])
		if eventID == "" {
			continue
		}
		jobID := "global"
		if cf, ok := ev["custom_fields"].(map[string]interface{}); ok {
			if jid, ok := cf["job_id"]; ok && jid != nil {
				// Python: custom_fields.get("job_id", "global") or "global" —
				// any truthy value (including numbers) is used as the group key,
				// only empty/None falls back to "global".
				if str := fmt.Sprint(jid); str != "" {
					jobID = str
				}
			}
		}
		job := e.events[jobID]
		if job == nil {
			job = newJobEvents()
			e.events[jobID] = job
		}
		job.set(eventID, ev)
	}
	// Evict the oldest (FIFO) entries once a job exceeds MAX * 1.1, aligned
	// with the Python popitem(last=False) loop.
	for _, job := range e.events {
		if len(job.order) > int(float64(maxEventsToCache)*1.1) {
			for len(job.order) > maxEventsToCache {
				job.popOldest()
			}
		}
	}
}

// handleReportEvents serves POST /report_events. The body is a JSON array of
// event strings; the response is rest_response(success=True, message="").
// Aligned with the Python report_events handler: only a JSON array body is
// accepted (anything else returns 400), and each array element is parsed
// individually so a non-string element (e.g. an object) is skipped instead of
// failing the whole request.
func (e *EventHead) handleReportEvents(w http.ResponseWriter, r *http.Request) {
	var raw []json.RawMessage
	if err := json.NewDecoder(r.Body).Decode(&raw); err != nil {
		// Aligned with the Python logger.warning("Failed to parse request
		// body...") in event_head.py; the go/pkg/log interface has no Warning
		// level, so Info (visible by default) is used.
		log.Log.Info("Failed to parse request body", "error", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	lines := make([]string, 0, len(raw))
	for _, el := range raw {
		var s string
		if json.Unmarshal(el, &s) != nil {
			// A non-string element is skipped, matching Python
			// parse_event_strings where json.loads(dict) raises and the
			// exception is caught and ignored.
			continue
		}
		lines = append(lines, s)
	}
	events := parseEventStrings(lines)
	e.updateEvents(events)
	// data is {"success": true}, aligned with the Python report_events handler
	// which returns rest_response(success=True) and the dashboard frontend reads
	// resp.data.success.
	head.RESTResponseCamel(w, http.StatusOK, "", map[string]interface{}{"success": true})
}

// handleEvents serves GET /events?job_id=. Without job_id it returns
// {job_id: [events...]} for every job; with job_id it returns the job's events.
func (e *EventHead) handleEvents(w http.ResponseWriter, r *http.Request) {
	jobID := r.URL.Query().Get("job_id")
	e.mu.RLock()
	defer e.mu.RUnlock()
	if jobID == "" {
		all := map[string]interface{}{}
		for jid, job := range e.events {
			all[jid] = job.values()
		}
		head.RESTResponseCamel(w, http.StatusOK, "All events fetched.", map[string]interface{}{"events": all})
		return
	}
	// A job_id with no events (including an unknown job) yields an empty list,
	// aligned with Python's defaultdict(JobEvents) lookup in get_event.
	jobEventsList := []interface{}{}
	if job := e.events[jobID]; job != nil {
		jobEventsList = job.values()
	}
	head.RESTResponseCamel(w, http.StatusOK, "Job events fetched.", map[string]interface{}{
		"job_id": jobID,
		"events": jobEventsList,
	})
}

// handleClusterEvents serves GET /api/v0/cluster_events with the state-API
// ListApiResponse shape, aligned with list_cluster_events / handle_list_api.
func (e *EventHead) handleClusterEvents(w http.ResponseWriter, r *http.Request) {
	select {
	case e.sem <- struct{}{}:
		defer func() { <-e.sem }()
	default:
		// Python EventHead.limit_handler_ returns HTTPStatusCode.INTERNAL_ERROR
		// (500) when the max in-progress request count is reached, nesting the
		// None result under data.result (rest_response(..., result=None)).
		head.RESTResponse(w, http.StatusInternalServerError,
			"Max number of in-progress requests="+strconv.Itoa(maxConcurrency)+" reached.", map[string]interface{}{"result": nil})
		return
	}
	opt, err := head.ParseListApiOptions(r)
	if err != nil {
		head.RESTResponse(w, http.StatusBadRequest, err.Error(), map[string]interface{}{"result": nil})
		return
	}
	// A limit above the server max must be rejected, aligned with
	// options_from_req in python/ray/dashboard/state_api_utils.py (the Go
	// ParseListApiOptions silently clamps instead, so enforce it here).
	if opt.Limit > maxListLimit {
		head.RESTResponse(w, http.StatusBadRequest,
			fmt.Sprintf("Given limit %d exceeds the supported limit %d. Use a lower limit, or set the `RAY_MAX_LIMIT_FROM_API_SERVER` environment variable to a larger value.", opt.Limit, maxListLimit), map[string]interface{}{"result": nil})
		return
	}
	// Validate filter columns against the ClusterEventState schema.
	for _, k := range opt.FilterKeys {
		if !clusterEventFilterableColumns[k] {
			head.RESTResponse(w, http.StatusBadRequest,
				"The given filter column "+k+" is not supported. Supported filter columns: [event_id severity source_type]", map[string]interface{}{"result": nil})
			return
		}
	}
	resp := e.listClusterEvents(opt)
	head.RESTResponse(w, http.StatusOK, "", map[string]interface{}{"result": resp})
}

// listClusterEvents transforms, filters, sorts and truncates the cached events
// into a ListApiResponse, aligned with _list_cluster_events_impl.
func (e *EventHead) listClusterEvents(opt *head.ListApiOptions) *head.ListApiResponse {
	e.mu.RLock()
	// Use a non-nil empty slice so an empty result serializes as [] rather than
	// null, matching Python's asdict(ListApiResponse(result=[])).
	result := make([]map[string]interface{}, 0)
	for _, job := range e.events {
		for _, id := range job.order {
			// Shallow-copy the cached event so the time field added below never
			// mutates the shared cache (which /report_events writes concurrently).
			copied := make(map[string]interface{}, len(job.events[id]))
			for k, v := range job.events[id] {
				copied[k] = v
			}
			result = append(result, copied)
		}
	}
	e.mu.RUnlock()

	for _, ev := range result {
		if ts, ok := toInt64(ev["timestamp"]); ok {
			ev["time"] = time.Unix(ts, 0).Format("2006-01-02 15:04:05")
		}
	}
	numAfterTruncation := len(result)
	sort.SliceStable(result, func(i, j int) bool {
		return toFloat(result[i]["timestamp"]) < toFloat(result[j]["timestamp"])
	})
	total := len(result)
	filtered := applyClusterEventFilters(result, opt)
	numFiltered := len(filtered)
	// Trim each entry to the ClusterEventState schema columns, aligned with the
	// filter_fields call inside do_filter (detail=False keeps only the base
	// columns, dropping label/source_hostname/source_pid/timestamp).
	for i, ev := range filtered {
		filtered[i] = trimClusterEventFields(ev, opt.Detail)
	}
	if len(filtered) > opt.Limit {
		filtered = filtered[:opt.Limit]
	}
	// partial_failure_warning is "" here, matching the Python
	// _list_cluster_events_impl which relies on the dataclass default.
	empty := ""
	return &head.ListApiResponse{
		Result:                filtered,
		Total:                 total,
		NumAfterTruncation:    numAfterTruncation,
		NumFiltered:           numFiltered,
		PartialFailureWarning: &empty,
	}
}

// trimClusterEventFields keeps only the ClusterEventState schema columns,
// filling a missing column with nil, aligned with filter_fields in
// python/ray/util/state/common.py.
func trimClusterEventFields(ev map[string]interface{}, detail bool) map[string]interface{} {
	cols := clusterEventBaseColumns
	if detail {
		cols = clusterEventDetailColumns
	}
	out := make(map[string]interface{}, len(cols))
	for _, col := range cols {
		if v, ok := ev[col]; ok {
			out[col] = v
		} else {
			out[col] = nil
		}
	}
	return out
}

// applyClusterEventFilters filters events by the option's filters with
// case-insensitive string comparison, aligned with do_filter.
func applyClusterEventFilters(entries []map[string]interface{}, opt *head.ListApiOptions) []map[string]interface{} {
	if len(opt.FilterKeys) == 0 {
		return entries
	}
	out := make([]map[string]interface{}, 0, len(entries))
	for _, ev := range entries {
		ok := true
		for i, k := range opt.FilterKeys {
			pred := "="
			if i < len(opt.FilterPredicates) {
				pred = opt.FilterPredicates[i]
			}
			want := ""
			if i < len(opt.FilterValues) {
				want = opt.FilterValues[i]
			}
			got, exists := ev[k]
			if !exists {
				ok = false
				break
			}
			gotStr := asString(got)
			if pred == "=" && !strings.EqualFold(gotStr, want) {
				ok = false
				break
			}
			if pred == "!=" && strings.EqualFold(gotStr, want) {
				ok = false
				break
			}
		}
		if ok {
			out = append(out, ev)
		}
	}
	return out
}

func asString(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

func toFloat(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	case int64:
		return float64(t)
	case json.Number:
		f, _ := t.Float64()
		return f
	case string:
		f, _ := strconv.ParseFloat(t, 64)
		return f
	}
	return 0
}

func toInt64(v interface{}) (int64, bool) {
	switch t := v.(type) {
	case float64:
		return int64(t), true
	case int:
		return int64(t), true
	case int64:
		return t, true
	case json.Number:
		i, err := t.Int64()
		if err != nil {
			f, _ := t.Float64()
			return int64(f), true
		}
		return i, true
	case string:
		n, err := strconv.ParseInt(t, 10, 64)
		if err == nil {
			return n, true
		}
	}
	return 0, false
}
