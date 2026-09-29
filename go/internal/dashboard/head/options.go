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

package head

import (
	"fmt"
	"net/http"
	"sort"
	"strconv"
)

// State API constants, aligned with python/ray/util/state/common.py.
const (
	defaultListLimit = 100
	maxListLimit     = 10000
	defaultTimeoutS  = 30
)

// ListApiOptions mirrors the Python ListApiOptions used by state APIs.
type ListApiOptions struct {
	Limit            int
	Timeout          int
	Detail           bool
	ExcludeDriver    bool
	FilterKeys       []string
	FilterPredicates []string
	FilterValues     []string
	// Resource names the schema used for column filtering. When non-empty the
	// returned entries are trimmed to the schema's base (or detail) columns,
	// aligned with filter_fields in python/ray/util/state/common.py.
	Resource string
}

// SummaryApiOptions mirrors the Python SummaryApiOptions used by the summarize
// state APIs (tasks/summarize etc.).
type SummaryApiOptions struct {
	Timeout          int
	SummaryBy        string
	FilterKeys       []string
	FilterPredicates []string
	FilterValues     []string
}

// ParseSummaryApiOptions parses summary options from the request query, aligned
// with summary_options_from_req in python/ray/dashboard/state_api_utils.py.
func ParseSummaryApiOptions(r *http.Request) (*SummaryApiOptions, error) {
	q := r.URL.Query()
	opt := &SummaryApiOptions{
		Timeout:   defaultTimeoutS,
		SummaryBy: q.Get("summary_by"),
	}
	if v := q.Get("timeout"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, newValueError("invalid timeout %q", v)
		}
		opt.Timeout = n
	}
	opt.FilterKeys = q["filter_keys"]
	opt.FilterPredicates = q["filter_predicates"]
	opt.FilterValues = q["filter_values"]
	return opt, nil
}

// ParseListApiOptions parses options from the request query, aligned with
// options_from_req in python/ray/dashboard/state_api_utils.py. Filter
// parameters may be repeated in the query and are collected into parallel
// arrays (filter_keys[i] / filter_predicates[i] / filter_values[i]).
func ParseListApiOptions(r *http.Request) (*ListApiOptions, error) {
	q := r.URL.Query()
	opt := &ListApiOptions{
		Limit:         defaultListLimit,
		Timeout:       defaultTimeoutS,
		ExcludeDriver: true,
	}
	if v := q.Get("limit"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, newValueError("invalid limit %q", v)
		}
		if n < 0 {
			// A negative limit would slice with a negative index in filterAndSort
			// (len(filtered) > opt.Limit is always true for n < 0), panicking
			// every list API. Python rejects it at islice with a ValueError;
			// reject it here for the same controlled-error behavior.
			return nil, newValueError("limit cannot be negative: %d", n)
		}
		if n > maxListLimit {
			return nil, newValueError(
				"Given limit %d exceeds the supported limit %d. Use a lower limit, or set the `RAY_MAX_LIMIT_FROM_API_SERVER` environment variable to a larger value.",
				n, maxListLimit,
			)
		}
		opt.Limit = n
	}
	if v := q.Get("timeout"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil {
			return nil, newValueError("invalid timeout %q", v)
		}
		opt.Timeout = n
	}
	if v := q.Get("detail"); v != "" {
		b, err := parseBoolParam(v)
		if err != nil {
			return nil, newValueError("invalid detail %q: must be True/true/1 or False/false/0", v)
		}
		opt.Detail = b
	}
	if v := q.Get("exclude_driver"); v != "" {
		b, err := parseBoolParam(v)
		if err != nil {
			return nil, newValueError("invalid exclude_driver %q: must be True/true/1 or False/false/0", v)
		}
		opt.ExcludeDriver = b
	}
	opt.FilterKeys = q["filter_keys"]
	opt.FilterPredicates = q["filter_predicates"]
	opt.FilterValues = q["filter_values"]
	// Validate every filter predicate, aligned with the ListApiOptions
	// __post_init__ check in python/ray/util/state/common.py which raises a
	// ValueError for any predicate other than "=" and "!=".
	for _, pred := range opt.FilterPredicates {
		if pred != "=" && pred != "!=" {
			return nil, newValueError(
				"Unsupported filter predicate %s is given. Available predicates: =, !=.", pred,
			)
		}
	}
	// The server-side timeout is 80% of the user timeout so the response can be
	// delivered before the HTTP timeout, aligned with the
	// server_timeout_multiplier applied in ListApiOptions.__post_init__.
	opt.Timeout = max(1, int(float64(opt.Timeout)*0.8))
	return opt, nil
}

// parseBoolParam converts a query boolean to a bool, aligned with
// convert_string_to_type in python/ray/util/state/util.py which accepts
// True/true/1 and False/false/0.
func parseBoolParam(v string) (bool, error) {
	switch v {
	case "True", "true", "1":
		return true, nil
	case "False", "false", "0":
		return false, nil
	}
	return false, fmt.Errorf("invalid boolean %q", v)
}

// filterAndSort filters by the option's filters, sorts deterministically and
// truncates to the limit, aligned with do_filter + sort + islice in
// python/ray/dashboard/state_aggregator.py.
func filterAndSort(entries []map[string]interface{}, opt *ListApiOptions) []map[string]interface{} {
	filtered := applyFilters(entries, opt)
	if len(filtered) > 1 {
		sortEntries(filtered, opt.Resource)
	}
	if len(filtered) > opt.Limit {
		filtered = filtered[:opt.Limit]
	}
	return filtered
}

// applyFilters returns the entries that satisfy every filter. When no filter
// keys are given, the input slice is returned unchanged.
func applyFilters(entries []map[string]interface{}, opt *ListApiOptions) []map[string]interface{} {
	if len(opt.FilterKeys) == 0 || len(entries) == 0 {
		return entries
	}
	filtered := make([]map[string]interface{}, 0, len(entries))
	for _, e := range entries {
		if matchesFilters(e, opt) {
			filtered = append(filtered, e)
		}
	}
	return filtered
}

// ValueError mirrors the Python ValueError raised by options_from_req/do_filter
// for invalid query parameters. Handlers map it to HTTP 400 BAD_REQUEST,
// matching the Python state API.
type ValueError struct{ msg string }

func (e *ValueError) Error() string { return e.msg }

func newValueError(format string, args ...interface{}) error {
	return &ValueError{msg: fmt.Sprintf(format, args...)}
}

// filterableColumns lists the columns accepted by filter_keys for each
// resource, aligned with the filterable_columns of the Python state schemas in
// python/ray/util/state/common.py.
var filterableColumns = map[string][]string{
	"actors":           {"actor_id", "class_name", "state", "job_id", "name", "node_id", "pid", "ray_namespace", "placement_group_id", "repr_name"},
	"nodes":            {"node_id", "node_ip", "is_head_node", "state", "node_name"},
	"workers":          {"worker_id", "is_alive", "worker_type", "exit_type", "node_id", "ip", "pid", "debugger_port", "num_paused_threads"},
	"jobs":             {"job_id", "type", "status", "submission_id"},
	"placement_groups": {"placement_group_id", "name", "creator_job_id", "state", "is_detached"},
	"tasks":            {"task_id", "attempt_number", "name", "state", "job_id", "actor_id", "type", "func_or_class_name", "parent_task_id", "node_id", "worker_id", "worker_pid", "error_type", "language", "placement_group_id", "is_debugger_paused"},
	"runtime_envs":     {"runtime_env", "success", "node_id", "error"},
}

// schemaColumns defines the base and detail columns of each resource schema,
// aligned with the state_column(detail=...) annotations in python/ray/util/
// state/common.py. base is the column set emitted when detail == False and
// detail is the full column set emitted when detail == True (base plus the
// detail-only columns). The column names match the snake_case keys produced by
// protoMessageToDict (preserving_proto_field_name=True).
type schemaColumns struct {
	base   []string
	detail []string
}

var resourceColumns = map[string]schemaColumns{
	"actors": {
		base: []string{
			"actor_id", "class_name", "state", "job_id", "name", "node_id", "pid", "ray_namespace",
		},
		detail: []string{
			"actor_id", "class_name", "state", "job_id", "name", "node_id", "pid", "ray_namespace",
			"serialized_runtime_env", "required_resources", "death_cause", "is_detached",
			"placement_group_id", "repr_name", "num_restarts",
			"num_restarts_due_to_lineage_reconstruction", "num_restarts_due_to_node_preemption",
			"call_site", "label_selector",
		},
	},
	"placement_groups": {
		base: []string{"placement_group_id", "name", "creator_job_id", "state"},
		detail: []string{
			"placement_group_id", "name", "creator_job_id", "state",
			"bundles", "is_detached", "stats",
		},
	},
	"nodes": {
		base: []string{
			"node_id", "node_ip", "is_head_node", "state", "state_message", "node_name",
			"resources_total", "labels",
		},
		detail: []string{
			"node_id", "node_ip", "is_head_node", "state", "state_message", "node_name",
			"resources_total", "labels", "start_time_ms", "end_time_ms",
		},
	},
	"jobs": {
		base: []string{
			"job_id", "submission_id", "entrypoint", "type", "status", "message", "error_type",
			"driver_info",
		},
		// Aligned with JobDetails.model_fields in pydantic_models.py.
		detail: []string{
			"type", "job_id", "submission_id", "driver_info", "status", "entrypoint",
			"message", "error_type", "start_time", "end_time", "metadata", "runtime_env",
			"driver_agent_http_address", "driver_node_id", "driver_exit_code",
		},
	},
	"workers": {
		base: []string{
			"worker_id", "is_alive", "worker_type", "exit_type", "node_id", "ip", "pid",
		},
		detail: []string{
			"worker_id", "is_alive", "worker_type", "exit_type", "node_id", "ip", "pid",
			"exit_detail", "worker_launch_time_ms", "worker_launched_time_ms",
			"start_time_ms", "end_time_ms", "debugger_port", "num_paused_threads",
		},
	},
	"tasks": {
		base: []string{
			"task_id", "attempt_number", "name", "state", "job_id", "actor_id", "type",
			"func_or_class_name", "parent_task_id", "node_id", "worker_id", "worker_pid",
			"error_type",
		},
		detail: []string{
			"task_id", "attempt_number", "name", "state", "job_id", "actor_id", "type",
			"func_or_class_name", "parent_task_id", "node_id", "worker_id", "worker_pid",
			"error_type", "language", "required_resources", "runtime_env_info",
			"placement_group_id", "events", "profiling_data", "creation_time_ms",
			"start_time_ms", "end_time_ms", "task_log_info", "error_message",
			"is_debugger_paused", "call_site", "label_selector",
		},
	},
	"objects": {
		// ObjectState has no detail-only columns; base == detail.
		base: []string{
			"object_id", "object_size", "task_status", "attempt_number", "reference_type",
			"call_site", "type", "pid", "ip",
		},
		detail: []string{
			"object_id", "object_size", "task_status", "attempt_number", "reference_type",
			"call_site", "type", "pid", "ip",
		},
	},
	"runtime_envs": {
		base:   []string{"runtime_env", "success", "creation_time_ms", "node_id"},
		detail: []string{"runtime_env", "success", "creation_time_ms", "node_id", "ref_cnt", "error"},
	},
	"cluster_events": {
		base:   []string{"severity", "time", "source_type", "message", "event_id"},
		detail: []string{"severity", "time", "source_type", "message", "event_id", "custom_fields"},
	},
}

// filterFields trims the entry to the schema's base (or detail) columns and
// fills any missing column with nil, aligned with filter_fields in
// python/ray/util/state/common.py. When the resource schema is unknown the
// entry is returned unchanged.
func filterFields(d map[string]interface{}, opt *ListApiOptions) map[string]interface{} {
	cols, ok := resourceColumns[opt.Resource]
	if !ok {
		return d
	}
	selected := cols.base
	if opt.Detail {
		selected = cols.detail
	}
	out := make(map[string]interface{}, len(selected))
	for _, col := range selected {
		if v, ok := d[col]; ok {
			out[col] = v
		} else {
			out[col] = nil
		}
	}
	return out
}

// validateFilters checks every filter key against the resource's filterable
// columns and returns a ValueError-style error aligned with the Python
// do_filter, which surfaces as a 400 BAD_REQUEST.
func validateFilters(opt *ListApiOptions, resource string) error {
	cols, ok := filterableColumns[resource]
	if !ok {
		return nil
	}
	for _, k := range opt.FilterKeys {
		if !stringInSlice(k, cols) {
			return newValueError(
				"The given filter column %s is not supported. Enter filters with --filter key=value or --filter key!=value. Supported filter columns: %v",
				k, cols,
			)
		}
	}
	return nil
}

// matchesFilters reports whether the entry satisfies every filter, aligned with
// the Python do_filter behavior: string fields match case-insensitively, while
// bool/int/float entry values convert the filter string to the matching type
// before comparison (convert_filters_type + the typed branches in do_filter).
// A filter over a missing column yields no match.
func matchesFilters(e map[string]interface{}, opt *ListApiOptions) bool {
	for i, k := range opt.FilterKeys {
		pred := "="
		if i < len(opt.FilterPredicates) {
			pred = opt.FilterPredicates[i]
		}
		want := ""
		if i < len(opt.FilterValues) {
			want = opt.FilterValues[i]
		}
		got, ok := e[k]
		if !ok {
			return false
		}
		match, err := filterValueMatch(got, want)
		if err != nil {
			// A filter value that cannot be converted to the column type is
			// treated as non-matching (the Python convert raises and the whole
			// filter is rejected; here we conservatively exclude the entry).
			return false
		}
		if pred == "=" && !match {
			return false
		}
		if pred == "!=" && match {
			return false
		}
	}
	return true
}

// filterValueMatch compares a datum value against a string filter value using
// the datum's runtime type, aligned with do_filter in state_api_utils.py.
func filterValueMatch(got interface{}, want string) (bool, error) {
	switch t := got.(type) {
	case bool:
		w, err := parseBoolParam(want)
		if err != nil {
			return false, err
		}
		return t == w, nil
	case int:
		w, err := strconv.Atoi(want)
		if err != nil {
			return false, err
		}
		return t == w, nil
	case int64:
		w, err := strconv.ParseInt(want, 10, 64)
		if err != nil {
			return false, err
		}
		return t == w, nil
	case float64:
		w, err := strconv.ParseFloat(want, 64)
		if err != nil {
			return false, err
		}
		return t == w, nil
	case string:
		return stringsEqualFold(t, want), nil
	default:
		// Unknown datum type: fall back to string equality.
		return stringsEqualFold(fmt.Sprintf("%v", got), want), nil
	}
}

func stringsEqualFold(a, b string) bool {
	return len(a) == len(b) && lowerString(a) == lowerString(b)
}

func lowerString(s string) string {
	b := []byte(s)
	for i := range b {
		if b[i] >= 'A' && b[i] <= 'Z' {
			b[i] += 'a' - 'A'
		}
	}
	return string(b)
}

// sortEntries sorts deterministically by the entry's primary id field, aligned
// with the Python `result.sort(key=lambda entry: entry["<id>"])`. Jobs sort by
// job_id with a missing/empty id first, matching `entry["job_id"] or ""` in
// list_jobs. Every other resource uses its hard-coded primary id field.
func sortEntries(entries []map[string]interface{}, resource string) {
	key := idSortKey(resource)
	sort.SliceStable(entries, func(i, j int) bool {
		return sortKeyString(entries[i][key]) < sortKeyString(entries[j][key])
	})
}

// sortKeyString renders a sort key as a string, treating nil as the empty
// string so a missing id sorts first (matching `or ""` in Python).
func sortKeyString(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprintf("%v", v)
}

// idSortKey returns the primary sort key for a resource, aligned with the
// hard-coded sort keys in the Python list_* methods of state_aggregator.py:
// tasks sort by task_id, workers by worker_id, actors by actor_id, nodes by
// node_id, placement groups by placement_group_id, jobs by job_id and objects
// by object_id.
func idSortKey(resource string) string {
	switch resource {
	case "jobs":
		return "job_id"
	case "tasks":
		return "task_id"
	case "workers":
		return "worker_id"
	case "actors":
		return "actor_id"
	case "nodes":
		return "node_id"
	case "placement_groups":
		return "placement_group_id"
	case "objects":
		return "object_id"
	}
	return "id"
}
