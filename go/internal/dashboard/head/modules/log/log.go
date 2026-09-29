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

// Package log implements the LogsManager-backed state log APIs (/api/v0/logs
// and /api/v0/logs/file), aligned with the Python LogsManager + list_logs /
// get_logs handlers in python/ray/dashboard/modules/log/log_manager.py and
// python/ray/dashboard/modules/state/state_head.py. Log files are listed and
// streamed through each node's dashboard agent LogService gRPC endpoint, whose
// address is read from the GCS internal KV store (namespace "dashboard").
package log

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// KV constants aligned with ray/_private/ray_constants.py and
// python/ray/dashboard/consts.py.
const (
	// kvNamespaceDashboard is KV_NAMESPACE_DASHBOARD.
	kvNamespaceDashboard = "dashboard"
	// dashboardAgentAddrNodeIDPrefix is DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX.
	dashboardAgentAddrNodeIDPrefix = "DASHBOARD_AGENT_ADDR_NODE_ID_PREFIX:"
	// dashboardAgentAddrIPPrefix is DASHBOARD_AGENT_ADDR_IP_PREFIX.
	dashboardAgentAddrIPPrefix = "DASHBOARD_AGENT_ADDR_IP_PREFIX:"
	// defaultRPCTimeout mirrors DEFAULT_RPC_TIMEOUT from
	// python/ray/util/state/common.py.
	defaultRPCTimeout = 30
	// defaultLogLimit mirrors DEFAULT_LOG_LIMIT from
	// python/ray/util/state/common.py.
	defaultLogLimit = 1000
	// logGRPCError is the initial-metadata key used by the log agent's StreamLog
	// to surface a non-standard gRPC error (e.g. a missing log file) to the
	// client. It mirrors LOG_GRPC_ERROR in log_consts.py; the Python client
	// (state_manager.stream_log) reads it and raises, and the Go client must do
	// the same so a missing file returns 500 instead of an empty 200 body.
	logGRPCError = "log_grpc_status"
)

// LogHead implements the /api/v0/logs and /api/v0/logs/{media_type} routes.
type LogHead struct {
	cfg    *head.HeadConfig
	client *head.GCSClient
	sem    chan struct{}
}

// New creates a LogHead backed by the given GCS client.
func New(cfg *head.HeadConfig, client *head.GCSClient) *LogHead {
	return &LogHead{
		cfg:    cfg,
		client: client,
		sem:    make(chan struct{}, 100),
	}
}

// Name returns the module name.
func (l *LogHead) Name() string { return "LogHead" }

// Start has no background tasks.
func (l *LogHead) Start(ctx context.Context) error { return nil }

// Healthy reports the module health.
func (l *LogHead) Healthy() bool { return true }

// RegisterHTTP registers the log routes. The Python dashboard mounts the logs
// handlers on the StateHead module, so a LogHead may also be held (and
// registered) by the StateHead; registering the same pattern twice panics the
// ServeMux, so each pattern is registered at most once per mux and later
// duplicates are skipped.
func (l *LogHead) RegisterHTTP(mux *http.ServeMux) error {
	registerLogRoutes(mux, l.handleListLogs, l.handleGetLogs)
	return nil
}

// registerLogRoutes registers the log routes idempotently: a duplicate
// registration panics the ServeMux, which is recovered and ignored so the
// already-registered handler (from the StateHead-held LogHead) stays in place.
func registerLogRoutes(mux *http.ServeMux, list, get http.HandlerFunc) {
	for _, r := range []struct {
		pattern string
		h       http.HandlerFunc
	}{
		{"GET /api/v0/logs", list},
		{"GET /api/v0/logs/{media_type}", get},
	} {
		func() {
			defer func() { _ = recover() }()
			mux.HandleFunc(r.pattern, r.h)
		}()
	}
}

// acquire limits concurrent requests, aligned with
// RateLimitedModule.enforce_max_concurrent_calls.
func (l *LogHead) acquire() bool {
	select {
	case l.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

func (l *LogHead) release() { <-l.sem }

// agentAddrs is the parsed [ip, http_port, grpc_port] value stored under the
// agent address KV keys.
type agentAddrs struct {
	ip       string
	httpPort int
	grpcPort int
}

// getAgentAddrByNodeID reads the agent address of a node from the GCS internal
// KV (namespace "dashboard"), aligned with the Python get_log_service_stub /
// reporter's get_agent_addr_by_node_id.
func (l *LogHead) getAgentAddrByNodeID(ctx context.Context, nodeIDHex string) (*agentAddrs, error) {
	return l.readAgentAddr(ctx, dashboardAgentAddrNodeIDPrefix+nodeIDHex, "node "+nodeIDHex)
}

// getAgentAddrByIP reads the agent address by ip, whose value is a JSON array
// [node_id, http_port, grpc_port].
func (l *LogHead) getAgentAddrByIP(ctx context.Context, ip string) (*agentAddrs, error) {
	reply, err := l.client.InternalKVGet(ctx, kvNamespaceDashboard, dashboardAgentAddrIPPrefix+ip)
	if err != nil {
		return nil, err
	}
	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("no agent address found for ip %s", ip)
	}
	var vals []interface{}
	if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) < 3 {
		return nil, fmt.Errorf("invalid agent address %q", string(reply.Value))
	}
	return &agentAddrs{
		ip:       ip,
		httpPort: int(floatVal(vals[1])),
		grpcPort: int(floatVal(vals[2])),
	}, nil
}

func (l *LogHead) readAgentAddr(ctx context.Context, key, desc string) (*agentAddrs, error) {
	reply, err := l.client.InternalKVGet(ctx, kvNamespaceDashboard, key)
	if err != nil {
		return nil, err
	}
	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("no agent address found for %s", desc)
	}
	var vals []interface{}
	if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) < 3 {
		return nil, fmt.Errorf("invalid agent address %q", string(reply.Value))
	}
	return &agentAddrs{
		ip:       stringVal(vals[0]),
		httpPort: int(floatVal(vals[1])),
		grpcPort: int(floatVal(vals[2])),
	}, nil
}

// resolveNodeID returns the node id hex for the given node_id / node_ip query
// params, aligned with the Python list_logs handler. A missing node_id falls
// back to resolving the ip; an error is returned when neither is provided or
// the ip cannot be resolved.
func (l *LogHead) resolveNodeID(ctx context.Context, nodeID, nodeIP string) (string, error) {
	if nodeID == "" && nodeIP == "" {
		return "", errors.New("Both node id and node ip are not provided. Please provide at least one of them.")
	}
	if nodeID == "" {
		// The ip-keyed KV value is [node_id, http_port, grpc_port].
		reply, err := l.client.InternalKVGet(ctx, kvNamespaceDashboard, dashboardAgentAddrIPPrefix+nodeIP)
		if err != nil || len(reply.Value) == 0 {
			return "", fmt.Errorf("Cannot find matching node_id for a given node ip %s", nodeIP)
		}
		var vals []interface{}
		if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) == 0 {
			return "", fmt.Errorf("Cannot find matching node_id for a given node ip %s", nodeIP)
		}
		return stringVal(vals[0]), nil
	}
	return nodeID, nil
}

// handleListLogs serves GET /api/v0/logs?node_id=...&glob=..., returning the
// log files on the node categorized by component, aligned with the Python
// list_logs handler (do_reply, snake_case keys).
func (l *LogHead) handleListLogs(w http.ResponseWriter, r *http.Request) {
	if !l.acquire() {
		http.Error(w, "Too many concurrent requests", http.StatusTooManyRequests)
		return
	}
	defer l.release()
	q := r.URL.Query()
	nodeID := q.Get("node_id")
	nodeIP := q.Get("node_ip")
	nodeID, err := l.resolveNodeID(r.Context(), nodeID, nodeIP)
	if err != nil {
		// Neither node_id nor node_ip is provided -> 400; an unresolvable
		// node_ip -> 404, aligned with the Python list_logs handler.
		status := http.StatusBadRequest
		if nodeIP != "" {
			status = http.StatusNotFound
		}
		// Python do_reply(result=None) nests the None under data.result
		// (data: {"result": null}), so pass an explicit {"result": nil} rather
		// than relying on the writeRESTResponse nil->{} fallback.
		head.RESTResponse(w, status, err.Error(), map[string]interface{}{"result": nil})
		return
	}
	glob := q.Get("glob")
	if glob == "" {
		glob = "*"
	}
	timeout := defaultRPCTimeout
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeout = n
		}
	}
	files, err := l.listLogs(r.Context(), nodeID, glob, timeout)
	if err != nil {
		head.RESTResponse(w, http.StatusInternalServerError, err.Error(), map[string]interface{}{"result": nil})
		return
	}
	data := make(map[string]interface{}, len(files))
	for k, v := range files {
		data[k] = v
	}
	// do_reply(result=<categorized dict>) nests the dict under data.result,
	// matching the response shape the frontend reads (resp.data.data.result).
	head.RESTResponse(w, http.StatusOK, "", map[string]interface{}{"result": data})
}

// handleGetLogs serves GET /api/v0/logs/{media_type}, streaming the log file
// content, aligned with the Python get_logs handler. media_type is "file"
// (return the whole log then close) or "stream" (keep the connection and push
// new lines), matching GetLogOptions.media_type.
func (l *LogHead) handleGetLogs(w http.ResponseWriter, r *http.Request) {
	if !l.acquire() {
		http.Error(w, "Too many concurrent requests", http.StatusTooManyRequests)
		return
	}
	defer l.release()
	mediaType := r.PathValue("media_type")
	if mediaType == "" {
		mediaType = "file"
	}
	if mediaType != "file" && mediaType != "stream" {
		// Python GetLogOptions.__post_init__ raises ValueError for an unknown
		// media type, which bubbles out of the get_logs handler uncaught and
		// surfaces as a 500 (aiohttp default for unhandled exceptions), so the
		// Go handler returns 500 too.
		http.Error(w, fmt.Sprintf("Invalid media type: %s", mediaType), http.StatusInternalServerError)
		return
	}
	q := r.URL.Query()
	nodeID := q.Get("node_id")
	filename := q.Get("filename")
	suffix := q.Get("suffix")
	if suffix == "" {
		suffix = "out"
	}
	if actorID := q.Get("actor_id"); actorID != "" && nodeID == "" {
		// Resolve the actor's node id and worker log file, aligned with
		// LogsManager._resolve_actor_filename: the actor's worker_id + node_id
		// are matched against the worker log files on the node.
		resolvedNode, resolvedFile, err := l.resolveActorLogFile(r.Context(), actorID, suffix)
		if err != nil {
			http.Error(w, err.Error(), http.StatusNotFound)
			return
		}
		nodeID = resolvedNode
		filename = resolvedFile
	}
	if nodeID == "" {
		// Resolve the node id from the node ip, aligned with
		// LogsManager.stream_logs (ip_to_node_id fallback).
		ip := q.Get("node_ip")
		if ip == "" {
			http.Error(w, "node_id, node_ip or actor_id is required", http.StatusBadRequest)
			return
		}
		reply, err := l.client.InternalKVGet(r.Context(), kvNamespaceDashboard, dashboardAgentAddrIPPrefix+ip)
		if err != nil || len(reply.Value) == 0 {
			http.Error(w, fmt.Sprintf("Cannot find matching node_id for a given node ip %s", ip), http.StatusNotFound)
			return
		}
		var vals []interface{}
		if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) == 0 {
			http.Error(w, fmt.Sprintf("Cannot find matching node_id for a given node ip %s", ip), http.StatusNotFound)
			return
		}
		nodeID = stringVal(vals[0])
	}
	if nodeID == "" {
		http.Error(w, "node_id, node_ip or actor_id is required", http.StatusBadRequest)
		return
	}
	if filename == "" {
		http.Error(w, "filename is required", http.StatusBadRequest)
		return
	}
	lines := defaultLogLimit
	if v := q.Get("lines"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			lines = n
		}
	}
	timeout := defaultRPCTimeout
	if v := q.Get("timeout"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			timeout = n
		}
	}
	// Python parses the query param with lower()=="true"; EqualFold matches that
	// case-insensitive behavior for "true"/"True"/"TRUE".
	filterANSI := strings.EqualFold(q.Get("filter_ansi_code"), "true")
	keepAlive := mediaType == "stream"
	var interval float32
	if v := q.Get("interval"); v != "" {
		f, _ := strconv.ParseFloat(v, 64)
		interval = float32(f)
	}
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	// The download filename defaults to DEFAULT_DOWNLOAD_FILENAME ("file.txt")
	// when not specified, matching GetLogOptions.download_filename.
	downloadFilename := q.Get("download_filename")
	if downloadFilename == "" {
		downloadFilename = "file.txt"
	}
	w.Header().Set("Content-Disposition", fmt.Sprintf(`attachment; filename="%s"`, downloadFilename))
	// Aligned with the Python state_head.py get_logs handler
	// (logger.info("Streaming logs with options: ...")) and log_manager.py
	// (logger.info("Resolved log file: ...")).
	log.Log.Info("streaming logs", "media_type", mediaType, "node_id", nodeID, "filename", filename,
		"lines", lines, "filter_ansi_code", filterANSI)
	log.Log.Info("resolved log file", "node_id", nodeID, "filename", filename)
	if err := l.streamLogFile(r.Context(), w, nodeID, filename, lines, timeout, keepAlive, interval, filterANSI); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}

// resolveActorLogFile returns the node id and worker log filename for an actor,
// aligned with LogsManager._resolve_actor_filename + _resolve_worker_file in
// python/ray/dashboard/modules/log/log_manager.py: the actor's worker id is
// matched against the worker-*.out/err files listed on its node.
func (l *LogHead) resolveActorLogFile(ctx context.Context, actorID, suffix string) (string, string, error) {
	reply, err := l.client.GetAllActorInfo(ctx, &proto.GetAllActorInfoRequest{})
	if err != nil {
		return "", "", err
	}
	var actor *proto.ActorTableData
	for _, a := range reply.ActorTableData {
		if hexEncode(a.ActorId) == actorID {
			actor = a
			break
		}
	}
	if actor == nil {
		return "", "", fmt.Errorf("Actor ID %s not found.", actorID)
	}
	if actor.Address == nil || len(actor.Address.WorkerId) == 0 {
		return "", "", fmt.Errorf("Worker ID for Actor ID %s not found. Actor is not scheduled yet.", actorID)
	}
	if actor.Address == nil || len(actor.Address.NodeId) == 0 {
		return "", "", fmt.Errorf("Node ID for Actor ID %s not found. Actor is not scheduled yet.", actorID)
	}
	nodeID := hexEncode(actor.Address.NodeId)
	workerID := hexEncode(actor.Address.WorkerId)

	// List worker logs on the node and match the worker id, aligned with
	// _resolve_worker_file (worker logs look like worker-[worker_id]-[job_id]-[pid].out).
	logFiles, err := l.listLogs(ctx, nodeID, fmt.Sprintf("*%s*%s", workerID, suffix), defaultRPCTimeout)
	if err != nil {
		return "", "", err
	}
	for _, f := range append(append([]string{}, logFiles["worker_out"]...), logFiles["worker_err"]...) {
		// WORKER_LOG_PATTERN: worker-([0-9a-f]+)-([0-9a-f]+)-(\d+).(out|err)
		fields := strings.Split(f, "-")
		if len(fields) >= 2 && fields[1] == workerID {
			return nodeID, f, nil
		}
	}
	return "", "", fmt.Errorf("Could not find a log file for the actor %s", actorID)
}

// hexEncode encodes a byte slice as a lowercase hex string, returning "" for nil.
func hexEncode(b []byte) string {
	if len(b) == 0 {
		return ""
	}
	const digits = "0123456789abcdef"
	out := make([]byte, len(b)*2)
	for i, c := range b {
		out[i*2] = digits[c>>4]
		out[i*2+1] = digits[c&0xf]
	}
	return string(out)
}

// listLogs queries the node agent for the log files matching the glob and
// categorizes them by component, aligned with LogsManager.list_logs +
// _categorize_log_files.
func (l *LogHead) listLogs(ctx context.Context, nodeID, glob string, timeout int) (map[string][]string, error) {
	addrs, err := l.getAgentAddrByNodeID(ctx, nodeID)
	if err != nil {
		return nil, err
	}
	if addrs.grpcPort <= 0 {
		return nil, fmt.Errorf("agent on node %s has no gRPC server", nodeID)
	}
	conn, err := grpc.DialContext(ctx, buildAddress(addrs.ip, addrs.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	ctxTimeout, cancel := context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
	defer cancel()
	reply, err := proto.NewLogServiceClient(conn).ListLogs(ctxTimeout, &proto.ListLogsRequest{GlobFilter: glob})
	if err != nil {
		return nil, err
	}
	return categorizeLogFiles(reply.LogFiles), nil
}

// streamLogFile streams the requested log file from the node agent to w,
// aligned with LogsManager.stream_logs. With keepAlive (media_type "stream")
// the gRPC stream stays open and the agent keeps pushing new lines on the given
// poll interval; with keepAlive=false (media_type "file") it streams the whole
// log and terminates on EOF. For keepAlive streams no RPC timeout is applied so
// the connection is not force-terminated, matching the Python
// `timeout=None if keep_alive else options.timeout`.
func (l *LogHead) streamLogFile(ctx context.Context, w io.Writer, nodeID, filename string, lines, timeout int, keepAlive bool, interval float32, filterANSI bool) error {
	addrs, err := l.getAgentAddrByNodeID(ctx, nodeID)
	if err != nil {
		return err
	}
	if addrs.grpcPort <= 0 {
		return fmt.Errorf("agent on node %s has no gRPC server", nodeID)
	}
	conn, err := grpc.DialContext(ctx, buildAddress(addrs.ip, addrs.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer conn.Close()
	var streamCtx context.Context
	var cancel context.CancelFunc
	if keepAlive {
		streamCtx = ctx
	} else {
		streamCtx, cancel = context.WithTimeout(ctx, time.Duration(timeout)*time.Second)
		defer cancel()
	}
	lines32 := int32(lines)
	stream, err := proto.NewLogServiceClient(conn).StreamLog(streamCtx, &proto.StreamLogRequest{
		LogFileName: filename,
		KeepAlive:   keepAlive,
		Lines:       &lines32,
		Interval:    &interval,
	})
	if err != nil {
		return err
	}
	// The agent reports a missing log file through the initial metadata
	// (log_grpc_status), aligned with log_agent.py sending
	// send_initial_metadata([[LOG_GRPC_ERROR, str(e)]]) on FileNotFoundError and
	// state_manager.stream_log raising ValueError on it. Read the header before
	// the first Recv so a missing file surfaces the agent's message as an error
	// (-> HTTP 500) instead of an empty 200 body.
	if md, err := stream.Header(); err == nil {
		if vals := md.Get(logGRPCError); len(vals) > 0 {
			return errors.New(vals[0])
		}
	}
	var buf []byte
	for {
		reply, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			return err
		}
		if filterANSI {
			buf = append(buf, stripANSI(reply.Data)...)
			// Flush on newline to bound memory for large files.
			if idx := lastNewline(buf); idx >= 0 {
				if _, err := w.Write(buf[:idx+1]); err != nil {
					return err
				}
				buf = buf[idx+1:]
			}
		} else {
			if _, err := w.Write(reply.Data); err != nil {
				return err
			}
		}
	}
	if filterANSI && len(buf) > 0 {
		_, err = w.Write(buf)
	}
	return err
}

// categorizeLogFiles groups the log files by component, aligned with
// LogsManager._categorize_log_files.
func categorizeLogFiles(files []string) map[string][]string {
	result := map[string][]string{}
	for _, f := range files {
		switch {
		case strings.Contains(f, "worker") && strings.HasSuffix(f, ".out"):
			result["worker_out"] = append(result["worker_out"], f)
		case strings.Contains(f, "worker") && strings.HasSuffix(f, ".err"):
			result["worker_err"] = append(result["worker_err"], f)
		case strings.Contains(f, "core-worker") && strings.HasSuffix(f, ".log"):
			result["core_worker"] = append(result["core_worker"], f)
		case strings.Contains(f, "core-driver") && strings.HasSuffix(f, ".log"):
			result["driver"] = append(result["driver"], f)
		case strings.Contains(f, "raylet."):
			result["raylet"] = append(result["raylet"], f)
		case strings.Contains(f, "gcs_server."):
			result["gcs_server"] = append(result["gcs_server"], f)
		case strings.Contains(f, "log_monitor"):
			result["internal"] = append(result["internal"], f)
		case strings.Contains(f, "monitor"):
			result["autoscaler"] = append(result["autoscaler"], f)
		case strings.Contains(f, "agent."):
			result["agent"] = append(result["agent"], f)
		case strings.Contains(f, "dashboard."):
			result["dashboard"] = append(result["dashboard"], f)
		default:
			result["internal"] = append(result["internal"], f)
		}
	}
	return result
}

// buildAddress joins host and port into host:port.
func buildAddress(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

// stringVal returns a string for the JSON decoded value, defaulting to "".
func stringVal(v interface{}) string {
	if s, ok := v.(string); ok {
		return s
	}
	return ""
}

// floatVal returns the float64 of a JSON decoded number, defaulting to 0.
func floatVal(v interface{}) float64 {
	switch t := v.(type) {
	case float64:
		return t
	case int:
		return float64(t)
	}
	return 0
}

// lastNewline returns the index of the last newline in b, or -1.
func lastNewline(b []byte) int {
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] == '\n' {
			return i
		}
	}
	return -1
}

// stripANSI removes ANSI escape sequences from b, aligned with the Python
// ANSI_ESC_PATTERN filtering in the get_logs handler.
func stripANSI(b []byte) []byte {
	out := b[:0]
	for i := 0; i < len(b); i++ {
		if b[i] == 0x1b {
			// Skip the escape sequence: ESC [ ... final-byte (0x40-0x7e).
			j := i + 1
			for j < len(b) && (b[j] == '[' || (b[j] >= '0' && b[j] <= '?') || (b[j] >= ' ' && b[j] <= '/')) {
				j++
			}
			if j < len(b) && b[j] >= 0x40 && b[j] <= 0x7e {
				j++
			}
			i = j - 1
			continue
		}
		out = append(out, b[i])
	}
	return out
}
