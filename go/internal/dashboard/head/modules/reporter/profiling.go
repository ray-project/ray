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

package reporter

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"

	"github.com/ray-project/ray/go/internal/dashboard/head"
	"github.com/ray-project/ray/go/pkg/log"
	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// agentAddrs holds the decoded agent address tuple (ip, http_port, grpc_port)
// from the dashboard KV.
type agentAddrs struct {
	ip       string
	httpPort int
	grpcPort int
}

// getAgentAddrByNodeID resolves the agent address for a node, aligned with
// ReportHead._get_stub_address_by_node_id: the KV value is the JSON array
// [ip, http_port, grpc_port].
func (r *ReportHead) getAgentAddrByNodeID(ctx context.Context, nodeIDHex string) (*agentAddrs, error) {
	reply, err := r.client.InternalKVGet(ctx, kvNamespaceDashboard, dashboardAgentAddrNodeIDPrefix+nodeIDHex)
	if err != nil {
		return nil, err
	}
	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("no agent address found for node %s", nodeIDHex)
	}
	var vals []interface{}
	if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) < 3 {
		return nil, fmt.Errorf("invalid agent address %q", string(reply.Value))
	}
	return &agentAddrs{
		ip:       asString2(vals[0]),
		httpPort: int(toFloat(vals[1])),
		grpcPort: int(toFloat(vals[2])),
	}, nil
}

// getAgentAddrByIP resolves the agent address by node IP, aligned with
// ReportHead._get_stub_address_by_ip: the KV value is the JSON array
// [node_id, http_port, grpc_port].
func (r *ReportHead) getAgentAddrByIP(ctx context.Context, ip string) (*agentAddrs, error) {
	reply, err := r.client.InternalKVGet(ctx, kvNamespaceDashboard, dashboardAgentAddrIPPrefix+ip)
	if err != nil {
		return nil, err
	}
	if len(reply.Value) == 0 {
		return nil, fmt.Errorf("no agent address found for node IP %s", ip)
	}
	var vals []interface{}
	if err := json.Unmarshal(reply.Value, &vals); err != nil || len(vals) < 3 {
		return nil, fmt.Errorf("invalid agent address %q", string(reply.Value))
	}
	return &agentAddrs{
		ip:       ip,
		httpPort: int(toFloat(vals[1])),
		grpcPort: int(toFloat(vals[2])),
	}, nil
}

// newReporterStub dials the reporter gRPC service on ip:grpc_port.
func newReporterStub(ctx context.Context, addrs *agentAddrs) (proto.ReporterServiceClient, *grpc.ClientConn, error) {
	conn, err := grpc.DialContext(ctx, buildAddress(addrs.ip, addrs.grpcPort), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, err
	}
	return proto.NewReporterServiceClient(conn), conn, nil
}

// resolveAgentAddr handles the node_id / ip resolution common to the worker
// profiling routes: node_id takes precedence, else ip.
func (r *ReportHead) resolveAgentAddr(ctx context.Context, nodeIDHex, ip string) (*agentAddrs, error) {
	if nodeIDHex != "" {
		return r.getAgentAddrByNodeID(ctx, nodeIDHex)
	}
	return r.getAgentAddrByIP(ctx, ip)
}

// runningTask returns the worker pid/worker_id of a task attempt, aligned with
// get_worker_details_for_running_task: filters task_id + attempt_number and
// raises an error unless the task is RUNNING.
func (r *ReportHead) runningTask(ctx context.Context, taskID string, attemptNumber int) (int, string, error) {
	opt := &head.ListApiOptions{
		Limit:            100,
		Timeout:          stateAPITimeoutSeconds,
		Detail:           true,
		ExcludeDriver:    true,
		FilterKeys:       []string{"task_id", "attempt_number"},
		FilterPredicates: []string{"=", "="},
		FilterValues:     []string{taskID, strconv.Itoa(attemptNumber)},
	}
	resp, err := r.api.ListTasks(ctx, opt)
	if err != nil {
		return 0, "", err
	}
	if len(resp.Result) == 0 {
		return 0, "", fmt.Errorf("no task found for task_id %s attempt %d", taskID, attemptNumber)
	}
	entry := resp.Result[0]
	pid := int(toFloat(entry["worker_pid"]))
	workerID := asString2(entry["worker_id"])
	state := asString2(entry["state"])
	if state != "RUNNING" {
		return 0, "", fmt.Errorf("The task attempt is not running: the current state is %s.", state)
	}
	return pid, workerID, nil
}

// taskIDsRunningInWorker lists the running task ids in a worker, aligned with
// get_task_ids_running_in_a_worker.
func (r *ReportHead) taskIDsRunningInWorker(ctx context.Context, workerID string) ([]string, error) {
	opt := &head.ListApiOptions{
		Limit:            100,
		Timeout:          stateAPITimeoutSeconds,
		Detail:           true,
		ExcludeDriver:    true,
		FilterKeys:       []string{"worker_id", "state"},
		FilterPredicates: []string{"=", "="},
		FilterValues:     []string{workerID, "RUNNING"},
	}
	resp, err := r.api.ListTasks(ctx, opt)
	if err != nil {
		return nil, err
	}
	var ids []string
	for _, entry := range resp.Result {
		if id := asString2(entry["task_id"]); id != "" {
			ids = append(ids, id)
		}
	}
	return ids, nil
}

// acquireProfiling limits concurrent profiling requests (aligned with the
// single-thread executor in reporter_head.py). It returns a release func.
func (r *ReportHead) acquireProfiling() func() {
	r.profSem <- struct{}{}
	return func() { <-r.profSem }
}

// handleTaskTraceback serves /task/traceback.
func (r *ReportHead) handleTaskTraceback(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	taskID := q.Get("task_id")
	attempt := q.Get("attempt_number")
	nodeIDHex := q.Get("node_id")
	if taskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}
	if attempt == "" {
		http.Error(w, "task's attempt number is required", http.StatusBadRequest)
		return
	}
	if nodeIDHex == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	attemptNum, err := strconv.Atoi(attempt)
	if err != nil {
		http.Error(w, "task's attempt number is required", http.StatusBadRequest)
		return
	}
	native := q.Get("native") == "1"
	release := r.acquireProfiling()
	defer release()

	pid, _, err := r.runningTask(req.Context(), taskID, attemptNum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	addrs, err := r.getAgentAddrByNodeID(req.Context(), nodeIDHex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python reporter_head.py logger.info("Sending stack trace
	// request to ...").
	log.Log.Info("sending stack trace request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "native", native)
	reply, err := stub.GetTraceback(req.Context(), &proto.GetTracebackRequest{Pid: uint32(pid), Native: boolPtr(native)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning stack trace", "size", len(reply.Output))
	_, workerID, err := r.runningTask(req.Context(), taskID, attemptNum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	taskIDs, err := r.taskIDsRunningInWorker(req.Context(), workerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	output := reply.Output
	if len(taskIDs) > 1 {
		output = warningForMultiTaskInAWorker + fmt.Sprintf("%v", taskIDs) + "\n" + output
	}
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(output))
}

// handleTaskCPUProfile serves /task/cpu_profile.
func (r *ReportHead) handleTaskCPUProfile(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	taskID := q.Get("task_id")
	attempt := q.Get("attempt_number")
	nodeIDHex := q.Get("node_id")
	if taskID == "" {
		http.Error(w, "task_id is required", http.StatusBadRequest)
		return
	}
	if attempt == "" {
		http.Error(w, "task's attempt number is required", http.StatusBadRequest)
		return
	}
	if nodeIDHex == "" {
		http.Error(w, "node_id is required", http.StatusBadRequest)
		return
	}
	attemptNum, err := strconv.Atoi(attempt)
	if err != nil {
		http.Error(w, "task's attempt number is required", http.StatusBadRequest)
		return
	}
	durationS := defaultCPUDurationS
	if v := q.Get("duration"); v != "" {
		durationS, err = strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid duration", http.StatusBadRequest)
			return
		}
	}
	// The proto field is uint32: Python raises ValueError when assigning a
	// negative duration, while Go would silently wrap it around to ~136 years
	// and pin the worker for the whole profiling. Reject negatives explicitly.
	if durationS < 0 {
		http.Error(w, fmt.Sprintf("duration cannot be negative: %d.", durationS), http.StatusBadRequest)
		return
	}
	if durationS > maxCPUDurationS {
		http.Error(w, fmt.Sprintf("The max duration allowed is %d seconds: %d.", maxCPUDurationS, durationS), http.StatusBadRequest)
		return
	}
	format := q.Get("format")
	if format == "" {
		format = defaultCPUFormat
	}
	native := q.Get("native") == "1"
	release := r.acquireProfiling()
	defer release()

	pid, _, err := r.runningTask(req.Context(), taskID, attemptNum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	addrs, err := r.getAgentAddrByNodeID(req.Context(), nodeIDHex)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python logger.info("Sending CPU profiling request to
	// ...").
	log.Log.Info("sending cpu profiling request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "task_id", taskID, "native", native)
	reply, err := stub.CpuProfiling(req.Context(), &proto.CpuProfilingRequest{Pid: uint32(pid), Duration: uint32Ptr(uint32(durationS)), Format: stringPtr(format), Native: boolPtr(native)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning profiling response", "size", len(reply.Output))
	_, workerID, err := r.runningTask(req.Context(), taskID, attemptNum)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	taskIDs, err := r.taskIDsRunningInWorker(req.Context(), workerID)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	output := svgStyle + reply.Output
	if len(taskIDs) > 1 {
		output = fmt.Sprintf(`<p style="color: #E37400;">%s %s </br> </p> </br>`, emojiWarning, warningForMultiTaskInAWorker+fmt.Sprintf("%v", taskIDs)) + output
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte(output))
}

// handleWorkerTraceback serves /worker/traceback.
func (r *ReportHead) handleWorkerTraceback(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	pidStr := q.Get("pid")
	ip := q.Get("ip")
	nodeIDHex := q.Get("node_id")
	if pidStr == "" {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	if nodeIDHex == "" && ip == "" {
		http.Error(w, "ip or node_id is required", http.StatusBadRequest)
		return
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	native := q.Get("native") == "1"
	release := r.acquireProfiling()
	defer release()

	addrs, err := r.resolveAgentAddr(req.Context(), nodeIDHex, ip)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python logger.info("Sending stack trace request to ...").
	log.Log.Info("sending stack trace request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "native", native)
	reply, err := stub.GetTraceback(req.Context(), &proto.GetTracebackRequest{Pid: uint32(pid), Native: boolPtr(native)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning stack trace", "size", len(reply.Output))
	w.Header().Set("Content-Type", "text/plain")
	_, _ = w.Write([]byte(reply.Output))
}

// handleWorkerCPUProfile serves /worker/cpu_profile.
func (r *ReportHead) handleWorkerCPUProfile(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	pidStr := q.Get("pid")
	ip := q.Get("ip")
	nodeIDHex := q.Get("node_id")
	if pidStr == "" {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	if nodeIDHex == "" && ip == "" {
		http.Error(w, "ip or node_id is required", http.StatusBadRequest)
		return
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	durationS := defaultCPUDurationS
	if v := q.Get("duration"); v != "" {
		durationS, err = strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid duration", http.StatusBadRequest)
			return
		}
	}
	// The proto field is uint32: Python raises ValueError when assigning a
	// negative duration, while Go would silently wrap it around to ~136 years
	// and pin the worker for the whole profiling. Reject negatives explicitly.
	if durationS < 0 {
		http.Error(w, fmt.Sprintf("duration cannot be negative: %d.", durationS), http.StatusBadRequest)
		return
	}
	if durationS > maxCPUDurationS {
		http.Error(w, fmt.Sprintf("The max duration allowed is %d seconds: %d.", maxCPUDurationS, durationS), http.StatusBadRequest)
		return
	}
	format := q.Get("format")
	if format == "" {
		format = defaultCPUFormat
	}
	native := q.Get("native") == "1"
	release := r.acquireProfiling()
	defer release()

	addrs, err := r.resolveAgentAddr(req.Context(), nodeIDHex, ip)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python logger.info("Sending CPU profiling request to
	// ...").
	log.Log.Info("sending cpu profiling request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "native", native)
	reply, err := stub.CpuProfiling(req.Context(), &proto.CpuProfilingRequest{Pid: uint32(pid), Duration: uint32Ptr(uint32(durationS)), Format: stringPtr(format), Native: boolPtr(native)})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning profiling response", "size", len(reply.Output))
	contentType := "text/plain"
	if format == "flamegraph" {
		contentType = "image/svg+xml"
	}
	w.Header().Set("Content-Type", contentType)
	_, _ = w.Write([]byte(reply.Output))
}

// handleWorkerGPUProfile serves /worker/gpu_profile. On success the Python
// handler redirects to the logs file API; the Go module returns the file path
// in the Location header plus a 302, keeping the redirect semantics without a
// separate logs module.
func (r *ReportHead) handleWorkerGPUProfile(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	pidStr := q.Get("pid")
	ip := q.Get("ip")
	nodeIDHex := q.Get("node_id")
	if pidStr == "" {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	if nodeIDHex == "" && ip == "" {
		http.Error(w, "ip or node_id is required", http.StatusBadRequest)
		return
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		http.Error(w, "pid is required", http.StatusBadRequest)
		return
	}
	numIterations := defaultGPUIterations
	if v := q.Get("num_iterations"); v != "" {
		numIterations, err = strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid num_iterations", http.StatusBadRequest)
			return
		}
	}
	release := r.acquireProfiling()
	defer release()

	addrs, err := r.resolveAgentAddr(req.Context(), nodeIDHex, ip)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python logger.info("Sending GPU profiling request to
	// ...").
	log.Log.Info("sending gpu profiling request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "num_iterations", numIterations)
	reply, err := stub.GpuProfiling(req.Context(), &proto.GpuProfilingRequest{Pid: uint32(pid), NumIterations: uint32Ptr(uint32(numIterations))})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning profiling response", "size", len(reply.Output))
	filepath := reply.Output
	downloadFilename := filepath
	if idx := lastSlash(filepath); idx >= 0 {
		downloadFilename = filepath[idx+1:]
	}
	// URL-encode the query params, aligned with the Python urlencode(...) used to
	// build the redirect URL (filepath may contain spaces / special characters).
	redirect := url.Values{}
	redirect.Set("node_ip", addrs.ip)
	redirect.Set("filename", filepath)
	redirect.Set("download_filename", downloadFilename)
	redirect.Set("lines", "-1")
	redirectURL := "/api/v0/logs/file?" + redirect.Encode()
	w.Header().Set("Location", redirectURL)
	w.WriteHeader(http.StatusFound)
}

func boolPtr(b bool) *bool { return &b }

func stringPtr(s string) *string { return &s }

func uint32Ptr(u uint32) *uint32 { return &u }

func lastSlash(s string) int {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return i
		}
	}
	return -1
}

// handleMemoryProfile serves /memory_profile. It supports both the worker
// (pid + ip/node_id) and task (task_id + attempt_number + node_id) forms.
func (r *ReportHead) handleMemoryProfile(w http.ResponseWriter, req *http.Request) {
	q := req.URL.Query()
	isTask := q.Get("task_id") != ""

	var pid int
	var addrs *agentAddrs
	var workerID string
	var err error

	if isTask {
		attempt := q.Get("attempt_number")
		nodeIDHex := q.Get("node_id")
		if attempt == "" {
			http.Error(w, "Failed to execute task profiling: task's attempt number is required", http.StatusInternalServerError)
			return
		}
		if nodeIDHex == "" {
			http.Error(w, "Failed to execute task profiling: task's node id is required", http.StatusInternalServerError)
			return
		}
		attemptNum, aerr := strconv.Atoi(attempt)
		if aerr != nil {
			http.Error(w, "Failed to execute task profiling: task's attempt number is required", http.StatusInternalServerError)
			return
		}
		release := r.acquireProfiling()
		pid, workerID, err = r.runningTask(req.Context(), q.Get("task_id"), attemptNum)
		if err != nil {
			release()
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		addrs, err = r.getAgentAddrByNodeID(req.Context(), nodeIDHex)
		if err != nil {
			release()
			http.Error(w, fmt.Sprintf("Failed to execute: no agent address found for node %s", nodeIDHex), http.StatusInternalServerError)
			return
		}
		// keep holding the profiling slot until the RPC completes
		defer func() {
			release()
		}()
	} else {
		pidStr := q.Get("pid")
		ip := q.Get("ip")
		nodeIDHex := q.Get("node_id")
		if pidStr == "" {
			http.Error(w, "pid is required", http.StatusInternalServerError)
			return
		}
		if nodeIDHex == "" && ip == "" {
			http.Error(w, "ip or node_id is required", http.StatusBadRequest)
			return
		}
		pid, err = strconv.Atoi(pidStr)
		if err != nil {
			http.Error(w, "pid is required", http.StatusInternalServerError)
			return
		}
		release := r.acquireProfiling()
		defer release()
		addrs, err = r.resolveAgentAddr(req.Context(), nodeIDHex, ip)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to execute: no agent address found for node %s", nodeIDHex), http.StatusInternalServerError)
			return
		}
	}

	durationS := defaultMemoryDurationS
	if v := q.Get("duration"); v != "" {
		durationS, err = strconv.Atoi(v)
		if err != nil {
			http.Error(w, "invalid duration", http.StatusBadRequest)
			return
		}
	}
	format := q.Get("format")
	if format == "" {
		format = defaultMemoryFormat
	}
	native := q.Get("native") == "1"
	leaks := q.Get("leaks") == "1"
	tracePythonAllocators := q.Get("trace_python_allocators") == "1"

	stub, conn, err := newReporterStub(req.Context(), addrs)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer conn.Close()
	// Aligned with the Python logger.info("Retrieving memory profiling request
	// to ...").
	log.Log.Info("retrieving memory profiling request", "addr", buildAddress(addrs.ip, addrs.grpcPort), "pid", pid, "native", native)
	reply, err := stub.MemoryProfiling(req.Context(), &proto.MemoryProfilingRequest{
		Pid:                   uint32(pid),
		Format:                stringPtr(format),
		Leaks:                 boolPtr(leaks),
		Duration:              uint32Ptr(uint32(durationS)),
		Native:                boolPtr(native),
		TracePythonAllocators: boolPtr(tracePythonAllocators),
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	warning := ""
	if reply.Warning != nil {
		warning = *reply.Warning
	}
	if isTask {
		taskIDs, terr := r.taskIDsRunningInWorker(req.Context(), workerID)
		if terr != nil {
			http.Error(w, terr.Error(), http.StatusInternalServerError)
			return
		}
		if len(taskIDs) > 1 {
			warning += "\n" + warningForMultiTaskInAWorker + fmt.Sprintf("%v", taskIDs)
		}
	}
	if !reply.Success {
		http.Error(w, reply.Output, http.StatusInternalServerError)
		return
	}
	log.Log.Info("returning profiling response", "size", len(reply.Output))
	output := reply.Output
	if warning != "" {
		output = fmt.Sprintf(`<p style="color: #E37400;">%s %s </br> </p> </br>`, emojiWarning, warning) + output
	}
	w.Header().Set("Content-Type", "text/html")
	_, _ = w.Write([]byte(output))
}
