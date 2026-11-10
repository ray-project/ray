// Copyright 2026 The Ray Authors.
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

package job

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"math/big"
	"net/http"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ray-project/ray/go/internal/dashboard/jobutil"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
)

const (
	// maxChunkLineLength and maxChunkCharLength bound one websocket log chunk,
	// aligned with the Python log tail batching.
	maxChunkLineLength = 10
	maxChunkCharLength = 20000
)

// generateJobID returns a new submission id, aligned with the Python
// generate_job_id: a "raysubmit_" prefix plus 16 characters drawn from an
// alphabet without visually ambiguous letters and digits.
func generateJobID() string {
	const prefix = "raysubmit_"
	const alphabet = "abcdefghjkmnpqrstuvwxyzABCDEFGHJKMNPQRSTUVWXYZ23456789"
	idPart := make([]byte, 16)
	for i := range idPart {
		n, err := rand.Int(rand.Reader, big.NewInt(int64(len(alphabet))))
		if err != nil {
			idPart[i] = alphabet[time.Now().UnixNano()%int64(len(alphabet))]
			continue
		}
		idPart[i] = alphabet[n.Int64()]
	}
	return prefix + string(idPart)
}

// stripKeysWithValueNone drops the keys whose value is nil, matching the
// Python strip_keys_with_value_none applied to a decoded request body.
func stripKeysWithValueNone(d map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(d))
	for k, v := range d {
		if v != nil {
			out[k] = v
		}
	}
	return out
}

var urlSecretRE = regexp.MustCompile(`https?:\/\/.*:(.*)@.*`)

// redactURLPassword replaces the password of a URL with "<redacted>", used
// before a URL is echoed in a log line or an error message.
func redactURLPassword(url string) string {
	m := urlSecretRE.FindStringSubmatch(url)
	if len(m) > 1 {
		url = strings.Replace(url, ":"+m[1]+"@", ":<redacted>@", 1)
	}
	return url
}

// fileTailIterator follows a log file like `tail -f`, waiting for the file to
// appear and then emitting whole lines in chunks. Each chunk holds at most
// maxChunkLineLength lines or maxChunkCharLength characters. A nil chunk means
// the file is still missing. Both channels are closed when ctx is cancelled.
func fileTailIterator(ctx context.Context, path string) (<-chan []string, <-chan error) {
	linesCh := make(chan []string)
	errCh := make(chan error, 1)
	go func() {
		defer close(linesCh)
		defer close(errCh)
		for {
			if _, err := os.Stat(path); err == nil {
				break
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			select {
			case linesCh <- nil:
			case <-ctx.Done():
				return
			}
		}

		f, err := os.Open(path)
		if err != nil {
			select {
			case errCh <- err:
			case <-ctx.Done():
			}
			return
		}
		defer f.Close()

		var lines []string
		chunkCharCount := 0
		buf := make([]byte, 4096)
		pending := ""
		atEOF := false

		for {
			if len(lines) >= maxChunkLineLength || chunkCharCount > maxChunkCharLength || atEOF {
				var out []string
				if len(lines) > 0 {
					out = lines
				}
				select {
				case linesCh <- out:
				case <-ctx.Done():
					return
				}
				lines = nil
				chunkCharCount = 0
			}
			if atEOF {
				select {
				case <-ctx.Done():
					return
				case <-time.After(time.Second):
				}
				atEOF = false
				continue
			}

			n, rerr := f.Read(buf)
			if n > 0 {
				pending += string(buf[:n])
			}
			for {
				idx := strings.IndexByte(pending, '\n')
				if idx < 0 {
					break
				}
				line := pending[:idx+1]
				pending = pending[idx+1:]
				lines = append(lines, line)
				chunkCharCount += len(line)
			}
			if rerr != nil {
				if rerr != io.EOF {
					select {
					case errCh <- rerr:
					case <-ctx.Done():
					}
					return
				}
				if pending != "" {
					lines = append(lines, pending)
					chunkCharCount += len(pending)
					pending = ""
				}
				atEOF = true
			}
		}
	}()
	return linesCh, errCh
}

// fastTailLastNLines returns the last numLines lines of a file, reading it
// backwards in blocks so a large log is never fully loaded. The result is
// truncated to its last maxChars characters. Any invalid argument or file
// error yields an empty string, matching the CLI log endpoint semantics.
func fastTailLastNLines(path string, numLines int, maxChars int) string {
	const blockSize = 8192
	if numLines < 0 {
		return ""
	}
	if numLines == 0 {
		return ""
	}
	if maxChars <= 0 {
		return ""
	}

	f, err := os.Open(path)
	if err != nil {
		return ""
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return ""
	}
	fileSize := fi.Size()
	if fileSize == 0 {
		return ""
	}

	var chunks []byte
	position := fileSize
	newlinesFound := 0
	for position > 0 && newlinesFound < numLines+1 {
		readSize := int64(blockSize)
		if readSize > position {
			readSize = position
		}
		start := position - readSize
		if _, err := f.Seek(start, io.SeekStart); err != nil {
			break
		}
		chunk := make([]byte, readSize)
		// ReadFull keeps a short Read from silently dropping the block-leading
		// bytes: the scan only advances past bytes the file actually returned.
		n, err := io.ReadFull(f, chunk)
		if n == 0 {
			break
		}
		chunks = append(chunk[:n], chunks...)
		newlinesFound += bytes.Count(chunk[:n], []byte("\n"))
		if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
			break
		}
		position = start
	}

	text := string(chunks)
	var lines []string
	for len(text) > 0 {
		idx := strings.IndexByte(text, '\n')
		if idx < 0 {
			lines = append(lines, text)
			break
		}
		lines = append(lines, text[:idx+1])
		text = text[idx+1:]
	}
	if len(lines) > numLines {
		lines = lines[len(lines)-numLines:]
	}
	result := strings.Join(lines, "")
	if len(result) > maxChars {
		result = result[len(result)-maxChars:]
	}
	return result
}

// getDriverJobs returns every non-internal driver job keyed by job id together
// with the running driver of each submission job keyed by submission id,
// aligned with get_driver_jobs in job/utils.py.
func getDriverJobs(ctx context.Context, client gcs.Client, jobOrSubmissionID string) (map[string]*JobDetails, map[string]*DriverInfo, error) {
	allJobs, err := client.ListJobsWithFilter(ctx, jobOrSubmissionID, true, true)
	if err != nil {
		return nil, nil, fmt.Errorf("list jobs: %w", err)
	}

	// Sort by job id so the driver picked for a submission id is deterministic
	// when a submission was retried.
	sorted := make([]*proto.JobTableData, len(allJobs))
	copy(sorted, allJobs)
	sort.Slice(sorted, func(i, j int) bool {
		return hex.EncodeToString(sorted[i].JobId) < hex.EncodeToString(sorted[j].JobId)
	})

	jobs := map[string]*JobDetails{}
	submissionDrivers := map[string]*DriverInfo{}
	for _, entry := range sorted {
		if entry.Config == nil || strings.HasPrefix(entry.Config.RayNamespace, jobutil.RayInternalNamespacePrefix) {
			continue
		}
		jobID := hex.EncodeToString(entry.JobId)
		metadata := entry.Config.Metadata
		submissionID := metadata[jobutil.JobIDMetadataKey]
		driver := &DriverInfo{
			ID:            jobID,
			NodeIPAddress: jobutil.DriverIP(entry),
			PID:           strconv.FormatInt(entry.DriverPid, 10),
		}
		if submissionID == "" {
			status := JobStatusRunning
			if entry.IsDead {
				status = JobStatusSucceeded
			}
			if metadata == nil {
				metadata = map[string]string{}
			}
			jobs[jobID] = &JobDetails{
				Type:       JobTypeDriver,
				JobID:      jobutil.PtrString(jobID),
				Status:     status,
				Entrypoint: entry.Entrypoint,
				StartTime:  jobutil.Int64Ptr(int64(entry.StartTime)),
				EndTime:    jobutil.Int64Ptr(int64(entry.EndTime)),
				Metadata:   metadata,
				RuntimeEnv: jobutil.RuntimeEnvToDict(jobutil.RuntimeEnvSerialized(entry)),
				DriverInfo: driver,
			}
		} else {
			submissionDrivers[submissionID] = driver
		}
	}
	return jobs, submissionDrivers, nil
}

// findJobByIDs resolves a job id or a submission id to its job details,
// aligned with find_job_by_ids in job/utils.py. It returns (nil, nil) when
// neither the driver table nor the KV holds a match.
func findJobByIDs(ctx context.Context, client gcs.Client, infoClient *JobInfoStorageClient, jobOrSubmissionID string) (*JobDetails, error) {
	driverJobs, submissionDrivers, err := getDriverJobs(ctx, client, jobOrSubmissionID)
	if err != nil {
		return nil, err
	}
	if job := driverJobs[jobOrSubmissionID]; job != nil {
		return job, nil
	}
	submissionID := ""
	for id, driver := range submissionDrivers {
		if driver.ID == jobOrSubmissionID {
			submissionID = id
			break
		}
	}
	if submissionID == "" {
		submissionID = jobOrSubmissionID
	}
	info, err := infoClient.GetInfo(ctx, submissionID)
	if err != nil {
		return nil, err
	}
	if info == nil {
		return nil, nil
	}
	var driver *DriverInfo
	if d := submissionDrivers[submissionID]; d != nil {
		driver = d
	}
	return submissionJobDetails(submissionID, info, driver), nil
}

// submissionJobDetails builds the job details of a submission job, merging the
// stored job info with its driver info when a driver is running.
func submissionJobDetails(submissionID string, info *JobInfo, driver *DriverInfo) *JobDetails {
	var jobID *string
	if driver != nil {
		jobID = jobutil.PtrString(driver.ID)
	}
	return &JobDetails{
		Type:                   JobTypeSubmission,
		JobID:                  jobID,
		SubmissionID:           jobutil.PtrString(submissionID),
		DriverInfo:             driver,
		Status:                 info.Status,
		Entrypoint:             info.Entrypoint,
		Message:                info.Message,
		ErrorType:              info.ErrorType,
		StartTime:              info.StartTime,
		EndTime:                info.EndTime,
		Metadata:               info.Metadata,
		RuntimeEnv:             jobutil.RuntimeEnvFromJSON(info.RuntimeEnvJSON),
		DriverAgentHTTPAddress: info.DriverAgentHTTPAddress,
		DriverNodeID:           info.DriverNodeID,
		DriverExitCode:         info.DriverExitCode,
	}
}

func jsonMarshal(v interface{}) ([]byte, error) {
	return json.Marshal(v)
}

// valueError marks a request validation failure so the route layer can map it
// to HTTP 400 rather than 500.
type valueError struct{ msg string }

func (e *valueError) Error() string { return e.msg }

// validateJobSubmitRequest type-checks the decoded request body against the
// JobSubmitRequest schema, aligned with the Python
// validate_job_submit_request.
func validateJobSubmitRequest(raw map[string]interface{}) error {
	if entrypoint, ok := raw["entrypoint"]; ok {
		if _, isStr := entrypoint.(string); !isStr {
			return &valueError{msg: fmt.Sprintf("entrypoint must be a string, got %s", jsonTypeName(entrypoint))}
		}
	}
	for _, key := range []string{"submission_id", "job_id"} {
		if v, ok := raw[key]; ok {
			if _, isStr := v.(string); !isStr {
				return &valueError{msg: fmt.Sprintf("%s must be a string if provided, got %s", key, jsonTypeName(v))}
			}
		}
	}
	if re, ok := raw["runtime_env"]; ok {
		if _, isDict := re.(map[string]interface{}); !isDict {
			return &valueError{msg: fmt.Sprintf("runtime_env must be a dict, got %s", jsonTypeName(re))}
		}
	}
	if meta, ok := raw["metadata"]; ok {
		dict, isDict := meta.(map[string]interface{})
		if !isDict {
			return &valueError{msg: fmt.Sprintf("metadata must be a dict, got %s", jsonTypeName(meta))}
		}
		for _, v := range dict {
			if _, isStr := v.(string); !isStr {
				return &valueError{msg: fmt.Sprintf("metadata values must be strings, got %s", jsonTypeName(v))}
			}
		}
	}
	for _, key := range []string{"entrypoint_num_cpus", "entrypoint_num_gpus"} {
		if v, ok := raw[key]; ok {
			if _, isNum := v.(float64); !isNum {
				return &valueError{msg: fmt.Sprintf("%s must be a number, got %s", key, jsonTypeName(v))}
			}
		}
	}
	if v, ok := raw["entrypoint_memory"]; ok {
		num, isNum := v.(float64)
		if !isNum {
			return &valueError{msg: fmt.Sprintf("entrypoint_memory must be an integer, got %s", jsonTypeName(v))}
		}
		if num != math.Trunc(num) {
			return &valueError{msg: fmt.Sprintf("entrypoint_memory must be an integer, got %s", jsonTypeName(v))}
		}
	}
	if res, ok := raw["entrypoint_resources"]; ok {
		dict, isDict := res.(map[string]interface{})
		if !isDict {
			return &valueError{msg: fmt.Sprintf("entrypoint_resources must be a dict, got %s", jsonTypeName(res))}
		}
		for _, v := range dict {
			if _, isNum := v.(float64); !isNum {
				return &valueError{msg: fmt.Sprintf("entrypoint_resources values must be numbers, got %s", jsonTypeName(v))}
			}
		}
	}
	if selector, ok := raw["entrypoint_label_selector"]; ok && !emptyLabelSelector(selector) {
		// The Python agent honors this field by folding it into the supervisor
		// actor's label selector. This agent pins the supervisor with a node
		// affinity strategy instead, and the actor submission interface it uses
		// has no label selector at all, so accepting the field would silently
		// drop it: the job would be hard-pinned to the head node while the
		// caller believed its selector was in effect. Reject it instead.
		return &valueError{msg: "entrypoint_label_selector is not supported by the Go job agent: the job supervisor is pinned to the head node with a node affinity strategy, which cannot carry a label selector"}
	}
	return nil
}

// emptyLabelSelector reports whether a decoded entrypoint_label_selector value
// is one the Python agent would treat as absent: null, or an empty dict. An
// empty dict is falsy in Python, so a request carrying one is accepted there
// and must not be rejected here.
func emptyLabelSelector(v interface{}) bool {
	if v == nil {
		return true
	}
	dict, isDict := v.(map[string]interface{})
	return isDict && len(dict) == 0
}

// jsonTypeName returns the Python type name of a decoded JSON value, so the
// validation errors read like the Python ones.
func jsonTypeName(v interface{}) string {
	switch v.(type) {
	case string:
		return "str"
	case float64:
		return "float"
	case bool:
		return "bool"
	case map[string]interface{}:
		return "dict"
	case []interface{}:
		return "list"
	case nil:
		return "NoneType"
	default:
		return fmt.Sprintf("%T", v)
	}
}

// parseAndValidateRequest decodes, validates, and converts a job submit
// request body.
func parseAndValidateRequest(r *http.Request) (*JobSubmitRequest, error) {
	var raw map[string]interface{}
	dec := json.NewDecoder(r.Body)
	if err := dec.Decode(&raw); err != nil {
		return nil, &valueError{msg: fmt.Sprintf("failed to parse request body: %v", err)}
	}
	raw = stripKeysWithValueNone(raw)
	if err := validateJobSubmitRequest(raw); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(raw)
	if err != nil {
		return nil, &valueError{msg: fmt.Sprintf("failed to re-serialize request: %v", err)}
	}
	req := &JobSubmitRequest{}
	if err := json.Unmarshal(payload, req); err != nil {
		return nil, &valueError{msg: fmt.Sprintf("invalid request type: %v", err)}
	}
	if req.Entrypoint == "" {
		return nil, &valueError{msg: "entrypoint must be a non-empty string"}
	}
	return req, nil
}
