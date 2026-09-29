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

package job

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/coder/websocket"
)

// JobAgentSubmissionClient forwards job submission requests to the JobAgent on
// a specific node over HTTP/WebSocket, aligned with the Python
// JobAgentSubmissionClient (job/job_head.py).
type JobAgentSubmissionClient struct {
	agentAddress string
	httpClient   *http.Client
}

// NewJobAgentSubmissionClient creates a client for the agent at the given
// http address (e.g. "http://1.2.3.4:8266").
func NewJobAgentSubmissionClient(agentAddress string, httpClient *http.Client) *JobAgentSubmissionClient {
	if httpClient == nil {
		httpClient = &http.Client{}
	}
	return &JobAgentSubmissionClient{agentAddress: agentAddress, httpClient: httpClient}
}

// AgentAddress returns the agent http address.
func (c *JobAgentSubmissionClient) AgentAddress() string { return c.agentAddress }

// doJSON performs an HTTP request against the agent and decodes the 200 JSON
// response into out. A non-200 response surfaces an error with the status code
// and body text, aligned with JobAgentSubmissionClient._raise_error.
func (c *JobAgentSubmissionClient) doJSON(ctx context.Context, method, path string, body interface{}, out interface{}) error {
	var reader io.Reader
	if body != nil {
		payload, err := json.Marshal(body)
		if err != nil {
			return fmt.Errorf("marshal request body: %w", err)
		}
		reader = bytes.NewReader(payload)
	}
	req, err := http.NewRequestWithContext(ctx, method, c.agentAddress+path, reader)
	if err != nil {
		return err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json; charset=utf-8")
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("request failed with status code %d: %s", resp.StatusCode, string(respBody))
	}
	if out != nil {
		if err := json.Unmarshal(respBody, out); err != nil {
			return fmt.Errorf("unmarshal response: %w", err)
		}
	}
	return nil
}

// SubmitJobInternal submits a job to the agent.
func (c *JobAgentSubmissionClient) SubmitJobInternal(ctx context.Context, req *JobSubmitRequest) (*JobSubmitResponse, error) {
	var out JobSubmitResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/job_agent/jobs/", req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// StopJobInternal stops a submission job.
func (c *JobAgentSubmissionClient) StopJobInternal(ctx context.Context, submissionID string) (*JobStopResponse, error) {
	var out JobStopResponse
	if err := c.doJSON(ctx, http.MethodPost, "/api/job_agent/jobs/"+submissionID+"/stop", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// DeleteJobInternal deletes a submission job.
func (c *JobAgentSubmissionClient) DeleteJobInternal(ctx context.Context, submissionID string) (*JobDeleteResponse, error) {
	var out JobDeleteResponse
	if err := c.doJSON(ctx, http.MethodDelete, "/api/job_agent/jobs/"+submissionID, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// GetJobLogsInternal fetches the logs of a submission job.
func (c *JobAgentSubmissionClient) GetJobLogsInternal(ctx context.Context, submissionID string) (*JobLogsResponse, error) {
	var out JobLogsResponse
	if err := c.doJSON(ctx, http.MethodGet, "/api/job_agent/jobs/"+submissionID+"/logs", nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// TailJobLogs dials the agent WebSocket log tail endpoint and returns a
// channel of text log lines plus a function to close the connection. The
// returned channel is closed when the agent closes the WebSocket, aligned with
// JobAgentSubmissionClient.tail_job_logs.
func (c *JobAgentSubmissionClient) TailJobLogs(ctx context.Context, submissionID string) (<-chan string, func(), error) {
	url := "ws" + strings.TrimPrefix(c.agentAddress, "http") + "/api/job_agent/jobs/" + submissionID + "/logs/tail"
	conn, _, err := websocket.Dial(ctx, url, nil)
	if err != nil {
		return nil, nil, err
	}
	lines := make(chan string)
	closed := make(chan struct{})
	go func() {
		defer close(lines)
		for {
			typ, data, err := conn.Read(ctx)
			if err != nil {
				return
			}
			if typ == websocket.MessageText {
				select {
				case lines <- string(data):
				case <-closed:
					return
				}
			}
		}
	}()
	closeFn := func() {
		select {
		case <-closed:
			return
		default:
		}
		close(closed)
		_ = conn.Close(websocket.StatusNormalClosure, "")
	}
	return lines, closeFn, nil
}
