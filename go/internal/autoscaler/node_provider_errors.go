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

package autoscaler

import "fmt"

// CloudInstanceProviderError is the base error of cloud instance providers.
// It corresponds to the Python side node_provider.CloudInstanceProviderError:
// the error reported when a cloud instance provider asynchronous operation
// (launch/terminate) fails.
type CloudInstanceProviderError struct {
	// Msg is the error message.
	Msg string
	// TimestampNs is when the error happened, in nanoseconds.
	TimestampNs int64
}

func (e *CloudInstanceProviderError) Error() string {
	return e.Msg
}

// LaunchNodeError is the error of failing to launch a cloud instance.
// It corresponds to the Python side node_provider.LaunchNodeError,
// produced by the cloud provider and consumed by the reconciler
// (REQUESTED -> ALLOCATION_FAILED).
type LaunchNodeError struct {
	CloudInstanceProviderError
	// NodeType is the node type whose launch failed.
	NodeType string
	// Count is the number of instances that failed to launch.
	Count int
	// RequestId is the ID of the corresponding launch request.
	RequestId string
	// Cause is the underlying error.
	Cause error
}

func (e *LaunchNodeError) Error() string {
	return fmt.Sprintf("LaunchNodeError(node_type=%s, count=%d, request_id=%s): %v",
		e.NodeType, e.Count, e.RequestId, e.Cause)
}

// TerminateNodeError is the error of failing to terminate a cloud instance.
// It corresponds to the Python side node_provider.TerminateNodeError,
// produced by the cloud provider and consumed by the reconciler
// (TERMINATING -> TERMINATION_FAILED).
type TerminateNodeError struct {
	CloudInstanceProviderError
	// CloudInstanceId is the cloud instance ID whose termination failed.
	CloudInstanceId string
	// RequestId is the ID of the corresponding termination request.
	RequestId string
	// Cause is the underlying error.
	Cause error
}

func (e *TerminateNodeError) Error() string {
	return fmt.Sprintf("TerminateNodeError(cloud_instance_id=%s, request_id=%s): %v",
		e.CloudInstanceId, e.RequestId, e.Cause)
}
