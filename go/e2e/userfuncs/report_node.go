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

package userfuncs

import (
	"github.com/ray-project/ray/go/pkg/runtime/api"
)

// reportNodeID returns the hex node id of the worker that executes it. E2E
// placement group drivers use it to verify that a task bound to a placement
// group bundle actually runs on a cluster node (the bundle's scheduling
// target) and that the node is a member of the cluster.
//
// It is registered manually in RegisterFunctions (like echoBytes /
// runtimeEnvOfTask) so it can be invoked with zero arguments; raygen is not
// involved.
func reportNodeID() (string, error) {
	ctx, err := api.GetRuntimeContext()
	if err != nil {
		return "", err
	}
	return ctx.NodeID().Hex(), nil
}

// GetReportNodeID returns the reportNodeID function for direct invocation.
func GetReportNodeID() func() (string, error) {
	return reportNodeID
}
