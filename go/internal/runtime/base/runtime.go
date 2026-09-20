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

// Package base provides base types and interfaces for Ray Go Runtime.
package base

// Re-export types from contract package for backward compatibility
import contract "github.com/ray-project/ray/go/pkg/runtime/contract"

type Runtime = contract.Runtime
type RunMode = contract.RunMode
type NodeState = contract.NodeState
type NodeInfo = contract.NodeInfo
type ActorState = contract.ActorState
type ActorInfo = contract.ActorInfo

// Re-export RunMode constants
const (
	RunModeCluster = contract.RunModeCluster
	RunModeLocal   = contract.RunModeLocal
)
