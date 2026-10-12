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

package cgo

import (
	"sync"

	"github.com/ray-project/ray/go/pkg/ids"
)

// createdActorIDs is a process-wide, thread-safe set of actor IDs that
// were created by this process. It backs api.GetActorHandle so the Go runtime
// does not need to reach into the C++ ActorManager (core_worker.cc is frozen).
// Entries are added on successful CreateActor and never removed, mirroring the
// C++ ActorManager which keeps handles until process exit.
type createdActorIDs struct {
	m sync.Map // map[ids.ActorID]struct{}
}

// createdActorIDSet is the process-wide registry singleton.
var createdActorIDSet = &createdActorIDs{}

func (r *createdActorIDs) add(actorID ids.ActorID) {
	r.m.Store(actorID, struct{}{})
}

func (r *createdActorIDs) contains(actorID ids.ActorID) bool {
	_, ok := r.m.Load(actorID)
	return ok
}
