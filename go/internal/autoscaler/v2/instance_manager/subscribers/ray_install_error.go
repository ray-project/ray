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

package subscribers

// RayInstallError is the Ray installation failure error.
// Corresponds to Python threaded_ray_installer.RayInstallError:
// produced by the ThreadedRayInstaller (Phase 8, not yet ported) and consumed
// by the reconciler (RAY_INSTALLING -> RAY_INSTALL_FAILED).
type RayInstallError struct {
	// ImInstanceId is the instance manager's instance ID.
	ImInstanceId string
	// Details describes the error.
	Details string
}

func (e RayInstallError) Error() string {
	return "failed to install ray for instance " + e.ImInstanceId + ": " + e.Details
}
