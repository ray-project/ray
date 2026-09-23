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

// Package testenv provides the shared environment checks used by the GCS
// integration tests.
package testenv

import (
	"fmt"
	"os"
	"os/exec"
	"strconv"
)

// envIntegrationTests must be set to a true value to run the GCS integration
// tests.
const envIntegrationTests = "RAY_GCS_INTEGRATION_TESTS"

// IntegrationEnabled reports whether the GCS integration tests were explicitly
// enabled through RAY_GCS_INTEGRATION_TESTS. Those tests start and stop a real
// Ray cluster, so they stay opt-in and never run as part of the default unit
// test path.
func IntegrationEnabled() bool {
	enabled, err := strconv.ParseBool(os.Getenv(envIntegrationTests))
	return err == nil && enabled
}

// CheckEnv checks whether the tools required by the integration tests are
// available.
// An empty string means the environment is ready; otherwise the returned
// English reason is meant for t.Skip or for printing by the caller.
func CheckEnv() string {
	if !IntegrationEnabled() {
		return fmt.Sprintf("set %s=1 to run the GCS integration tests", envIntegrationTests)
	}
	if _, err := exec.LookPath("ray"); err != nil {
		return fmt.Sprintf("ray not found, skipping GCS integration tests: %v", err)
	}
	if _, err := exec.LookPath("python3"); err != nil {
		return fmt.Sprintf("python3 not found, skipping GCS integration tests: %v", err)
	}
	return ""
}
