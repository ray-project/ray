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

package native

import (
	"os"
	"strconv"
)

const (
	envSubscriberChannelSize = "RAY_GCS_SUBSCRIBER_CHANNEL_SIZE"
	defaultChannelSize       = 1000
	minChannelSize           = 100
	maxChannelSize           = 100000
)

// getSubscriberChannelSize returns the subscriber channel buffer size read
// from the environment variable.
func getSubscriberChannelSize() int {
	if val := os.Getenv(envSubscriberChannelSize); val != "" {
		if size, err := strconv.Atoi(val); err == nil && size > 0 {
			// Clamp to the configured bounds.
			if size < minChannelSize {
				return minChannelSize
			}
			if size > maxChannelSize {
				return maxChannelSize
			}
			return size
		}
	}
	return defaultChannelSize
}
