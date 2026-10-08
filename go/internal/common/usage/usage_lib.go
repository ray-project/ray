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

package usage

import (
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/ray-project/ray/go/internal/runtime_env"
	"github.com/ray-project/ray/go/pkg/gcs"
	"github.com/ray-project/ray/go/proto"
)

var (
	// recordedExtraUsageTags is the in-memory cache used for deduplication and
	// pre-initialization support.
	recordedExtraUsageTags = make(map[string]string)
	// recordedExtraUsageTagsLock guards concurrent access to the cache.
	recordedExtraUsageTagsLock sync.RWMutex
)

// RecordExtraUsageTag records an extra usage stats KV tag.
func RecordExtraUsageTag(ctx context.Context, key proto.TagKey, value string, gcsClient gcs.Client) error {
	keyStr := strings.ToLower(proto.TagKey_name[int32(key)])

	// Update the in-memory cache in a thread-safe way.
	recordedExtraUsageTagsLock.Lock()
	if existingValue, exists := recordedExtraUsageTags[keyStr]; exists && existingValue == value {
		recordedExtraUsageTagsLock.Unlock()
		return nil
	}
	recordedExtraUsageTags[keyStr] = value
	recordedExtraUsageTagsLock.Unlock()

	if !runtime_env.InternalKVInitialized() && gcsClient == nil {
		// This happens if the record is before ray.init and
		// no GCS client is used for recording explicitly.
		return nil
	}

	// Write the tag into the KV store.
	return putExtraUsageTag(ctx, keyStr, value, gcsClient)
}

// putExtraUsageTag writes an extra usage tag into the KV store.
func putExtraUsageTag(ctx context.Context, key string, value string, gcsClient gcs.Client) error {
	fullKey := fmt.Sprintf("%s%s", EXTRA_USAGE_TAG_PREFIX, key)
	if gcsClient != nil {
		_, err := gcsClient.Put(ctx, USAGE_STATS_NAMESPACE, fullKey, []byte(value), true)
		if err != nil {
			return fmt.Errorf("failed to put extra usage tag with GCS client: %w", err)
		}
	} else {
		_, err := runtime_env.InternalKVPut(ctx, fullKey, []byte(value), true, USAGE_STATS_NAMESPACE)
		if err != nil {
			return fmt.Errorf("failed to put extra usage tag with internal KV: %w", err)
		}
	}

	return nil
}

// GetRecordedExtraUsageTags returns all extra usage tags currently held in the
// in-memory cache.
func GetRecordedExtraUsageTags() map[string]string {
	recordedExtraUsageTagsLock.RLock()
	defer recordedExtraUsageTagsLock.RUnlock()

	// Return a copy so callers cannot mutate the internal state.
	result := make(map[string]string, len(recordedExtraUsageTags))
	for k, v := range recordedExtraUsageTags {
		result[k] = v
	}
	return result
}

// ResetRecordedExtraUsageTags clears the in-memory cache.
func ResetRecordedExtraUsageTags() {
	recordedExtraUsageTagsLock.Lock()
	defer recordedExtraUsageTagsLock.Unlock()
	recordedExtraUsageTags = make(map[string]string)
}
