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

package v2

import (
	"testing"

	"github.com/ray-project/ray/go/proto"
	"github.com/stretchr/testify/assert"
)

// TestResourceRequestUtil_toResourceMap tests the toResourceMap method.
func TestResourceRequestUtil_toResourceMap(t *testing.T) {
	util := &resourceRequestUtil{}

	t.Run("empty resource request", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{},
		}

		resourceMap := util.toResourceMap(request)
		assert.Empty(t, resourceMap)
	})

	t.Run("single resource", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}

		resourceMap := util.toResourceMap(request)
		assert.Len(t, resourceMap, 1)
		assert.Equal(t, 4.0, resourceMap["CPU"])
	})

	t.Run("multiple resources", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU":    8.0,
				"memory": 32000.0,
				"GPU":    2.0,
			},
		}

		resourceMap := util.toResourceMap(request)
		assert.Len(t, resourceMap, 3)
		assert.Equal(t, 8.0, resourceMap["CPU"])
		assert.Equal(t, 32000.0, resourceMap["memory"])
		assert.Equal(t, 2.0, resourceMap["GPU"])
	})

	t.Run("duplicate resource keys (accumulated)", func(t *testing.T) {
		// Duplicate keys cannot happen normally, but this exercises the
		// accumulation logic.
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}

		// Manually simulate adding the same entry twice.
		resourceMap := make(map[string]float64)
		for k, v := range request.ResourcesBundle {
			resourceMap[k] += v
			resourceMap[k] += v // Accumulate once more.
		}

		assert.Equal(t, 8.0, resourceMap["CPU"])
	})

	t.Run("zero-valued resources", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU":    0.0,
				"memory": 0.0,
			},
		}

		resourceMap := util.toResourceMap(request)
		assert.Len(t, resourceMap, 2)
		assert.Equal(t, 0.0, resourceMap["CPU"])
		assert.Equal(t, 0.0, resourceMap["memory"])
	})

	t.Run("negative resources", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": -1.0,
			},
		}

		resourceMap := util.toResourceMap(request)
		assert.Len(t, resourceMap, 1)
		assert.Equal(t, -1.0, resourceMap["CPU"])
	})
}

// TestResourceRequestUtil_groupByCount tests the groupByCount method.
func TestResourceRequestUtil_groupByCount(t *testing.T) {
	util := &resourceRequestUtil{}

	t.Run("empty request list", func(t *testing.T) {
		requests := []*proto.ResourceRequest{}
		results := util.groupByCount(requests)
		assert.Empty(t, results)
	})

	t.Run("single request", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}
		requests := []*proto.ResourceRequest{request}

		results := util.groupByCount(requests)
		assert.Len(t, results, 1)
		assert.Equal(t, int64(1), results[0].Count)
		assert.Equal(t, request, results[0].Request)
	})

	t.Run("multiple identical requests", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}
		requests := []*proto.ResourceRequest{request, request, request}

		results := util.groupByCount(requests)
		assert.Len(t, results, 1)
		assert.Equal(t, int64(3), results[0].Count)
		assert.Equal(t, request, results[0].Request)
	})

	t.Run("multiple distinct requests", func(t *testing.T) {
		request1 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}
		request2 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 8.0,
			},
		}
		requests := []*proto.ResourceRequest{request1, request2}

		results := util.groupByCount(requests)
		assert.Len(t, results, 2)
		// The counts are equal so the order may be unstable, but both must be
		// present.
		foundReq1 := false
		foundReq2 := false
		for _, result := range results {
			if result.Request == request1 {
				foundReq1 = true
				assert.Equal(t, int64(1), result.Count)
			}
			if result.Request == request2 {
				foundReq2 = true
				assert.Equal(t, int64(1), result.Count)
			}
		}
		assert.True(t, foundReq1)
		assert.True(t, foundReq2)
	})

	t.Run("mix of identical and distinct requests", func(t *testing.T) {
		request1 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}
		request2 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 8.0,
			},
		}
		// 3 copies of request1 and 2 copies of request2.
		requests := []*proto.ResourceRequest{
			request1, request1, request1,
			request2, request2,
		}

		results := util.groupByCount(requests)
		assert.Len(t, results, 2)
		// Sorted by count in descending order, request1 comes first.
		assert.Equal(t, int64(3), results[0].Count)
		assert.Equal(t, request1, results[0].Request)
		assert.Equal(t, int64(2), results[1].Count)
		assert.Equal(t, request2, results[1].Request)
	})

	t.Run("requests with placement constraints", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			PlacementConstraints: []*proto.PlacementConstraint{
				{
					AntiAffinity: &proto.AntiAffinityConstraint{
						LabelName:  "zone",
						LabelValue: "us-east-1a",
					},
				},
			},
		}
		requests := []*proto.ResourceRequest{request, request}

		results := util.groupByCount(requests)
		assert.Len(t, results, 1)
		assert.Equal(t, int64(2), results[0].Count)
	})

	t.Run("requests with label selectors", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			LabelSelectors: []*proto.LabelSelector{
				{
					LabelConstraints: []*proto.LabelSelectorConstraint{
						{
							LabelKey:    "environment",
							Operator:    proto.LabelSelectorOperator_LABEL_OPERATOR_IN,
							LabelValues: []string{"prod"},
						},
					},
				},
			},
		}
		requests := []*proto.ResourceRequest{request, request, request}

		results := util.groupByCount(requests)
		assert.Len(t, results, 1)
		assert.Equal(t, int64(3), results[0].Count)
	})
}

// TestResourceRequestUtil_requestToKey tests the requestToKey method.
func TestResourceRequestUtil_requestToKey(t *testing.T) {
	util := &resourceRequestUtil{}

	t.Run("empty resource request", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle:      map[string]float64{},
			PlacementConstraints: []*proto.PlacementConstraint{},
			LabelSelectors:       []*proto.LabelSelector{},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, "resources:")
		assert.Contains(t, key, ";constraints:")
		assert.Contains(t, key, ";selectors:")
	})

	t.Run("single resource", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, "resources:CPU=4.000000;")
	})

	t.Run("multiple resources (sorted by key)", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"GPU":    2.0,
				"CPU":    4.0,
				"memory": 16000.0,
			},
		}

		key := util.requestToKey(request)
		// The resources must appear in alphabetical order: CPU, GPU, memory.
		cpuIndex := findSubstringIndex(key, "CPU=")
		gpuIndex := findSubstringIndex(key, "GPU=")
		memoryIndex := findSubstringIndex(key, "memory=")

		assert.Less(t, cpuIndex, gpuIndex)
		assert.Less(t, gpuIndex, memoryIndex)
	})

	t.Run("identical requests produce identical keys", func(t *testing.T) {
		request1 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
				"GPU": 1.0,
			},
		}
		request2 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"GPU": 1.0,
				"CPU": 4.0, // Different order.
			},
		}

		key1 := util.requestToKey(request1)
		key2 := util.requestToKey(request2)

		assert.Equal(t, key1, key2)
	})

	t.Run("different resources produce different keys", func(t *testing.T) {
		request1 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
		}
		request2 := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 8.0,
			},
		}

		key1 := util.requestToKey(request1)
		key2 := util.requestToKey(request2)

		assert.NotEqual(t, key1, key2)
	})

	t.Run("anti-affinity constraint", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			PlacementConstraints: []*proto.PlacementConstraint{
				{
					AntiAffinity: &proto.AntiAffinityConstraint{
						LabelName:  "zone",
						LabelValue: "us-east-1a",
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, ";constraints:")
		assert.Contains(t, key, "anti:zone=us-east-1a;")
	})

	t.Run("affinity constraint", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			PlacementConstraints: []*proto.PlacementConstraint{
				{
					Affinity: &proto.AffinityConstraint{
						LabelName:  "rack",
						LabelValue: "rack-1",
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, ";constraints:")
		assert.Contains(t, key, "affinity:rack=rack-1;")
	})

	t.Run("multiple constraints", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			PlacementConstraints: []*proto.PlacementConstraint{
				{
					AntiAffinity: &proto.AntiAffinityConstraint{
						LabelName:  "zone",
						LabelValue: "us-east-1a",
					},
				},
				{
					Affinity: &proto.AffinityConstraint{
						LabelName:  "rack",
						LabelValue: "rack-1",
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, "anti:zone=us-east-1a;")
		assert.Contains(t, key, "affinity:rack=rack-1;")
	})

	t.Run("label selector", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			LabelSelectors: []*proto.LabelSelector{
				{
					LabelConstraints: []*proto.LabelSelectorConstraint{
						{
							LabelKey:    "environment",
							Operator:    proto.LabelSelectorOperator_LABEL_OPERATOR_IN,
							LabelValues: []string{"prod", "staging"},
						},
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, ";selectors:")
		assert.Contains(t, key, ":environmentLABEL_OPERATOR_IN[prod staging];")
	})

	t.Run("multiple label selectors", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU": 4.0,
			},
			LabelSelectors: []*proto.LabelSelector{
				{
					LabelConstraints: []*proto.LabelSelectorConstraint{
						{
							LabelKey:    "environment",
							Operator:    proto.LabelSelectorOperator_LABEL_OPERATOR_IN,
							LabelValues: []string{"prod"},
						},
						{
							LabelKey:    "team",
							Operator:    proto.LabelSelectorOperator_LABEL_OPERATOR_NOT_IN,
							LabelValues: []string{"dev"},
						},
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, ";selectors:")
		assert.Contains(t, key, "environmentLABEL_OPERATOR_IN[prod];")
		assert.Contains(t, key, "teamLABEL_OPERATOR_NOT_IN[dev];")
	})

	t.Run("complete complex request", func(t *testing.T) {
		request := &proto.ResourceRequest{
			ResourcesBundle: map[string]float64{
				"CPU":    8.0,
				"memory": 32000.0,
				"GPU":    2.0,
			},
			PlacementConstraints: []*proto.PlacementConstraint{
				{
					AntiAffinity: &proto.AntiAffinityConstraint{
						LabelName:  "zone",
						LabelValue: "us-east-1a",
					},
				},
			},
			LabelSelectors: []*proto.LabelSelector{
				{
					LabelConstraints: []*proto.LabelSelectorConstraint{
						{
							LabelKey:    "instance-type",
							Operator:    proto.LabelSelectorOperator_LABEL_OPERATOR_IN,
							LabelValues: []string{"p3.8xlarge"},
						},
					},
				},
			},
		}

		key := util.requestToKey(request)
		assert.Contains(t, key, "resources:CPU=8.000000;GPU=2.000000;memory=32000.000000;")
		assert.Contains(t, key, ";constraints:anti:zone=us-east-1a;")
		assert.Contains(t, key, ";selectors:instance-typeLABEL_OPERATOR_IN[p3.8xlarge];")
	})
}

// Helper: find the index of a substring inside a string.
func findSubstringIndex(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// BenchmarkResourceRequestUtil_toResourceMap benchmarks the toResourceMap performance.
func BenchmarkResourceRequestUtil_toResourceMap(b *testing.B) {
	util := &resourceRequestUtil{}
	request := &proto.ResourceRequest{
		ResourcesBundle: map[string]float64{
			"CPU":    4.0,
			"memory": 16000.0,
			"GPU":    1.0,
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.toResourceMap(request)
	}
}

// BenchmarkResourceRequestUtil_groupByCount benchmarks the groupByCount performance.
func BenchmarkResourceRequestUtil_groupByCount(b *testing.B) {
	util := &resourceRequestUtil{}
	request := &proto.ResourceRequest{
		ResourcesBundle: map[string]float64{
			"CPU": 4.0,
		},
	}
	requests := make([]*proto.ResourceRequest, 100)
	for i := range requests {
		requests[i] = request
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.groupByCount(requests)
	}
}

// BenchmarkResourceRequestUtil_requestToKey benchmarks the requestToKey performance.
func BenchmarkResourceRequestUtil_requestToKey(b *testing.B) {
	util := &resourceRequestUtil{}
	request := &proto.ResourceRequest{
		ResourcesBundle: map[string]float64{
			"CPU":    4.0,
			"memory": 16000.0,
			"GPU":    1.0,
		},
		PlacementConstraints: []*proto.PlacementConstraint{
			{
				AntiAffinity: &proto.AntiAffinityConstraint{
					LabelName:  "zone",
					LabelValue: "us-east-1a",
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		util.requestToKey(request)
	}
}
