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
	"fmt"
	"sort"
	"strings"

	"github.com/ray-project/ray/go/proto"
)

var ResourceRequestUtil = &resourceRequestUtil{}

type resourceRequestUtil struct{}

// toResourceMap converts a ResourceRequest into a resource-name -> quantity map.
func (util *resourceRequestUtil) toResourceMap(request *proto.ResourceRequest) map[string]float64 {
	resourceMap := make(map[string]float64)
	for k, v := range request.ResourcesBundle {
		resourceMap[k] += v
	}
	return resourceMap
}

// groupByCount aggregates resource requests by count.
// Identical ResourceRequests are merged into one ResourceRequestByCount object.
func (util *resourceRequestUtil) groupByCount(requests []*proto.ResourceRequest) []*proto.ResourceRequestByCount {
	// Track the requests already seen, keyed by the string produced by
	// requestToKey.
	resourceRequestsByCount := make(map[*proto.ResourceRequest]int64)
	seenRequests := make(map[string]*proto.ResourceRequest)

	for _, req := range requests {
		key := util.requestToKey(req)
		if existingReq, ok := seenRequests[key]; ok {
			resourceRequestsByCount[existingReq]++
		} else {
			seenRequests[key] = req
			resourceRequestsByCount[req] = 1
		}
	}

	// Build the result list.
	results := make([]*proto.ResourceRequestByCount, 0, len(seenRequests))
	for req, count := range resourceRequestsByCount {
		results = append(results, &proto.ResourceRequestByCount{
			Request: req,
			Count:   count,
		})
	}

	// Sort by count in descending order so the most frequent requests come first.
	sort.Slice(results, func(i, j int) bool {
		return results[i].Count > results[j].Count
	})

	return results
}

// requestToKey converts a ResourceRequest into a unique string key.
func (util *resourceRequestUtil) requestToKey(req *proto.ResourceRequest) string {
	// Build a string representation covering every relevant field.
	var sb strings.Builder

	// Resource bundle.
	sb.WriteString("resources:")
	// Sort by key for deterministic output.
	keys := make([]string, 0, len(req.ResourcesBundle))
	for k := range req.ResourcesBundle {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		sb.WriteString(fmt.Sprintf("%s=%f;", k, req.ResourcesBundle[k]))
	}

	// Placement constraints.
	sb.WriteString(";constraints:")
	for _, pc := range req.PlacementConstraints {
		if pc.AntiAffinity != nil {
			sb.WriteString(fmt.Sprintf("anti:%s=%s;", pc.AntiAffinity.LabelName, pc.AntiAffinity.LabelValue))
		}
		if pc.Affinity != nil {
			sb.WriteString(fmt.Sprintf("affinity:%s=%s;", pc.Affinity.LabelName, pc.Affinity.LabelValue))
		}
	}

	// Label selectors.
	sb.WriteString(";selectors:")
	for _, selector := range req.LabelSelectors {
		for _, constraint := range selector.LabelConstraints {
			sb.WriteString(fmt.Sprintf("%s%s%v;",
				constraint.LabelKey,
				constraint.Operator.String(),
				constraint.LabelValues))
		}
	}

	return sb.String()
}
