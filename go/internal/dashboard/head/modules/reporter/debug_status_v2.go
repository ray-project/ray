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

package reporter

import (
	"context"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/ray-project/ray/go/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// v2NodeStatusName mirrors the NodeStatus enum names in
// python/ray/autoscaler/v2 (RUNNING/DEAD/IDLE).
func v2NodeStatusName(s proto.NodeStatus) string {
	switch s {
	case proto.NodeStatus_RUNNING:
		return "RUNNING"
	case proto.NodeStatus_DEAD:
		return "DEAD"
	case proto.NodeStatus_IDLE:
		return "IDLE"
	}
	return "UNKNOWN"
}

// fetchClusterStatusV2 calls the GCS AutoscalerStateService.GetClusterStatus
// RPC (aligned with get_cluster_status in
// python/ray/autoscaler/v2/sdk.py). It returns nil on any failure; the caller
// falls back to the no-cluster-status message.
func (r *ReportHead) fetchClusterStatusV2(ctx context.Context, gcsAddress string) *proto.GetClusterStatusReply {
	if gcsAddress == "" {
		return nil
	}
	conn, err := grpc.DialContext(ctx, gcsAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil
	}
	defer conn.Close()
	stub := proto.NewAutoscalerStateServiceClient(conn)
	reply, err := stub.GetClusterStatus(ctx, &proto.GetClusterStatusRequest{})
	if err != nil {
		return nil
	}
	return reply
}

// v2NodeInfo is the parsed NodeInfo dataclass from
// python/ray/autoscaler/v2/schema.py.
type v2NodeInfo struct {
	instanceTypeName string
	rayNodeTypeName  string
	instanceID       string
	ipAddress        string
	nodeStatus       string
	nodeID           string
	resourceUsage    []v2ResourceUsage
	idleTimeMS       int64
	details          string
	nodeActivity     []string
}

type v2ResourceUsage struct {
	name  string
	total float64
	used  float64
}

func (n *v2NodeInfo) usageMap() map[string][2]float64 {
	out := map[string][2]float64{}
	for _, u := range n.resourceUsage {
		out[u.name] = [2]float64{u.used, u.total}
	}
	return out
}

// v2ClusterStatus mirrors the ClusterStatus dataclass.
type v2ClusterStatus struct {
	activeNodes     []*v2NodeInfo
	idleNodes       []*v2NodeInfo
	pendingNodes    []*v2NodeInfo
	failedNodes     []*v2NodeInfo
	pendingLaunches []v2LaunchRequest
	failedLaunches  []v2LaunchRequest
	clusterUsage    map[string][2]float64
	resourceDemand  []v2ResourceDemand
	requestTsS      float64
}

type v2LaunchRequest struct {
	rayNodeTypeName string
	count           int
	requestTsS      int64
	details         string
}

type v2ResourceDemand struct {
	bundle map[string]float64
	count  int
}

// parseClusterStatusV2 ports ClusterStatusParser.from_get_cluster_status_reply.
func parseClusterStatusV2(reply *proto.GetClusterStatusReply) *v2ClusterStatus {
	cs := &v2ClusterStatus{clusterUsage: map[string][2]float64{}, requestTsS: float64(time.Now().Unix())}
	state := reply.ClusterResourceState
	as := reply.AutoscalingState

	for _, ns := range state.NodeStates {
		node := parseV2Node(ns)
		if node.nodeStatus == "DEAD" {
			cs.failedNodes = append(cs.failedNodes, node)
		} else if node.nodeStatus == "IDLE" {
			cs.idleNodes = append(cs.idleNodes, node)
		} else {
			cs.activeNodes = append(cs.activeNodes, node)
		}
	}
	// Cluster resource usage: aggregate usage of non-dead nodes.
	for _, ns := range state.NodeStates {
		if ns.Status == proto.NodeStatus_DEAD {
			continue
		}
		d := map[string][2]float64{}
		for res, total := range ns.TotalResources {
			e := d[res]
			e[1] += total
			e[0] += total
			d[res] = e
		}
		for res, avail := range ns.AvailableResources {
			e := d[res]
			e[0] -= avail
			d[res] = e
		}
		for k, v := range d {
			e := cs.clusterUsage[k]
			cs.clusterUsage[k] = [2]float64{e[0] + v[0], e[1] + v[1]}
		}
	}
	for _, req := range as.PendingInstanceRequests {
		cs.pendingLaunches = append(cs.pendingLaunches, v2LaunchRequest{
			rayNodeTypeName: req.RayNodeTypeName,
			count:           int(req.Count),
			requestTsS:      req.RequestTs,
		})
	}
	for _, req := range as.FailedInstanceRequests {
		cs.failedLaunches = append(cs.failedLaunches, v2LaunchRequest{
			rayNodeTypeName: req.RayNodeTypeName,
			count:           int(req.Count),
			requestTsS:      req.StartTs,
			details:         req.Reason,
		})
	}
	for _, pn := range as.PendingInstances {
		cs.pendingNodes = append(cs.pendingNodes, &v2NodeInfo{
			instanceTypeName: pn.InstanceTypeName,
			rayNodeTypeName:  pn.RayNodeTypeName,
			instanceID:       pn.InstanceId,
			ipAddress:        pn.IpAddress,
			details:          pn.Details,
		})
	}
	// Resource demands.
	for _, rc := range state.PendingResourceRequests {
		if rc.Request == nil {
			continue
		}
		cs.resourceDemand = append(cs.resourceDemand, v2ResourceDemand{
			bundle: rc.Request.ResourcesBundle,
			count:  int(rc.Count),
		})
	}
	return cs
}

func parseV2Node(ns *proto.NodeState) *v2NodeInfo {
	nodeID := hex.EncodeToString(ns.NodeId)
	rayNodeType := ns.RayNodeTypeName
	if rayNodeType == "" {
		rayNodeType = "node_" + nodeID
	}
	node := &v2NodeInfo{
		instanceTypeName: ns.InstanceTypeName,
		rayNodeTypeName:  rayNodeType,
		instanceID:       ns.InstanceId,
		ipAddress:        ns.NodeIpAddress,
		nodeStatus:       v2NodeStatusName(ns.Status),
		nodeID:           nodeID,
		nodeActivity:     ns.NodeActivity,
	}
	if ns.Status != proto.NodeStatus_DEAD {
		for res, total := range ns.TotalResources {
			node.resourceUsage = append(node.resourceUsage, v2ResourceUsage{name: res, total: total, used: total})
		}
		for res, avail := range ns.AvailableResources {
			for i := range node.resourceUsage {
				if node.resourceUsage[i].name == res {
					node.resourceUsage[i].used = node.resourceUsage[i].total - avail
					break
				}
			}
		}
		if ns.Status == proto.NodeStatus_IDLE {
			node.idleTimeMS = ns.IdleDurationMs
		}
	}
	return node
}

// formatClusterStatusV2 ports ClusterStatusFormatter.format.
func formatClusterStatusV2(cs *v2ClusterStatus) string {
	requestTs := cs.requestTsS
	header := fmt.Sprintf("======== Autoscaler status: %s ========", time.Unix(int64(requestTs), 0).Format("2006-01-02 15:04:05.000000"))
	separator := strings.Repeat("-", len(header))

	availableNodeReport := v2NodeReport(cs.activeNodes)
	idleNodeReport := v2NodeReport(cs.idleNodes)
	pendingReport := v2PendingReport(cs.pendingNodes, cs.pendingLaunches)
	failureReport := v2FailureReport(cs.failedLaunches, cs.failedNodes)
	clusterUsageReport := v2ClusterUsageReport(cs.clusterUsage)
	constraintsReport := " (none)"
	demandReport := v2DemandReport(cs.resourceDemand)

	lines := []string{
		header,
		"Node status",
		separator,
		"Active:",
		availableNodeReport,
		"Idle:",
		idleNodeReport,
		"Pending:",
		pendingReport,
		failureReport,
		"",
		"Resources",
		separator,
		"Total Usage:",
		clusterUsageReport,
		"From request_resources:",
		constraintsReport,
		"Pending Demands:",
		demandReport,
		"",
	}
	return strings.TrimSpace(strings.Join(lines, "\n"))
}

func v2NodeReport(nodes []*v2NodeInfo) string {
	counts := map[string]int{}
	for _, n := range nodes {
		counts[n.rayNodeTypeName]++
	}
	if len(counts) == 0 {
		return " (no active nodes)"
	}
	var lines []string
	for _, t := range sortedKeys(counts) {
		lines = append(lines, fmt.Sprintf(" %d %s", counts[t], t))
	}
	return strings.Join(lines, "\n")
}

func v2PendingReport(pendingNodes []*v2NodeInfo, pendingLaunches []v2LaunchRequest) string {
	var lines []string
	launchCounts := map[string]int{}
	for _, l := range pendingLaunches {
		launchCounts[l.rayNodeTypeName] += l.count
	}
	for _, t := range sortedKeys(launchCounts) {
		lines = append(lines, fmt.Sprintf(" %s, %d launching", t, launchCounts[t]))
	}
	for _, n := range pendingNodes {
		lines = append(lines, fmt.Sprintf(" %s: %s, %s", n.instanceID, n.rayNodeTypeName, strings.ToLower(n.details)))
	}
	if len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return " (no pending nodes)"
}

func v2FailureReport(failedLaunches []v2LaunchRequest, failedNodes []*v2NodeInfo) string {
	type failureLine struct {
		ts int64
		s  string
	}
	var lines []failureLine
	for _, l := range failedLaunches {
		attempted := time.Unix(l.requestTsS, 0)
		formatted := fmt.Sprintf("%02d:%02d:%02d", attempted.Hour(), attempted.Minute(), attempted.Second())
		lines = append(lines, failureLine{ts: l.requestTsS, s: fmt.Sprintf(" %s: LaunchFailed (latest_attempt: %s)", l.rayNodeTypeName, formatted)})
	}
	for _, n := range failedNodes {
		lines = append(lines, failureLine{s: fmt.Sprintf(" %s: NodeTerminated (instance_id: %s)", n.rayNodeTypeName, n.instanceID)})
	}
	// Sort descending by request ts (stable: launch lines keep relative order
	// before node lines because node lines have zero ts and are sorted after
	// when ts equal? Python sorts failed_launches only; failed_nodes are
	// appended after in the order they appear).
	sort.SliceStable(lines, func(i, j int) bool {
		return lines[i].ts > lines[j].ts
	})
	out := make([]string, 0, len(lines))
	for _, l := range lines {
		out = append(out, l.s)
	}
	if len(out) > autoscalerMaxFailuresDisplayed {
		out = out[:autoscalerMaxFailuresDisplayed]
	}
	failureReport := "Recent failures:\n"
	if len(out) > 0 {
		failureReport += strings.Join(out, "\n")
	} else {
		failureReport += " (no failures)"
	}
	return failureReport
}

func v2ClusterUsageReport(usage map[string][2]float64) string {
	lines := parseUsage(usage, false)
	out := make([]string, 0, len(lines)+1)
	for _, line := range lines {
		out = append(out, " "+line)
	}
	out = append(out, "")
	return strings.Join(out, "\n")
}

func v2DemandReport(demands []v2ResourceDemand) string {
	dc := make([]dictCount, 0, len(demands))
	for _, d := range demands {
		dc = append(dc, dictCount{bundle: d.bundle, count: d.count})
	}
	lines := formatResourceDemandSummary(dc)
	if len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return " (no resource demands)"
}
