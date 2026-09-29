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
	"fmt"
	"sort"
	"strings"
	"time"
)

// autoscalerMaxFailuresDisplayed is aligned with AUTOSCALER_MAX_FAILURES_DISPLAYED
// in python/ray/autoscaler/_private/constants.py.
const autoscalerMaxFailuresDisplayed = 20

// placementGroupBundleResourceName is aligned with
// PLACEMENT_GROUP_BUNDLE_RESOURCE_NAME in python/ray/_common/utils.py.
const placementGroupBundleResourceName = "bundle"

// formatInfoString ports format_info_string from
// python/ray/autoscaler/_private/util.py.
func formatInfoString(lm *loadMetricsSummary, as *autoscalerSummary, reportTime time.Time, gcsRequestTime, nonTerminatedNodesTime, autoscalerUpdateTime float64) string {
	// Python's header is "======== Autoscaler status: {time} ========" where
	// time is str(datetime) -> "2006-01-02 15:04:05.000000".
	header := fmt.Sprintf("======== Autoscaler status: %s ========", reportTime.Format("2006-01-02 15:04:05.000000"))
	separator := strings.Repeat("-", len(header))

	availableNodeReport := " (no active nodes)"
	if len(as.activeNodes) > 0 {
		var lines []string
		for _, nodeType := range sortedKeys(as.activeNodes) {
			lines = append(lines, fmt.Sprintf(" %d %s", as.activeNodes[nodeType], nodeType))
		}
		availableNodeReport = strings.Join(lines, "\n")
	}

	idleNodeReport := " (no idle nodes)"
	if len(as.idleNodes) > 0 {
		var lines []string
		for _, nodeType := range sortedKeys(as.idleNodes) {
			lines = append(lines, fmt.Sprintf(" %d %s", as.idleNodes[nodeType], nodeType))
		}
		idleNodeReport = strings.Join(lines, "\n")
	}

	var pendingLines []string
	for _, nodeType := range sortedKeys(as.pendingLaunches) {
		pendingLines = append(pendingLines, fmt.Sprintf(" %s, %d launching", nodeType, as.pendingLaunches[nodeType]))
	}
	for _, pn := range as.pendingNodes {
		pendingLines = append(pendingLines, fmt.Sprintf(" %s: %s, %s", pn[0], pn[1], strings.ToLower(pn[2])))
	}
	pendingReport := " (no pending nodes)"
	if len(pendingLines) > 0 {
		pendingReport = strings.Join(pendingLines, "\n")
	}

	var failureLines []string
	for _, fn := range as.failedNodes {
		failureLines = append(failureLines, fmt.Sprintf(" %s: NodeTerminated (ip: %s)", fn[1], fn[0]))
	}
	if as.nodeAvailabilitySummary != nil {
		records := append([]nodeAvailabilityRecord(nil), as.nodeAvailabilitySummary.nodeAvailabilities...)
		sort.SliceStable(records, func(i, j int) bool {
			return records[i].lastCheckedTimestamp < records[j].lastCheckedTimestamp
		})
		for _, rec := range records {
			if rec.isAvailable {
				continue
			}
			attempted := time.Unix(int64(rec.lastCheckedTimestamp), 0)
			formattedTime := fmt.Sprintf("%02d:%02d:%02d", attempted.Hour(), attempted.Minute(), attempted.Second())
			line := fmt.Sprintf(" %s: %s (latest_attempt: %s)", rec.nodeType, rec.unavailableNodeCategory, formattedTime)
			failureLines = append(failureLines, line)
		}
	}
	// Python: failure_lines[: -MAX_FAILURES_DISPLAYED : -1] keeps at most the
	// last MAX_FAILURES_DISPLAYED lines in reverse order.
	failureLines = lastFailuresReversed(failureLines, autoscalerMaxFailuresDisplayed)
	failureReport := "Recent failures:\n"
	if len(failureLines) > 0 {
		failureReport += strings.Join(failureLines, "\n")
	} else {
		failureReport += " (no failures)"
	}

	usageReport := getUsageReport(lm, false)
	constraintsReport := getConstraintReport(lm.requestDemand)
	demandReport := getDemandReport(lm)

	var b strings.Builder
	b.WriteString(fmt.Sprintf("%s\n", header))
	b.WriteString("Node status\n")
	b.WriteString(fmt.Sprintf("%s\n", separator))
	b.WriteString("Active:\n")
	b.WriteString(fmt.Sprintf("%s\n", availableNodeReport))
	if !as.legacy {
		b.WriteString("Idle:\n")
		b.WriteString(fmt.Sprintf("%s\n", idleNodeReport))
	}
	b.WriteString("Pending:\n")
	b.WriteString(fmt.Sprintf("%s\n", pendingReport))
	b.WriteString(fmt.Sprintf("%s\n\n", failureReport))
	b.WriteString("Resources\n")
	b.WriteString(fmt.Sprintf("%s\n", separator))
	b.WriteString("Total Usage:\n")
	b.WriteString(fmt.Sprintf("%s\n", usageReport))
	b.WriteString("From request_resources:\n")
	b.WriteString(fmt.Sprintf("%s\n", constraintsReport))
	b.WriteString("Pending Demands:\n")
	b.WriteString(demandReport)
	return strings.TrimSpace(b.String())
}

// lastFailuresReversed ports the Python slice `lines[:-n:-1]`: the last n
// elements in reverse order (or all elements reversed when len <= n).
func lastFailuresReversed(lines []string, n int) []string {
	if len(lines) == 0 {
		return nil
	}
	start := 0
	if len(lines) > n {
		start = len(lines) - n
	}
	out := make([]string, 0, len(lines)-start)
	for i := len(lines) - 1; i >= start; i-- {
		out = append(out, lines[i])
	}
	return out
}

func sortedKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func getUsageReport(lm *loadMetricsSummary, verbose bool) string {
	lines := parseUsage(lm.usage, verbose)
	var sb strings.Builder
	for _, line := range lines {
		sb.WriteString(" " + line + "\n")
	}
	return sb.String()
}

// memorySuffixes is aligned with MEMORY_SUFFIXES in
// python/ray/autoscaler/_private/util.py.
var memorySuffixes = []struct {
	suffix string
	bytes  float64
}{
	{"TiB", 1 << 40},
	{"GiB", 1 << 30},
	{"MiB", 1 << 20},
	{"KiB", 1 << 10},
}

func formatMemory(memBytes float64) string {
	for _, sfx := range memorySuffixes {
		if memBytes >= sfx.bytes {
			return fmt.Sprintf("%.2f%s", memBytes/sfx.bytes, sfx.suffix)
		}
	}
	return fmt.Sprintf("%d", int(memBytes)) + "B"
}

// parsePGResourceStr ports parse_placement_group_resource_str: returns
// (resourceName, pgName, isCountable). pgName empty means not a PG resource.
func parsePGResourceStr(resource string) (string, string, bool) {
	// Indexed pattern: (.+)_group_(\d+)_([0-9a-zA-Z]+)
	if i := strings.LastIndex(resource, "_group_"); i >= 0 {
		tail := resource[i+len("_group_"):]
		parts := strings.SplitN(tail, "_", 2)
		if len(parts) == 2 && allDigits(parts[0]) && alnumOnly(parts[1]) {
			return resource[:i], parts[1], false
		}
		if alnumOnly(parts[0]) {
			return resource[:i], parts[0], true
		}
	}
	return resource, "", true
}

func allDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

func alnumOnly(s string) bool {
	if s == "" {
		return false
	}
	for _, c := range s {
		if !((c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z')) {
			return false
		}
	}
	return true
}

// parseUsage ports parse_usage from python/ray/autoscaler/_private/util.py.
func parseUsage(usage map[string][2]float64, verbose bool) []string {
	pgUsage := map[string]float64{}
	pgTotal := map[string]float64{}
	for resource, uv := range usage {
		pgResource, pgName, isCountable := parsePGResourceStr(resource)
		if pgName == "" {
			continue
		}
		if _, ok := pgUsage[pgResource]; !ok {
			pgUsage[pgResource] = 0
		}
		if isCountable {
			pgUsage[pgResource] += uv[0]
			pgTotal[pgResource] += uv[1]
		}
	}
	keys := make([]string, 0, len(usage))
	for k := range usage {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var lines []string
	for _, resource := range keys {
		if strings.Contains(resource, "node:") {
			continue
		}
		_, pgName, _ := parsePGResourceStr(resource)
		if pgName != "" {
			continue
		}
		used, total := usage[resource][0], usage[resource][1]
		pgUsed, pgTotalV := 0.0, 0.0
		usedInPG := false
		if v, ok := pgUsage[resource]; ok {
			usedInPG = true
			pgUsed = v
			pgTotalV = pgTotal[resource]
			used = used - pgTotalV + pgUsed
		}
		if resource == "memory" || resource == "object_store_memory" {
			line := fmt.Sprintf("%s/%s %s", formatMemory(used), formatMemory(total), resource)
			if usedInPG {
				line += fmt.Sprintf(" (%s used of %s reserved in placement groups)", formatMemory(pgUsed), formatMemory(pgTotalV))
			}
			lines = append(lines, line)
		} else if strings.HasPrefix(resource, "accelerator_type:") && !verbose {
			continue
		} else {
			line := fmt.Sprintf("%g/%g %s", used, total, resource)
			if usedInPG {
				line += fmt.Sprintf(" (%g used of %g reserved in placement groups)", pgUsed, pgTotalV)
			}
			lines = append(lines, line)
		}
	}
	return lines
}

// getConstraintReport ports get_constraint_report.
func getConstraintReport(requestDemand []dictCount) string {
	var lines []string
	for _, d := range requestDemand {
		lines = append(lines, fmt.Sprintf(" %v: %d from request_resources()", d.bundle, d.count))
	}
	if len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return " (none)"
}

// formatPG ports format_pg: "bundle * count, ... (strategy)".
func formatPG(pg map[string]interface{}) string {
	strategy, _ := pg["strategy"].(string)
	var shapeStrs []string
	if bundles, ok := pg["bundles"].([]interface{}); ok {
		for _, b := range bundles {
			if pair, ok := b.([]interface{}); ok && len(pair) == 2 {
				bundle := fmt.Sprintf("%v", pair[0])
				count := int(toFloat(pair[1]))
				shapeStrs = append(shapeStrs, fmt.Sprintf("%s * %d", bundle, count))
			}
		}
	}
	return strings.Join(shapeStrs, ", ") + " (" + strategy + ")"
}

func getDemandReport(lm *loadMetricsSummary) string {
	var lines []string
	if len(lm.resourceDemand) > 0 {
		lines = append(lines, formatResourceDemandSummary(lm.resourceDemand)...)
	}
	for _, d := range lm.pgDemand {
		pgStr := formatPG(d.pg)
		lines = append(lines, fmt.Sprintf(" %s: %d+ pending placement groups", pgStr, d.count))
	}
	if len(lines) > 0 {
		return strings.Join(lines, "\n")
	}
	return " (no resource demands)"
}

// formatResourceDemandSummary ports format_resource_demand_summary.
func formatResourceDemandSummary(demand []dictCount) []string {
	bundleDemand := map[string]int{}
	pgBundleDemand := map[string]int{}
	for _, d := range demand {
		pgFiltered, usingPG := filterPGFromBundle(d.bundle)
		if usingPG {
			if _, ok := pgFiltered[placementGroupBundleResourceName]; ok {
				delete(pgFiltered, placementGroupBundleResourceName)
			}
		}
		if len(pgFiltered) == 0 {
			continue
		}
		key := sortedBundleKey(pgFiltered)
		bundleDemand[key] += d.count
		if usingPG {
			pgBundleDemand[key] += d.count
		}
	}
	// Deterministic order: sort by the serialized bundle.
	var keys []string
	for k := range bundleDemand {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var lines []string
	for _, k := range keys {
		bundle := bundleKeyToMap(k)
		line := fmt.Sprintf(" %v: %d+ pending tasks/actors", bundle, bundleDemand[k])
		if n, ok := pgBundleDemand[k]; ok {
			line += fmt.Sprintf(" (%d+ using placement groups)", n)
		}
		lines = append(lines, line)
	}
	return lines
}

// filterPGFromBundle ports filter_placement_group_from_bundle.
func filterPGFromBundle(bundle map[string]float64) (map[string]float64, bool) {
	usingPG := false
	result := map[string]float64{}
	for resource, count := range bundle {
		resourceName, pgName, _ := parsePGResourceStr(resource)
		result[resourceName] += count
		if pgName != "" {
			usingPG = true
		}
	}
	return result, usingPG
}

// sortedBundleKey serializes a bundle deterministically as
// "k1:v1,k2:v2,..." so identical bundles collapse.
func sortedBundleKey(bundle map[string]float64) string {
	var keys []string
	for k := range bundle {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var sb strings.Builder
	for i, k := range keys {
		if i > 0 {
			sb.WriteString(",")
		}
		sb.WriteString(fmt.Sprintf("%s:%g", k, bundle[k]))
	}
	return sb.String()
}

func bundleKeyToMap(key string) map[string]float64 {
	out := map[string]float64{}
	for _, part := range strings.Split(key, ",") {
		if idx := strings.Index(part, ":"); idx >= 0 {
			var v float64
			fmt.Sscanf(part[idx+1:], "%g", &v)
			out[part[:idx]] = v
		}
	}
	return out
}
