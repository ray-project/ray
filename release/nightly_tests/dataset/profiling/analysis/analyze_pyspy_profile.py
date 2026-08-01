#!/usr/bin/env python3
# ABOUTME: Analyzes py-spy speedscope JSON profiles to extract leaf (self-time) and inclusive-time statistics.
# ABOUTME: Supports filtering by thread name and grouping leaf functions into configurable categories.

"""
Usage:
    python analyze_pyspy_profile.py <speedscope.json> [--thread <substring>] [--top N] [--categories FILE]

Examples:
    # Top 30 leaf functions for the StreamingExecutor thread:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --top 30

    # All threads, top 20 (default):
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json

    # Inclusive time for specific functions:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --inclusive invoke_detectors,detect,get_task

    # With custom category groupings (JSON file mapping category -> [function names]):
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --categories categories.json

    # Top 10 caller stacks for readinto:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --callers readinto --top 10

    # Caller stacks with max 8 frames of context:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --callers readinto --depth 8

    # Call graph (callers + callees) for detect:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --call-graph detect

    # Call graph with deeper context:
    python analyze_pyspy_profile.py pyspy_driver.speedscope.json --thread StreamingExecutor --call-graph detect --depth 8 --top 10
"""

import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple


def load_profile(path: str) -> dict:
    with open(path, encoding="utf-8", errors="replace") as f:
        return json.load(f)


def find_matching_profiles(
    data: dict, thread_filter: Optional[str]
) -> List[Tuple[str, dict]]:
    """Return (name, profile) pairs matching the thread filter substring."""
    results = []
    for profile in data["profiles"]:
        name = profile.get("name", "")
        if thread_filter is None or thread_filter.lower() in name.lower():
            results.append((name, profile))
    return results


def analyze_self_time(
    frames: List[dict], samples: List[List[int]], weights: List[float]
) -> Dict[int, float]:
    """Count weighted self-time for each frame index (leaf of each sample stack)."""
    self_time = defaultdict(float)
    for stack, weight in zip(samples, weights):
        if stack:
            leaf = stack[-1]
            self_time[leaf] += weight
    return dict(self_time)


def analyze_inclusive_time(
    frames: List[dict],
    samples: List[List[int]],
    weights: List[float],
    target_names: Set[str],
) -> Dict[str, Tuple[float, int]]:
    """Count weighted inclusive time for functions matching target_names.

    Returns {name: (total_weight, sample_count)}.
    """
    # Build reverse lookup: frame_index -> name (only for targets)
    target_indices = {}
    for idx, frame in enumerate(frames):
        if frame["name"] in target_names:
            target_indices[idx] = frame["name"]

    inclusive = defaultdict(lambda: [0.0, 0])
    for stack, weight in zip(samples, weights):
        # A function gets inclusive credit once per sample, even if it
        # appears multiple times in the stack (recursion).
        seen = set()
        for frame_idx in stack:
            if frame_idx in target_indices:
                name = target_indices[frame_idx]
                if name not in seen:
                    seen.add(name)
                    inclusive[name][0] += weight
                    inclusive[name][1] += 1

    return {name: (vals[0], vals[1]) for name, vals in inclusive.items()}


def categorize_functions(
    self_time_by_idx: Dict[int, float],
    frames: List[dict],
    categories: Dict[str, List[str]],
) -> Dict[str, Tuple[float, List[str]]]:
    """Group leaf functions into categories.

    categories: {category_name: [function_name, ...]}
    Returns: {category_name: (total_weight, [matched_function_names])}
    """
    # Build function_name -> category lookup
    func_to_cat = {}
    for cat, funcs in categories.items():
        for fn in funcs:
            func_to_cat[fn] = cat

    cat_totals = defaultdict(lambda: [0.0, []])
    uncategorized_total = 0.0

    for frame_idx, weight in self_time_by_idx.items():
        fname = frames[frame_idx]["name"]
        cat = func_to_cat.get(fname)
        if cat:
            cat_totals[cat][0] += weight
            if fname not in cat_totals[cat][1]:
                cat_totals[cat][1].append(fname)
        else:
            uncategorized_total += weight

    result = {cat: (vals[0], vals[1]) for cat, vals in cat_totals.items()}
    if uncategorized_total > 0:
        result["Other"] = (uncategorized_total, [])
    return result


def analyze_caller_stacks(
    frames: List[dict],
    samples: List[List[int]],
    weights: List[float],
    target_name: str,
    max_depth: Optional[int] = None,
    thread_names: Optional[List[str]] = None,
) -> List[Tuple[Tuple[str, ...], float, int, str]]:
    """Find unique caller stacks for a target function.

    For each sample where target_name appears, extracts the stack from
    the root down to (and including) the target function. Groups identical
    stacks by (chain, thread_name) and sums their weights.

    Returns [(stack_tuple, total_weight, sample_count, thread_name), ...]
    sorted by descending weight.
    """
    # Build frame_index -> bool for target function
    target_indices = set()
    for idx, frame in enumerate(frames):
        if frame["name"] == target_name:
            target_indices.add(idx)

    # Key includes thread name so identical stacks from different threads
    # are kept separate.
    stack_weights = defaultdict(lambda: [0.0, 0])
    for i, (stack, weight) in enumerate(zip(samples, weights)):
        # Find the deepest (last) occurrence of the target in this stack.
        # Using the deepest handles recursion — we get the full caller chain.
        target_pos = None
        for pos in range(len(stack) - 1, -1, -1):
            if stack[pos] in target_indices:
                target_pos = pos
                break
        if target_pos is None:
            continue

        # Extract caller chain: root -> ... -> target
        caller_chain = stack[: target_pos + 1]
        # Apply depth limit (keep the N frames closest to the target)
        if max_depth is not None and len(caller_chain) > max_depth:
            caller_chain = caller_chain[-(max_depth):]

        # Convert to tuple of function names for grouping
        chain_key = tuple(frames[idx]["name"] for idx in caller_chain)
        tname = thread_names[i] if thread_names else ""
        group_key = (chain_key, tname)
        stack_weights[group_key][0] += weight
        stack_weights[group_key][1] += 1

    result = [
        (chain, vals[0], vals[1], tname)
        for (chain, tname), vals in stack_weights.items()
    ]
    result.sort(key=lambda x: -x[1])
    return result


def analyze_callee_stacks(
    frames: List[dict],
    samples: List[List[int]],
    weights: List[float],
    target_name: str,
    max_depth: Optional[int] = None,
) -> List[Tuple[Tuple[str, ...], float, int]]:
    """Find unique callee stacks for a target function.

    For each sample where target_name appears, extracts the stack from
    the target function down to the leaf (what it calls). Uses the
    shallowest (first) occurrence to maximize callee chain length.
    Groups identical stacks and sums their weights.

    Returns [(stack_tuple, total_weight, sample_count), ...] sorted by
    descending weight.
    """
    target_indices = set()
    for idx, frame in enumerate(frames):
        if frame["name"] == target_name:
            target_indices.add(idx)

    stack_weights = defaultdict(lambda: [0.0, 0])
    for stack, weight in zip(samples, weights):
        # Find the shallowest (first) occurrence of the target.
        target_pos = None
        for pos in range(len(stack)):
            if stack[pos] in target_indices:
                target_pos = pos
                break
        if target_pos is None:
            continue

        # Extract callee chain: target -> ... -> leaf
        callee_chain = stack[target_pos:]
        # Apply depth limit (keep the N frames closest to the target)
        if max_depth is not None and len(callee_chain) > max_depth:
            callee_chain = callee_chain[:max_depth]

        chain_key = tuple(frames[idx]["name"] for idx in callee_chain)
        stack_weights[chain_key][0] += weight
        stack_weights[chain_key][1] += 1

    result = [(chain, vals[0], vals[1]) for chain, vals in stack_weights.items()]
    result.sort(key=lambda x: -x[1])
    return result


DEFAULT_CATEGORIES = {
    "Actor state": [
        "__hash__",
        "_actor_id",
        "_get_local_state",
        "refresh_actor_state",
        "_update_running_actor_state",
        "running_actors",
        "get_actor_id",
        "max_tasks_in_flight_per_actor",
    ],
    "HTTP/detect": [
        "readinto",
        "_make_http_get_request",
        "ray_address_to_api_server_url",
        "internal_kv_get_with_retry",
        "_initialize_internal_kv",
        "check_connected",
    ],
    "List/set comprehensions": [
        "<listcomp>",
        "<setcomp>",
        "<dictcomp>",
    ],
    "HeapDict": [
        "_decrease_key",
        "_swap",
        "__setitem__",
        "_build_actor_busyness_heap",
    ],
    "Scheduling misc": [
        "wait",
        "safe_round",
        "select_operator_to_run",
    ],
}


def format_table(
    headers: List[str], rows: List[List[str]], align: Optional[List[str]] = None
) -> str:
    """Format a markdown table with column alignment.

    align: list of 'l' or 'r' per column. Defaults to left.
    """
    if align is None:
        align = ["l"] * len(headers)

    col_widths = [len(h) for h in headers]
    for row in rows:
        for i, cell in enumerate(row):
            col_widths[i] = max(col_widths[i], len(cell))

    def pad(s, w, a):
        return s.rjust(w) if a == "r" else s.ljust(w)

    sep_parts = []
    for i, w in enumerate(col_widths):
        if align[i] == "r":
            sep_parts.append("-" * (w - 1) + ":")
        else:
            sep_parts.append("-" * w)

    header_line = (
        "| "
        + " | ".join(pad(h, col_widths[i], align[i]) for i, h in enumerate(headers))
        + " |"
    )
    sep_line = "| " + " | ".join(sep_parts) + " |"
    data_lines = []
    for row in rows:
        data_lines.append(
            "| "
            + " | ".join(
                pad(cell, col_widths[i], align[i]) for i, cell in enumerate(row)
            )
            + " |"
        )

    return "\n".join([header_line, sep_line] + data_lines)


def main():
    parser = argparse.ArgumentParser(
        description="Analyze py-spy speedscope JSON profiles"
    )
    parser.add_argument("profile", help="Path to speedscope JSON file")
    parser.add_argument(
        "--thread", "-t", help="Filter profiles by thread name substring"
    )
    parser.add_argument(
        "--top", "-n", type=int, default=20, help="Number of top leaf functions to show"
    )
    parser.add_argument(
        "--inclusive",
        "-i",
        help="Comma-separated function names to compute inclusive time for",
    )
    parser.add_argument(
        "--categories",
        "-c",
        help="JSON file with category groupings: {category: [func_names]}",
    )
    parser.add_argument(
        "--no-categories",
        action="store_true",
        help="Skip category breakdown (show only leaf functions)",
    )
    parser.add_argument(
        "--callers",
        help="Show top caller stacks for this function name",
    )
    parser.add_argument(
        "--depth",
        type=int,
        default=5,
        help="Max stack depth to show for --callers (default: 5, 0=unlimited)",
    )
    parser.add_argument(
        "--call-graph",
        help="Show callers, self-time, and callees for this function name",
    )
    parser.add_argument(
        "--full",
        action="store_true",
        help="Print all unique stack traces sorted by weight",
    )
    parser.add_argument(
        "--list-threads",
        action="store_true",
        help="List all threads/profiles and exit",
    )

    args = parser.parse_args()

    data = load_profile(args.profile)
    frames = data["shared"]["frames"]

    if args.list_threads:
        print(f"\nThreads in {args.profile}:\n")
        entries = []
        for i, p in enumerate(data["profiles"]):
            name = p.get("name", "(unnamed)")
            n_samples = len(p.get("samples", []))
            total_weight = sum(p.get("weights", []))
            entries.append((i, n_samples, total_weight, name))
        grand_total = sum(w for _, _, w, _ in entries)
        entries.sort(key=lambda e: -e[2])
        rows = []
        for i, n_samples, total_weight, name in entries:
            pct = 100 * total_weight / grand_total if grand_total else 0
            rows.append([str(n_samples), f"{total_weight:.2f}s", f"{pct:.1f}%", name])
        print(
            format_table(
                ["Samples", "CPU time", "% total", "Thread name"],
                rows,
                ["r", "r", "r", "l"],
            )
        )
        return

    # Find matching profiles
    matches = find_matching_profiles(data, args.thread)
    if not matches:
        print(f"No profiles matching thread filter '{args.thread}'", file=sys.stderr)
        print("Use --list-threads to see available threads", file=sys.stderr)
        sys.exit(1)

    # Merge samples from all matching profiles
    all_samples = []
    all_weights = []
    all_thread_names = []
    profile_names = []
    for name, profile in matches:
        profile_names.append(name)
        n_samples = len(profile["samples"])
        all_samples.extend(profile["samples"])
        all_weights.extend(profile["weights"])
        all_thread_names.extend([name] * n_samples)

    total_weight = sum(all_weights)
    total_samples = len(all_samples)

    print(f"\nProfile: {args.profile}")
    print(f"Thread filter: {args.thread or '(all threads)'}")
    print(f"Matching profiles: {len(matches)}")
    for name in profile_names:
        print(f"  - {name}")
    print(f"Total samples: {total_samples:,}")
    print(f"Total CPU time: {total_weight:.2f}s")

    # If --callers is the only analysis requested, skip leaf/category output
    callers_only = (args.callers or args.full or args.call_graph) and not args.inclusive

    # --- Self-time (leaf function) analysis ---
    self_time = analyze_self_time(frames, all_samples, all_weights)

    # Sort by descending self-time
    sorted_self = sorted(self_time.items(), key=lambda x: -x[1])

    if not callers_only:
        print(f"\n{'=' * 72}")
        print(f"Top {args.top} Leaf Functions (self-time)")
        print(f"{'=' * 72}\n")

        rows = []
        for frame_idx, weight in sorted_self[: args.top]:
            frame = frames[frame_idx]
            pct = 100 * weight / total_weight if total_weight else 0
            # Count samples where this frame is the leaf
            sample_count = sum(1 for s in all_samples if s and s[-1] == frame_idx)
            loc = f"{Path(frame.get('file', '')).name}:{frame.get('line', '?')}"
            rows.append(
                [
                    f"{sample_count:,}",
                    f"{pct:.1f}%",
                    f"{weight:.2f}s",
                    frame["name"],
                    loc,
                ]
            )

        print(
            format_table(
                ["Samples", "Self %", "Self time", "Function", "Location"],
                rows,
                ["r", "r", "r", "l", "l"],
            )
        )

        # --- Category breakdown ---
        if not args.no_categories:
            if args.categories:
                with open(args.categories) as f:
                    categories = json.load(f)
            else:
                categories = DEFAULT_CATEGORIES

            print(f"\n{'=' * 72}")
            print("Self-Time by Category")
            print(f"{'=' * 72}\n")

            cat_results = categorize_functions(self_time, frames, categories)
            sorted_cats = sorted(cat_results.items(), key=lambda x: -x[1][0])

            rows = []
            for cat, (weight, func_names) in sorted_cats:
                pct = 100 * weight / total_weight if total_weight else 0
                funcs_str = ", ".join(func_names[:6])
                if len(func_names) > 6:
                    funcs_str += ", ..."
                rows.append([f"{pct:.1f}%", f"{weight:.2f}s", cat, funcs_str])

            print(
                format_table(
                    ["Self %", "Self time", "Category", "Functions"],
                    rows,
                    ["r", "r", "l", "l"],
                )
            )

    # --- Caller stack analysis ---
    if args.callers:
        depth = args.depth if args.depth != 0 else None
        callers_top = args.top if args.top != 20 else 5
        caller_stacks = analyze_caller_stacks(
            frames,
            all_samples,
            all_weights,
            args.callers,
            depth,
            thread_names=all_thread_names,
        )

        if not caller_stacks:
            print(
                f"\nFunction '{args.callers}' not found in any sample stack.",
                file=sys.stderr,
            )
        else:
            # Total weight across all stacks containing the target
            callers_total_weight = sum(w for _, w, _, _ in caller_stacks)
            callers_total_samples = sum(c for _, _, c, _ in caller_stacks)

            print(f"\n{'=' * 72}")
            depth_str = f", depth={args.depth}" if depth else ""
            print(f"Top {callers_top} Caller Stacks for '{args.callers}'{depth_str}")
            print(f"{'=' * 72}")
            print(
                f"\n{callers_total_samples:,} samples ({callers_total_weight:.2f}s) "
                f"contain '{args.callers}' "
                f"({100 * callers_total_weight / total_weight:.1f}% of thread CPU)\n"
            )

            for rank, (chain, weight, count, tname) in enumerate(
                caller_stacks[:callers_top], 1
            ):
                pct_of_thread = 100 * weight / total_weight if total_weight else 0
                pct_of_func = (
                    100 * weight / callers_total_weight if callers_total_weight else 0
                )
                # Extract short thread name from "Process NNNN Thread 0xHEX "name""
                short_tname = tname.rsplit('"', 2)[-2] if '"' in tname else tname
                print(
                    f"--- Stack #{rank}: {count:,} samples, "
                    f"{weight:.2f}s ({pct_of_thread:.1f}% of thread, "
                    f"{pct_of_func:.1f}% of '{args.callers}') "
                    f"[{short_tname}] ---"
                )
                # Print stack with indentation showing call depth
                for depth_idx, fname in enumerate(chain):
                    if depth_idx == len(chain) - 1:
                        print(f"    {'  ' * depth_idx}** {fname} **")
                    else:
                        print(f"    {'  ' * depth_idx}{fname}")
                print()

    # --- Call graph analysis ---
    if args.call_graph:
        target = args.call_graph
        depth = args.depth if args.depth != 0 else None
        cg_top = args.top if args.top != 20 else 5

        # Find all frame indices matching the target
        target_indices = set()
        for idx, frame in enumerate(frames):
            if frame["name"] == target:
                target_indices.add(idx)

        if not target_indices:
            print(f"\nFunction '{target}' not found in any frame.", file=sys.stderr)
        else:
            # Compute self-time: samples where target is the leaf
            func_self_weight = 0.0
            func_self_count = 0
            for stack, weight in zip(all_samples, all_weights):
                if stack and stack[-1] in target_indices:
                    func_self_weight += weight
                    func_self_count += 1

            # Compute inclusive time: samples where target appears anywhere
            func_inclusive_weight = 0.0
            func_inclusive_count = 0
            for stack, weight in zip(all_samples, all_weights):
                for frame_idx in stack:
                    if frame_idx in target_indices:
                        func_inclusive_weight += weight
                        func_inclusive_count += 1
                        break

            # Get caller and callee stacks
            # For callers: use shallowest (first) occurrence to be consistent
            # with callee analysis (split at the same point)
            caller_weights = defaultdict(lambda: [0.0, 0])
            callee_weights = defaultdict(lambda: [0.0, 0])
            for stack, weight in zip(all_samples, all_weights):
                # Find shallowest occurrence
                target_pos = None
                for pos in range(len(stack)):
                    if stack[pos] in target_indices:
                        target_pos = pos
                        break
                if target_pos is None:
                    continue

                # Callers: root -> ... -> target
                caller_chain = stack[: target_pos + 1]
                if depth is not None and len(caller_chain) > depth:
                    caller_chain = caller_chain[-depth:]
                caller_key = tuple(frames[idx]["name"] for idx in caller_chain)
                caller_weights[caller_key][0] += weight
                caller_weights[caller_key][1] += 1

                # Callees: target -> ... -> leaf
                callee_chain = stack[target_pos:]
                if depth is not None and len(callee_chain) > depth:
                    callee_chain = callee_chain[:depth]
                callee_key = tuple(frames[idx]["name"] for idx in callee_chain)
                callee_weights[callee_key][0] += weight
                callee_weights[callee_key][1] += 1

            sorted_callers = sorted(caller_weights.items(), key=lambda x: -x[1][0])
            sorted_callees = sorted(callee_weights.items(), key=lambda x: -x[1][0])

            # --- Print header ---
            print(f"\n{'=' * 72}")
            depth_str = f", depth={args.depth}" if depth else ""
            print(f"Call Graph for '{target}'{depth_str}")
            print(f"{'=' * 72}")

            self_pct = 100 * func_self_weight / total_weight if total_weight else 0
            inc_pct = 100 * func_inclusive_weight / total_weight if total_weight else 0
            print(
                f"\nInclusive: {func_inclusive_count:,} samples, "
                f"{func_inclusive_weight:.2f}s ({inc_pct:.1f}% of thread)"
            )
            print(
                f"Self-time: {func_self_count:,} samples, "
                f"{func_self_weight:.2f}s ({self_pct:.1f}% of thread)"
            )

            # --- Callers ---
            print(
                f"\n--- Callers ({len(sorted_callers):,} unique chains, "
                f"showing top {min(cg_top, len(sorted_callers))}) ---\n"
            )

            for rank, (chain, (weight, count)) in enumerate(sorted_callers[:cg_top], 1):
                pct = (
                    100 * weight / func_inclusive_weight if func_inclusive_weight else 0
                )
                print(f"  #{rank}: {count:,} samples, {weight:.2f}s ({pct:.1f}%)")
                for d, fname in enumerate(chain):
                    if fname == target:
                        print(f"      {'  ' * d}** {fname} **")
                    else:
                        print(f"      {'  ' * d}{fname}")
                print()

            # --- Callees ---
            # Separate self-time entries (chain length 1 = target is the leaf)
            callee_stacks = [(c, w, n) for c, (w, n) in sorted_callees if len(c) > 1]
            self_entries = [(c, w, n) for c, (w, n) in sorted_callees if len(c) == 1]

            print(
                f"--- Callees ({len(callee_stacks):,} unique chains, "
                f"showing top {min(cg_top, len(callee_stacks))}) ---\n"
            )

            if self_entries:
                sw = sum(w for _, w, _ in self_entries)
                sc = sum(n for _, _, n in self_entries)
                pct = 100 * sw / func_inclusive_weight if func_inclusive_weight else 0
                print(
                    f"  (self): {sc:,} samples, {sw:.2f}s ({pct:.1f}%) "
                    f"-- '{target}' is the leaf\n"
                )

            for rank, (chain, weight, count) in enumerate(callee_stacks[:cg_top], 1):
                pct = (
                    100 * weight / func_inclusive_weight if func_inclusive_weight else 0
                )
                print(f"  #{rank}: {count:,} samples, {weight:.2f}s ({pct:.1f}%)")
                for d, fname in enumerate(chain):
                    if d == 0:
                        print(f"      ** {fname} **")
                    elif d == len(chain) - 1:
                        print(f"      {'  ' * d}-> {fname}")
                    else:
                        print(f"      {'  ' * d}{fname}")
                print()

    # --- Full stack trace dump ---
    if args.full:
        # Group all samples by their complete stack trace
        stack_groups = defaultdict(lambda: [0.0, 0])
        for stack, weight in zip(all_samples, all_weights):
            key = tuple(frames[idx]["name"] for idx in stack)
            stack_groups[key][0] += weight
            stack_groups[key][1] += 1

        sorted_stacks = sorted(stack_groups.items(), key=lambda x: -x[1][0])
        full_top = args.top if args.top != 20 else len(sorted_stacks)

        print(f"\n{'=' * 72}")
        print(
            f"All Unique Stack Traces by Weight ({len(sorted_stacks):,} unique stacks)"
        )
        print(f"{'=' * 72}\n")

        for rank, (chain, (weight, count)) in enumerate(sorted_stacks[:full_top], 1):
            pct = 100 * weight / total_weight if total_weight else 0
            print(
                f"--- Stack #{rank}: {count:,} samples, {weight:.2f}s ({pct:.1f}%) ---"
            )
            for depth_idx, fname in enumerate(chain):
                if depth_idx == len(chain) - 1:
                    print(f"    {'  ' * depth_idx}** {fname} **")
                else:
                    print(f"    {'  ' * depth_idx}{fname}")
            print()

    # --- Inclusive time analysis ---
    if args.inclusive:
        target_names = set(args.inclusive.split(","))

        print(f"\n{'=' * 72}")
        print("Inclusive Time (function appears anywhere in stack)")
        print(f"{'=' * 72}\n")

        inclusive = analyze_inclusive_time(
            frames, all_samples, all_weights, target_names
        )
        sorted_inc = sorted(inclusive.items(), key=lambda x: -x[1][0])

        rows = []
        for name, (weight, count) in sorted_inc:
            pct = 100 * weight / total_weight if total_weight else 0
            rows.append([f"{count:,}", f"{pct:.1f}%", f"{weight:.2f}s", name])

        # Show any requested names that weren't found
        missing = target_names - set(inclusive.keys())
        if missing:
            for name in sorted(missing):
                rows.append(["0", "0.0%", "0.00s", f"{name} (not found)"])

        print(
            format_table(
                ["Samples", "Inclusive %", "Inclusive time", "Function"],
                rows,
                ["r", "r", "r", "l"],
            )
        )


if __name__ == "__main__":
    main()
