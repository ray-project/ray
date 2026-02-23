#!/usr/bin/env python3
"""
eugo_audit_deps.py — Audit Bazel external deps vs Meson deps for each target.

For each matched Bazel↔Meson target pair, this script:
1. Extracts all external (@...) deps from the Bazel target
2. Maps each to the expected Meson variable via the canonical mapping table
3. Checks if that Meson variable appears in the target's _dependencies block
4. Reports mismatches: missing deps, unmapped Bazel labels

This is a companion to eugo_audit_targets.py (which audits target parity).

Usage:
    python3 eugo_audit_deps.py [--verbose] [--dir DIR]

Exit codes:
    0 — All deps matched
    1 — Mismatches found
"""

import argparse
import os
import re
import sys
from collections import defaultdict

# ============================================================
# Canonical Bazel → Meson dependency mapping (from AGENTS.md §10)
# ============================================================

BAZEL_TO_MESON = {
    # --- Abseil ---
    "@com_google_absl//absl/base:core_headers": "absl_base_core_headers",
    "@com_google_absl//absl/time": "absl_time",
    "@com_google_absl//absl/types:optional": "absl_types_optional",
    "@com_google_absl//absl/container:flat_hash_map": "absl_container_flat_hash_map",
    "@com_google_absl//absl/container:flat_hash_set": "absl_container_flat_hash_set",
    "@com_google_absl//absl/container:node_hash_map": "absl_container_node_hash_map",
    "@com_google_absl//absl/container:btree": "absl_container_btree",
    "@com_google_absl//absl/container:inlined_vector": "absl_container_inlined_vector",
    "@com_google_absl//absl/random": "absl_random_random",
    "@com_google_absl//absl/random:bit_gen_ref": "absl_random_bit_gen_ref",
    "@com_google_absl//absl/synchronization": "absl_synchronization",
    "@com_google_absl//absl/cleanup": "absl_cleanup",
    "@com_google_absl//absl/debugging:failure_signal_handler": "absl_debugging_failure_signal_handler",
    "@com_google_absl//absl/debugging:stacktrace": "absl_debugging_stacktrace",
    "@com_google_absl//absl/debugging:symbolize": "absl_debugging_symbolize",
    "@com_google_absl//absl/algorithm": "absl_algorithm",
    "@com_google_absl//absl/strings": "absl_strings",
    "@com_google_absl//absl/strings:str_format": "absl_strings",  # str_format is part of strings
    "@com_google_absl//absl/memory": "absl_memory",
    "@com_google_absl//absl/hash": "absl_hash",
    # --- Boost ---
    "@boost//:asio": "boost_asio",
    "@boost//:system": "boost_system",
    "@boost//:thread": "boost_thread",
    "@boost//:fiber": "boost_fiber",
    "@boost//:beast": "boost_beast",
    "@boost//:any": "boost_any",
    "@boost//:bimap": "boost_bimap",
    "@boost//:circular_buffer": "boost_circular_buffer",
    "@boost//:algorithm": "boost_algorithm",
    "@boost//:bind": "boost_bind",
    "@boost//:endian": "boost",  # header-only, no dedicated alias yet
    "@boost//:functional": "boost_functional",
    "@boost//:iostreams": "boost_iostreams",
    "@boost//:optional": "boost_optional",
    "@boost//:process": "boost_process",
    "@boost//:range": "boost_range",
    "@boost//:asio_ssl": "boost_asio_ssl",
    # --- Other libraries ---
    "@com_github_spdlog//:spdlog": "spdlog",
    "@msgpack": "msgpack_cxx",
    "@com_github_google_flatbuffers//:flatbuffers": "flatbuffers",
    "@com_google_protobuf//:protobuf": "protobuf",
    "@com_github_grpc_grpc//:grpc++": "grpc",
    "@com_github_grpc_grpc//:grpc": "grpc",
    "@com_github_grpc_grpc//:grpc_opencensus_plugin": "grpc",
    "@com_github_grpc_grpc//:grpc++_reflection": "grpc",  # included in grpc meta-dep
    "@com_github_grpc_grpc//:grpcpp_admin": "grpc",  # included in grpc meta-dep
    "@com_github_grpc_grpc//src/proto/grpc/health/v1:health_proto": "grpc",  # auto-included with grpc
    "@nlohmann_json": "nlohmann_json",
    "@com_github_gflags_gflags//:gflags": "gflags",
    "@com_github_redis_hiredis//:hiredis": "hiredis",
    "@com_github_redis_hiredis//:hiredis_ssl": "hiredis_ssl",
    "@com_github_jupp0r_prometheus_cpp//pull": "prometheus_cpp_pull",
    "@io_opencensus_cpp//opencensus/stats": "opencensus_cpp_stats",
    "@io_opencensus_cpp//opencensus/tags": "opencensus_cpp_tags",
    "@io_opencensus_cpp//opencensus/exporters/stats/prometheus": "opencensus_cpp_prometheus_exporter",
    "@io_opencensus_cpp//opencensus/exporters/stats/stdout": "opencensus_cpp_stdout_exporter",
    # --- OpenTelemetry C++ ---
    "@io_opentelemetry_cpp//api": "opentelemetry_cpp_api",
    "@io_opentelemetry_cpp//exporters/otlp:otlp_grpc_metric_exporter": "opentelemetry_cpp_otlp_grpc_metric_exporter",
    "@io_opentelemetry_cpp//sdk/src/metrics": "opentelemetry_cpp_metrics",
    # --- absl extras ---
    "@com_google_absl//absl/base": "absl_base_core_headers",  # absl/base → core_headers
}

# Deps that are intentionally handled differently (not expected in Meson _dependencies)
INTENTIONALLY_SKIPPED = {
    "@com_google_googletest//:gtest": "handled by vendored stub (eugo/include/gtest/)",
    "@com_google_googletest//:gtest_main": "handled by vendored stub",
    "@com_google_googletest//:gtest_prod": "handled by vendored FRIEND_TEST stub",
    "@platforms//os:linux": "handled by host_machine.system() conditional",
    "@platforms//os:windows": "handled by host_machine.system() conditional",
    "@platforms//os:osx": "handled by host_machine.system() conditional",
    "@platforms//os:macos": "handled by host_machine.system() conditional",
}

# Meson variables that are equivalent (since all boost aliases resolve to same dep)
EQUIVALENT_MESON_VARS = {
    "boost": {
        "boost",
        "boost_algorithm",
        "boost_any",
        "boost_asio",
        "boost_asio_ssl",
        "boost_beast",
        "boost_bimap",
        "boost_bind",
        "boost_circular_buffer",
        "boost_fiber",
        "boost_functional",
        "boost_iostreams",
        "boost_optional",
        "boost_process",
        "boost_range",
        "boost_system",
        "boost_thread",
    },
    "boost_algorithm": {"boost", "boost_algorithm"},
    "boost_any": {"boost", "boost_any"},
    "boost_asio": {"boost", "boost_asio"},
    "boost_asio_ssl": {"boost", "boost_asio_ssl"},
    "boost_beast": {"boost", "boost_beast"},
    "boost_bimap": {"boost", "boost_bimap"},
    "boost_bind": {"boost", "boost_bind"},
    "boost_circular_buffer": {"boost", "boost_circular_buffer"},
    "boost_fiber": {"boost", "boost_fiber"},
    "boost_functional": {"boost", "boost_functional"},
    "boost_iostreams": {"boost", "boost_iostreams"},
    "boost_optional": {"boost", "boost_optional"},
    "boost_process": {"boost", "boost_process"},
    "boost_range": {"boost", "boost_range"},
    "boost_system": {"boost", "boost_system"},
    "boost_thread": {"boost", "boost_thread"},
}

# Directories to exclude
EXCLUDED_DIRS = {
    "src/ray/thirdparty/setproctitle",
    "src/ray/common/cgroup2/integration_tests",
    "src/ray/core_worker/lib/java",
    "src/ray/common/tests",
    "src/ray/raylet/tests",
}

# Target name patterns to exclude (test targets)
EXCLUDED_TARGET_PATTERNS = [
    r".*_test$",
    r".*_test_lib$",
    r".*test_utils.*",
    r".*test_fixture.*",
    r".*test_common.*",
    r".*_benchmark$",
    r"^is_linux$",
    r"^is_windows$",
    r"^fake_.*",  # Test doubles (fake_periodical_runner, fake_plasma_client, fake_raylet_client)
]

# Targets with known naming mismatches between Bazel and Meson.
# These produce "Cannot find Meson deps block" warnings but are correctly
# present in Meson under different names. Verified manually.
KNOWN_MESON_NAME_MISMATCHES = {
    # Bazel: node_manager_generated → Meson: node_manager_cpp_fbs_lib_dep (fbs pattern)
    "node_manager_generated",
    # Bazel: actor_manager → Meson: am_actor_manager_cpp_lib (am_ prefix, parent dir)
    "actor_manager",
}


def parse_bazel_targets_with_deps(bazel_path: str) -> list[dict]:
    """Parse Bazel targets and extract their deps lists with full fidelity."""
    with open(bazel_path) as f:
        content = f.read()

    targets = []

    for target_type in ["ray_cc_library", "ray_cc_binary", "flatbuffer_cc_library"]:
        pattern = re.compile(rf"{target_type}\s*\(", re.MULTILINE)
        for match in pattern.finditer(content):
            start = match.end()
            depth = 1
            pos = start
            while pos < len(content) and depth > 0:
                if content[pos] == "(":
                    depth += 1
                elif content[pos] == ")":
                    depth -= 1
                pos += 1

            block = content[start : pos - 1]

            name_match = re.search(r'name\s*=\s*"([^"]+)"', block)
            if not name_match:
                continue
            name = name_match.group(1)

            # Check exclusions
            is_excluded = False
            for pat in EXCLUDED_TARGET_PATTERNS:
                if re.match(pat, name):
                    is_excluded = True
                    break
            if is_excluded:
                continue

            # Extract deps - find the deps = [...] block
            deps_match = re.search(r"deps\s*=\s*\[", block)
            deps = []
            if deps_match:
                deps_start = deps_match.end()
                deps_depth = 1
                deps_pos = deps_start
                while deps_pos < len(block) and deps_depth > 0:
                    if block[deps_pos] == "[":
                        deps_depth += 1
                    elif block[deps_pos] == "]":
                        deps_depth -= 1
                    deps_pos += 1
                deps_block = block[deps_start : deps_pos - 1]
                deps = re.findall(r'"([^"]+)"', deps_block)

            # Also check select() blocks for platform-conditional deps
            select_deps = re.findall(r'"(@[^"]+)"', block)
            # Add any @-prefixed deps found in select blocks not already in deps
            for sd in select_deps:
                if sd not in deps and sd.startswith("@"):
                    deps.append(sd)

            # Separate internal vs external deps
            external_deps = [d for d in deps if d.startswith("@")]
            internal_deps = [d for d in deps if d.startswith(":") or d.startswith("//")]

            srcs = re.findall(r'"([^"]+\.cc)"', block)
            hdrs = re.findall(r'"([^"]+\.h)"', block)

            targets.append(
                {
                    "name": name,
                    "type": target_type,
                    "srcs": srcs,
                    "hdrs": hdrs,
                    "external_deps": external_deps,
                    "internal_deps": internal_deps,
                }
            )

    return targets


def find_meson_deps_block(meson_content: str, bazel_name: str) -> str | None:
    """Find the meson _dependencies block for a Bazel target name.

    Searches for:
    1. @original: bazel_name annotation → extract deps from that @begin/@end block
    2. bazel_name_cpp_lib_dependencies = [...]
    3. Various prefixed forms (cw_, am_, etc.)
    """

    def find_block_after_match(match_obj):
        """Find the @begin/@end block content after a match.
        Starts from the next line after the match (skips remainder of @begin line)."""
        # Skip to end of the @begin line to avoid capturing trailing " ===" etc.
        next_nl = meson_content.find("\n", match_obj.end())
        if next_nl == -1:
            return None
        block_start = next_nl + 1
        end_pattern = re.compile(r"#\s*=+\s*@end:", re.MULTILINE)
        end_match = end_pattern.search(meson_content, block_start)
        if end_match:
            return meson_content[block_start : end_match.start()]
        return None

    def block_has_code(block: str) -> bool:
        """Check if a block contains actual code (not just comments/whitespace).
        Returns False for comment-only placeholder blocks like:
            # NOTE: defined elsewhere
        """
        for line in block.strip().splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                return True
        return False

    def find_deps_array(var_name):
        """Find a _dependencies = [...] block by variable name."""
        deps_pattern = re.compile(rf"{re.escape(var_name)}\s*=\s*\[", re.MULTILINE)
        match = deps_pattern.search(meson_content)
        if match:
            start = match.end()
            depth = 1
            pos = start
            while pos < len(meson_content) and depth > 0:
                if meson_content[pos] == "[":
                    depth += 1
                elif meson_content[pos] == "]":
                    depth -= 1
                pos += 1
            return meson_content[match.start() : pos]
        return None

    # Strategy 1: find @begin block with @original: bazel_name
    # Also handles inline targets with path prefixes like "gcs/gcs_table_storage_cpp_lib"
    # and "(original: metric_interface from observability/)" with trailing context
    # Uses finditer to skip comment-only placeholder blocks (e.g., "NOTE: defined elsewhere")
    # NOTE: We search for exact bazel_name only — NOT bazel_name + "_lib",
    # because that would cross-match different Bazel targets
    # (e.g., "gcs_server" matching "@original: gcs_server_lib")
    original_pattern = re.compile(
        rf"#\s*=+\s*@begin:\s*\S+\s*\(?@?original:\s*{re.escape(bazel_name)}\b[^)]*\)?", re.MULTILINE
    )
    for match in original_pattern.finditer(meson_content):
        block = find_block_after_match(match)
        if block and block_has_code(block):
            return block

    # Strategy 2: find @begin block where the meson name derives from bazel name
    # Include path-prefixed variants for inline targets (e.g., gcs/gcs_table_storage_cpp_lib)
    known_prefixes = ["", "cw_", "am_", "ts_", "task_exec_", "gcs_"]
    path_prefixes = [""]  # add dir-based prefixes
    for dir_part in ["gcs/", "raylet/", "observability/", "common/", "util/", "stats/"]:
        path_prefixes.append(dir_part)

    def begin_line_matches_bazel_name(match_obj):
        """If the @begin line has an @original: annotation, verify it matches bazel_name.
        This prevents cross-matching, e.g., bazel 'gcs_server' matching
        '@begin: gcs/gcs_server_cpp_lib (@original: gcs_server_lib)'.
        If no @original, assume it matches."""
        # Get the full @begin line
        line_end = meson_content.find("\n", match_obj.start())
        if line_end < 0:
            line_end = len(meson_content)
        line = meson_content[match_obj.start() : line_end]
        # Check for @original annotation
        orig_match = re.search(r"@original:\s*(\S+)", line)
        if orig_match:
            original_name = orig_match.group(1).rstrip(")")
            return original_name == bazel_name
        return True  # No @original annotation → assume match

    for path_prefix in path_prefixes:
        for name_prefix in known_prefixes:
            meson_name = path_prefix + name_prefix + bazel_name + "_cpp_lib"
            begin_pattern = re.compile(rf"#\s*=+\s*@begin:\s*{re.escape(meson_name)}\b", re.MULTILINE)
            match = begin_pattern.search(meson_content)
            if match and begin_line_matches_bazel_name(match):
                block = find_block_after_match(match)
                if block and block_has_code(block):
                    return block

    # Strategy 3: find _dependencies = [...] variable directly
    for prefix in known_prefixes:
        var_name = prefix + bazel_name + "_cpp_lib_dependencies"
        result = find_deps_array(var_name)
        if result:
            # Verify this deps array isn't inside a block for a different Bazel target
            # (e.g., gcs_server_cpp_lib_dependencies belongs to gcs_server_lib, not gcs_server)
            deps_pos = meson_content.find(var_name)
            if deps_pos >= 0:
                # Look backwards for the nearest @begin with @original
                preceding = meson_content[:deps_pos]
                begin_matches = list(re.finditer(r"#\s*=+\s*@begin:.*?@original:\s*(\S+)", preceding, re.MULTILINE))
                if begin_matches:
                    last_orig = begin_matches[-1].group(1).rstrip(")")
                    if last_orig != bazel_name:
                        continue  # This deps array belongs to a different target
            return result

    # Strategy 4: for executables (ray_cc_binary), look for @begin blocks
    # that reference the bazel name directly (without _cpp_lib suffix)
    for path_prefix in path_prefixes:
        for variant in [bazel_name, bazel_name + "_bin"]:
            exe_begin_pattern = re.compile(rf"#\s*=+\s*@begin:\s*{re.escape(path_prefix + variant)}\b", re.MULTILINE)
            match = exe_begin_pattern.search(meson_content)
            if match:
                block = find_block_after_match(match)
                if block and block_has_code(block):
                    return block

    # Strategy 4b: find executable() call and extract its deps block
    exe_pattern = re.compile(
        rf"executable\(\s*['\"]({re.escape(bazel_name)}|{re.escape(bazel_name)}_bin)['\"]", re.MULTILINE
    )
    match = exe_pattern.search(meson_content)
    if match:
        # Find the surrounding block (up to next blank line or next @begin)
        block_start = max(0, meson_content.rfind("\n\n", 0, match.start()) + 1)
        block_end_match = re.search(r"\n\n|#\s*=+\s*@end:", meson_content[match.end() :])
        if block_end_match:
            return meson_content[block_start : match.end() + block_end_match.end()]
        return meson_content[block_start : match.end() + 500]

    # Strategy 5: look for dependencies: [deps list] near a target named after bazel_name
    deps_list_pattern = re.compile(rf"{re.escape(bazel_name)}_dependencies\s*=\s*\[", re.MULTILINE)
    match = deps_list_pattern.search(meson_content)
    if match:
        start = match.end()
        depth = 1
        pos = start
        while pos < len(meson_content) and depth > 0:
            if meson_content[pos] == "[":
                depth += 1
            elif meson_content[pos] == "]":
                depth -= 1
            pos += 1
        return meson_content[match.start() : pos]

    return None


def extract_meson_dep_vars(meson_block: str) -> set[str]:
    """Extract all Meson dependency variable names from a deps block.

    Looks for variable names that match known dependency patterns
    (absl_*, boost_*, spdlog, protobuf, grpc, etc.)
    """
    # Get all identifiers that could be dependency variables
    # These appear as bare names in the _dependencies = [...] list
    # or in dependencies: [...] of static_library/declare_dependency
    vars_found = set()

    # All known Meson dep variables from eugo/dependencies/meson.build
    known_vars = {
        "absl_base_core_headers",
        "absl_time",
        "absl_types_optional",
        "absl_container_flat_hash_map",
        "absl_container_flat_hash_set",
        "absl_container_node_hash_map",
        "absl_container_btree",
        "absl_random_random",
        "absl_random_bit_gen_ref",
        "absl_synchronization",
        "absl_cleanup",
        "absl_debugging_failure_signal_handler",
        "absl_debugging_stacktrace",
        "absl_debugging_symbolize",
        "absl_algorithm",
        "absl_strings",
        "absl_memory",
        "absl_hash",
        "boost",
        "boost_algorithm",
        "boost_any",
        "boost_asio",
        "boost_asio_ssl",
        "boost_beast",
        "boost_bimap",
        "boost_bind",
        "boost_circular_buffer",
        "boost_fiber",
        "boost_functional",
        "boost_iostreams",
        "boost_optional",
        "boost_process",
        "boost_range",
        "boost_system",
        "boost_thread",
        "spdlog",
        "msgpack_cxx",
        "flatbuffers",
        "protobuf",
        "grpc",
        "nlohmann_json",
        "gflags",
        "hiredis",
        "hiredis_ssl",
        "threads",
        "opencensus_cpp_prometheus_exporter",
        "opencensus_cpp_stdout_exporter",
        "opencensus_cpp_stats",
        "opencensus_cpp_tags",
        "prometheus_cpp_pull",
    }

    for var in known_vars:
        # Look for the variable name as a word boundary match
        if re.search(rf"\b{re.escape(var)}\b", meson_block):
            vars_found.add(var)

    return vars_found


def get_all_meson_content_for_dir(bazel_dir: str) -> str:
    """Get concatenated content of all meson.build files that could contain
    targets for this Bazel directory."""
    contents = []
    seen = set()

    def add_file(path):
        path = os.path.normpath(path)
        if path not in seen and os.path.exists(path):
            seen.add(path)
            with open(path) as f:
                contents.append(f.read())

    add_file(os.path.join(bazel_dir, "meson.build"))
    for root, dirs, files in os.walk(bazel_dir):
        if "meson.build" in files:
            add_file(os.path.join(root, "meson.build"))
    parent = os.path.dirname(bazel_dir)
    if parent and parent != bazel_dir:
        add_file(os.path.join(parent, "meson.build"))
    add_file("src/ray/meson.build")

    return "\n".join(contents)


def check_meson_has_dep(meson_block: str, expected_var: str) -> bool:
    """Check if a Meson deps block contains the expected variable,
    accounting for equivalent variables (e.g., boost aliases)."""
    # Direct check
    if re.search(rf"\b{re.escape(expected_var)}\b", meson_block):
        return True

    # Check equivalents (e.g., boost_asio satisfies a boost dep)
    if expected_var in EQUIVALENT_MESON_VARS:
        for equiv in EQUIVALENT_MESON_VARS[expected_var]:
            if re.search(rf"\b{re.escape(equiv)}\b", meson_block):
                return True

    return False


def audit_deps_for_directory(bazel_dir: str, verbose: bool = False) -> list[dict]:
    """Audit all external deps for all targets in a directory."""
    issues = []
    bazel_path = os.path.join(bazel_dir, "BUILD.bazel")

    if not os.path.exists(bazel_path):
        return []

    normalized = bazel_dir.replace("\\", "/")
    if normalized in EXCLUDED_DIRS:
        return []

    bazel_targets = parse_bazel_targets_with_deps(bazel_path)
    if not bazel_targets:
        return []

    meson_content = get_all_meson_content_for_dir(bazel_dir)
    if not meson_content:
        return []  # Already reported by target audit

    for target in bazel_targets:
        if not target["external_deps"]:
            continue

        meson_block = find_meson_deps_block(meson_content, target["name"])

        for bazel_dep in target["external_deps"]:
            # Check if intentionally skipped
            if bazel_dep in INTENTIONALLY_SKIPPED:
                if verbose:
                    print(f"    SKIP: {bazel_dep} — {INTENTIONALLY_SKIPPED[bazel_dep]}")
                continue

            # Look up in mapping table
            if bazel_dep not in BAZEL_TO_MESON:
                issues.append(
                    {
                        "level": "WARN",
                        "dir": bazel_dir,
                        "target": target["name"],
                        "bazel_dep": bazel_dep,
                        "message": f"Unmapped Bazel dep: {bazel_dep}",
                        "detail": "No entry in BAZEL_TO_MESON mapping table. "
                        "May need to add to eugo/dependencies/meson.build and mapping.",
                    }
                )
                continue

            expected_meson_var = BAZEL_TO_MESON[bazel_dep]

            if meson_block is None:
                if target["name"] in KNOWN_MESON_NAME_MISMATCHES:
                    continue  # Known naming mismatch — verified manually
                issues.append(
                    {
                        "level": "WARN",
                        "dir": bazel_dir,
                        "target": target["name"],
                        "bazel_dep": bazel_dep,
                        "message": f"Cannot find Meson deps block for target '{target['name']}'",
                        "detail": f"Expected Meson var: {expected_meson_var}. "
                        "Could not locate _dependencies list or @begin/@end block.",
                    }
                )
                continue

            if not check_meson_has_dep(meson_block, expected_meson_var):
                # Double-check: maybe the Meson block has it under a different
                # but equivalent boost alias
                issues.append(
                    {
                        "level": "ERROR",
                        "dir": bazel_dir,
                        "target": target["name"],
                        "bazel_dep": bazel_dep,
                        "message": f"Missing Meson dep: {expected_meson_var}",
                        "detail": f"Bazel dep {bazel_dep} maps to {expected_meson_var}, "
                        f"but it was not found in the Meson deps block for target '{target['name']}'.",
                    }
                )

    return issues


def scan_for_unmapped_bazel_labels(verbose: bool = False) -> list[dict]:
    """Scan ALL BUILD.bazel files for @-prefixed dep labels not in the mapping table."""
    issues = []
    seen_labels = set()

    for root, dirs, files in os.walk("src/ray"):
        if "BUILD.bazel" not in files:
            continue

        normalized = root.replace("\\", "/")
        if normalized in EXCLUDED_DIRS:
            continue

        with open(os.path.join(root, "BUILD.bazel")) as f:
            content = f.read()

        # Find all @-prefixed dep labels
        labels = re.findall(r'"(@[^"]+)"', content)
        for label in labels:
            if label in BAZEL_TO_MESON:
                continue
            if label in INTENTIONALLY_SKIPPED:
                continue
            if label in seen_labels:
                continue
            seen_labels.add(label)

            # Skip labels that look like they're in select() platform conditions
            # but not actual library deps
            if "//conditions:" in label:
                continue

            # Skip Bazel build rule imports (.bzl files) — not library deps
            if label.endswith(".bzl"):
                continue

            # Skip proto-related build labels (handled by codegen, not library deps)
            if "rules_proto" in label or "rules_cc" in label:
                continue

            # Skip proto source deps (e.g., @io_opencensus_proto//..., @com_google_protobuf//:*_proto)
            # These are handled by our protobuf codegen, not as library deps
            if "_proto" in label and "cc_proto" not in label and "grpc" not in label:
                continue

            issues.append(
                {
                    "level": "INFO",
                    "dir": root,
                    "target": "",
                    "bazel_dep": label,
                    "message": f"Unmapped @-label: {label}",
                    "detail": f"Found in {root}/BUILD.bazel. May be a dep that needs a Meson equivalent.",
                }
            )

    return issues


def main():
    parser = argparse.ArgumentParser(description="Audit Bazel external deps vs Meson deps for each target")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument("--dir", type=str, default=None, help="Audit only this directory")
    parser.add_argument("--errors-only", action="store_true", help="Show only ERROR level issues")
    args = parser.parse_args()

    # Phase 1: Per-target dep audit
    if args.dir:
        bazel_dirs = [args.dir]
    else:
        bazel_dirs = []
        for root, dirs, files in os.walk("src/ray"):
            if "BUILD.bazel" in files:
                bazel_dirs.append(root)

    print(f"Auditing external deps in {len(bazel_dirs)} directories...")
    print()

    all_issues = []
    targets_audited = 0
    deps_checked = 0

    for bazel_dir in sorted(bazel_dirs):
        if args.verbose:
            print(f"Checking {bazel_dir}/...")

        dir_issues = audit_deps_for_directory(bazel_dir, verbose=args.verbose)
        all_issues.extend(dir_issues)

        # Count targets and deps for this dir
        bazel_path = os.path.join(bazel_dir, "BUILD.bazel")
        if os.path.exists(bazel_path):
            targets = parse_bazel_targets_with_deps(bazel_path)
            targets_audited += len(targets)
            for t in targets:
                deps_checked += len(t["external_deps"])

    # Phase 2: Global scan for unmapped labels
    if not args.dir:
        print("Scanning for unmapped @-labels globally...")
        unmapped_issues = scan_for_unmapped_bazel_labels(verbose=args.verbose)
        all_issues.extend(unmapped_issues)

    # Filter
    if args.errors_only:
        all_issues = [i for i in all_issues if i["level"] == "ERROR"]

    # Report
    print()
    print(f"{'=' * 70}")
    print("Dependency Audit Summary")
    print(f"{'=' * 70}")
    print(f"  Targets audited : {targets_audited}")
    print(f"  Deps checked    : {deps_checked}")

    errors = [i for i in all_issues if i["level"] == "ERROR"]
    warns = [i for i in all_issues if i["level"] == "WARN"]
    infos = [i for i in all_issues if i["level"] == "INFO"]

    print(f"  ❌ ERRORS        : {len(errors)} (missing Meson deps)")
    print(f"  ⚠️  WARNINGS      : {len(warns)} (unmapped/unfound)")
    print(f"  ℹ️  INFO          : {len(infos)} (unmapped global labels)")
    print(f"{'=' * 70}")

    if not all_issues:
        print("\n✅ All Bazel external deps have correct Meson equivalents. Great job!")
        sys.exit(0)

    # Group by level
    if errors:
        print(f"\n{'=' * 70}")
        print("❌ ERRORS — Missing Meson dependencies:")
        print(f"{'=' * 70}")
        by_dir = defaultdict(list)
        for issue in errors:
            by_dir[issue["dir"]].append(issue)
        for dir_key in sorted(by_dir.keys()):
            print(f"\n  {dir_key}/:")
            for issue in by_dir[dir_key]:
                print(f"    [{issue['target']}] {issue['message']}")
                print(f"      → {issue['detail']}")

    if warns and not args.errors_only:
        print(f"\n{'=' * 70}")
        print("⚠️  WARNINGS — Could not verify:")
        print(f"{'=' * 70}")
        by_dir = defaultdict(list)
        for issue in warns:
            by_dir[issue["dir"]].append(issue)
        for dir_key in sorted(by_dir.keys()):
            print(f"\n  {dir_key}/:")
            for issue in by_dir[dir_key]:
                print(f"    [{issue['target']}] {issue['message']}")
                if args.verbose:
                    print(f"      → {issue['detail']}")

    if infos and not args.errors_only:
        print(f"\n{'=' * 70}")
        print("ℹ️  INFO — Unmapped @-labels (may not be actual deps):")
        print(f"{'=' * 70}")
        for issue in infos:
            print(f"    {issue['bazel_dep']}")
            print(f"      Found in: {issue['dir']}/BUILD.bazel")

    if errors:
        print(f"\n❌ {len(errors)} ERROR(s) found — these are likely real bugs.")
        sys.exit(1)
    elif warns:
        print(f"\n⚠️  {len(warns)} WARNING(s) found — manual review recommended.")
        sys.exit(0)
    else:
        sys.exit(0)


if __name__ == "__main__":
    main()
