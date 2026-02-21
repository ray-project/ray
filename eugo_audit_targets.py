#!/usr/bin/env python3
"""
eugo_audit_targets.py — Audit BUILD.bazel vs meson.build target parity.

For each directory under src/ray/ that contains a BUILD.bazel, this script:
1. Parses production (non-test) ray_cc_library/ray_cc_binary targets from BUILD.bazel
2. Checks if a corresponding meson.build exists
3. Parses @begin/@original annotations from meson.build
4. Also checks src/ray/meson.build for inline targets (used for circular dep breaking)
5. Reports mismatches: missing meson.build files, missing targets, extra targets

Usage:
    python3 eugo_audit_targets.py [--verbose] [--category CATEGORY]

Exit codes:
    0 — All targets matched
    1 — Mismatches found
"""

import argparse
import os
import re
import sys
from collections import defaultdict

# Directories that are intentionally excluded from Meson builds
EXCLUDED_DIRS = {
    "src/ray/thirdparty/setproctitle",  # replaced by pip package
    "src/ray/common/cgroup2/integration_tests",  # test-only
    "src/ray/core_worker/lib/java",  # Java JNI, not built in Meson
    "src/ray/common/tests",  # test utilities only
    "src/ray/raylet/tests",  # test utilities only
}

# Bazel target name patterns to exclude (test targets, config settings, etc.)
EXCLUDED_TARGET_PATTERNS = [
    r".*_test$",
    r".*_test_lib$",
    r".*test_utils.*",
    r".*test_fixture.*",
    r".*test_common.*",
    r".*_benchmark$",
    r"^is_linux$",  # config_setting
    r"^is_windows$",  # config_setting
]

# Fake/test-helper targets that exist in production BUILD.bazel but are only
# used by tests. These are header-only stubs/mocks. We track them in the audit
# but categorize them as "fake" so they can be filtered.
FAKE_TARGET_PATTERNS = [
    r"^fake_.*",
    r"^testing$",  # common/tests/testing
]

# Known Bazel target names that map to equivalent Meson targets under different names
# Bazel name → list of Meson variable patterns that satisfy it
KNOWN_EQUIVALENTS = {
    # FlatBuffers "generated" header-only targets are satisfied by the FBS codegen target
    "node_manager_generated": ["node_manager_cpp_fbs_lib_dep"],
    "plasma_generated": ["plasma_cpp_fbs_lib_dep"],
}

# Directories where meson.build is in a different location than BUILD.bazel
INLINE_TARGET_FILE = "src/ray/meson.build"


def parse_bazel_targets(bazel_path: str) -> list[dict]:
    """Parse ray_cc_library, ray_cc_binary, cc_proto_library, cc_grpc_library,
    and flatbuffer_cc_library targets from a BUILD.bazel file.
    """
    with open(bazel_path) as f:
        content = f.read()

    targets = []

    for target_type in [
        "ray_cc_library",
        "ray_cc_binary",
        "flatbuffer_cc_library",
        "cc_proto_library",
        "cc_grpc_library",
    ]:
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

            # Check exclusion patterns
            is_excluded = False
            for pat in EXCLUDED_TARGET_PATTERNS:
                if re.match(pat, name):
                    is_excluded = True
                    break
            if is_excluded:
                continue

            # Check if it's a fake/test-helper target
            is_fake = False
            for pat in FAKE_TARGET_PATTERNS:
                if re.match(pat, name):
                    is_fake = True
                    break

            srcs = re.findall(r'"([^"]+\.cc)"', block)
            hdrs = re.findall(r'"([^"]+\.h)"', block)
            deps = re.findall(r'"([^"]+)"', block)
            deps = [d for d in deps if d.startswith(":") or d.startswith("//") or d.startswith("@")]

            targets.append(
                {
                    "name": name,
                    "type": target_type,
                    "srcs": srcs,
                    "hdrs": hdrs,
                    "deps": deps,
                    "is_fake": is_fake,
                }
            )

    return targets


def parse_meson_targets(meson_path: str, dir_prefix: str = "") -> dict[str, dict]:
    """Parse @begin blocks from a meson.build file.

    Supports two formats:
      1. # === @begin: meson_name (@original: bazel_name) ===
      2. # === @begin: meson_name ===  (no @original — uses meson_name itself)

    Also detects prefixed names: if meson target is cw_common, it can match Bazel "common"
    by stripping known prefixes.
    """
    if not os.path.exists(meson_path):
        return {}

    with open(meson_path) as f:
        content = f.read()

    targets = {}

    # Pattern 1: with @original
    begin_with_original = re.compile(r"#\s*=*\s*@begin:\s*(\S+)\s*\(@original:\s*([^)]+)\)", re.MULTILINE)
    # Pattern 2: without @original
    begin_without_original = re.compile(r"#\s*=*\s*@begin:\s*(\S+)\s*=", re.MULTILINE)
    # Pattern 3: just @begin: name ===
    begin_simple = re.compile(r"#\s*=*\s*@begin:\s*([^\s(=]+)\s*={0,3}\s*$", re.MULTILINE)

    def process_block(meson_name, original_name, match_end):
        if dir_prefix and not meson_name.startswith(dir_prefix):
            return

        block_start = match_end
        end_match = re.search(r"#\s*=*\s*@end:", content[block_start:])
        if end_match:
            block = content[block_start : block_start + end_match.start()]
        else:
            block = content[block_start : block_start + 800]

        has_static_lib = "static_library(" in block
        has_declare_dep = "declare_dependency(" in block
        has_custom_target = "custom_target(" in block
        srcs = re.findall(r"'([^']+\.cc)'", block)

        targets[original_name] = {
            "meson_name": meson_name,
            "has_static_lib": has_static_lib,
            "has_declare_dep": has_declare_dep,
            "has_custom_target": has_custom_target,
            "srcs": srcs,
        }

    for match in begin_with_original.finditer(content):
        meson_name = match.group(1)
        original_name = match.group(2).strip()
        process_block(meson_name, original_name, match.end())

    for match in begin_simple.finditer(content):
        meson_name = match.group(1)
        # Skip if already matched by the @original pattern
        if any(t["meson_name"] == meson_name for t in targets.values()):
            continue
        # Derive Bazel name from Meson name by stripping common suffixes
        original_name = meson_name
        for suffix in ["_cpp_lib", "_cpp_fbs_lib", "_proto_cpp_lib", "_proto_cpp_grpc_lib", "_proto_py"]:
            if meson_name.endswith(suffix):
                original_name = meson_name[: -len(suffix)]
                break
        process_block(meson_name, original_name, match.end())

    return targets


def find_meson_target_by_bazel_name(
    bazel_name: str, bazel_dir: str, all_meson_targets: dict, meson_content: str
) -> bool:
    """Check if a Bazel target has a corresponding Meson target.

    Handles several naming conventions:
    - Direct match: bazel_name
    - Prefixed: cw_<bazel_name>, am_<bazel_name>, ts_<bazel_name>, etc.
    - Proto: <name>_proto_cpp, <name>_proto_cpp_grpc, <name>_proto_py
    - Variable name search: <bazel_name>_cpp_lib_dep in meson content
    - Known equivalents: KNOWN_EQUIVALENTS mapping
    """
    # Check known equivalents first
    if bazel_name in KNOWN_EQUIVALENTS:
        for equiv_var in KNOWN_EQUIVALENTS[bazel_name]:
            if re.search(rf"\b{re.escape(equiv_var)}\s*=", meson_content):
                return True

    # Direct match via @original
    if bazel_name in all_meson_targets:
        return True

    # Known prefixes for subdirectory targets
    known_prefixes = ["cw_", "am_", "ts_", "task_exec_", "gcs_"]
    for prefix in known_prefixes:
        if prefix + bazel_name in all_meson_targets:
            return True

    # Proto target naming: Bazel xxx_cc_proto → Meson xxx_proto_cpp_lib
    proto_mappings = {
        "_cc_proto": "_proto_cpp",
        "_cc_grpc": "_proto_cpp_grpc",
        "_py_proto": "_proto_py",
    }
    for bazel_suffix, meson_suffix in proto_mappings.items():
        if bazel_name.endswith(bazel_suffix):
            base = bazel_name[: -len(bazel_suffix)]
            meson_proto_name = base + meson_suffix
            # Check if exists as a variable in meson content
            if re.search(rf"{meson_proto_name}\b", meson_content):
                return True

    # Search for variable definition in meson content
    # e.g., bazel_name = "ray_syncer" → look for ray_syncer_cpp_lib_dep =
    # or ray_syncer_cpp_lib =
    for suffix in ["_cpp_lib_dep", "_cpp_lib", "_cpp_fbs_lib_dep"]:
        var_name = bazel_name + suffix
        if re.search(rf"\b{re.escape(var_name)}\s*=", meson_content):
            return True

    # Prefixed variable search
    for prefix in known_prefixes:
        for suffix in ["_cpp_lib_dep", "_cpp_lib"]:
            var_name = prefix + bazel_name + suffix
            if re.search(rf"\b{re.escape(var_name)}\s*=", meson_content):
                return True

    return False


def get_all_meson_content(bazel_dir: str) -> str:
    """Get concatenated content of all meson.build files that could contain
    targets for this Bazel directory.

    Searches:
    1. Local meson.build
    2. All subdirectory meson.build files (e.g., common/asio/meson.build)
    3. Parent meson.build (for inline targets like actor_manager)
    4. src/ray/meson.build (for circular dep inline targets)
    """
    contents = []
    seen = set()

    def add_file(path):
        path = os.path.normpath(path)
        if path not in seen and os.path.exists(path):
            seen.add(path)
            with open(path) as f:
                contents.append(f.read())

    # Local meson.build
    add_file(os.path.join(bazel_dir, "meson.build"))

    # All subdirectory meson.build files
    for root, dirs, files in os.walk(bazel_dir):
        if "meson.build" in files:
            add_file(os.path.join(root, "meson.build"))

    # Parent meson.build (e.g., core_worker/meson.build for actor_management/)
    parent = os.path.dirname(bazel_dir)
    if parent and parent != bazel_dir:
        add_file(os.path.join(parent, "meson.build"))

    # src/ray/meson.build (inline targets for circular dep breaking)
    add_file(INLINE_TARGET_FILE)

    return "\n".join(contents)


def classify_issue(target: dict) -> str:
    """Classify an issue into a category for filtering/reporting."""
    if target["is_fake"]:
        return "fake"
    if target["type"] in ("cc_proto_library", "cc_grpc_library"):
        return "proto"
    if target["type"] == "flatbuffer_cc_library":
        return "flatbuf"
    if target["type"] == "ray_cc_binary":
        return "binary"
    if not target["srcs"]:
        return "header-only"
    return "library"


def audit_directory(bazel_dir: str, verbose: bool = False) -> list[dict]:
    """Audit a single directory's BUILD.bazel against its meson.build.

    Returns a list of issue dicts with keys: message, category, target
    """
    issues = []
    bazel_path = os.path.join(bazel_dir, "BUILD.bazel")

    if not os.path.exists(bazel_path):
        return []

    normalized = bazel_dir.replace("\\", "/")
    if normalized in EXCLUDED_DIRS:
        if verbose:
            print(f"  SKIP (excluded): {bazel_dir}")
        return []

    bazel_targets = parse_bazel_targets(bazel_path)
    if not bazel_targets:
        if verbose:
            print(f"  SKIP (no production targets): {bazel_dir}")
        return []

    meson_path = os.path.join(bazel_dir, "meson.build")
    has_local_meson = os.path.exists(meson_path)
    meson_content = get_all_meson_content(bazel_dir)

    if not has_local_meson:
        # Check if targets are inline in src/ray/meson.build
        all_found = True
        for t in bazel_targets:
            if not find_meson_target_by_bazel_name(t["name"], bazel_dir, {}, meson_content):
                all_found = False
                break
        if not all_found:
            issues.append(
                {
                    "message": f"MISSING meson.build: {bazel_dir}/ has {len(bazel_targets)} production targets",
                    "category": "missing_file",
                    "targets": [t["name"] for t in bazel_targets],
                }
            )
            return issues

    # Check each Bazel target
    local_meson_targets = parse_meson_targets(meson_path) if has_local_meson else {}
    inline_meson_targets = parse_meson_targets(
        INLINE_TARGET_FILE, os.path.relpath(bazel_dir, "src/ray").rstrip("/") + "/"
    )
    all_meson_targets = {**local_meson_targets, **inline_meson_targets}

    for target in bazel_targets:
        found = find_meson_target_by_bazel_name(target["name"], bazel_dir, all_meson_targets, meson_content)
        if not found:
            category = classify_issue(target)
            srcs_str = f", srcs={target['srcs']}" if target["srcs"] else ""
            hdrs_str = f", hdrs={target['hdrs']}" if target["hdrs"] else ""
            issues.append(
                {
                    "message": f"MISSING TARGET: {bazel_dir}/{target['name']} "
                    f"(type={target['type']}{srcs_str}{hdrs_str})",
                    "category": category,
                    "target": target,
                }
            )

    return issues


def main():
    parser = argparse.ArgumentParser(description="Audit BUILD.bazel vs meson.build target parity")
    parser.add_argument("--verbose", "-v", action="store_true", help="Show detailed output")
    parser.add_argument(
        "--category",
        "-c",
        type=str,
        default=None,
        help="Filter by category: library, header-only, fake, proto, binary, missing_file",
    )
    parser.add_argument(
        "--include-fake", action="store_true", help="Include fake_*/test-helper targets (excluded by default)"
    )
    parser.add_argument("--include-proto", action="store_true", help="Include proto/grpc targets (excluded by default)")
    args = parser.parse_args()

    bazel_dirs = []
    for root, dirs, files in os.walk("src/ray"):
        if "BUILD.bazel" in files:
            bazel_dirs.append(root)

    meson_dirs = set()
    for root, dirs, files in os.walk("src/ray"):
        if "meson.build" in files:
            meson_dirs.add(root)

    print(f"Found {len(bazel_dirs)} BUILD.bazel directories under src/ray/")
    print(f"Found {len(meson_dirs)} meson.build directories under src/ray/")
    print()

    all_issues = []
    category_counts = defaultdict(int)

    for bazel_dir in sorted(bazel_dirs):
        if args.verbose:
            bazel_targets = parse_bazel_targets(os.path.join(bazel_dir, "BUILD.bazel"))
            if bazel_targets:
                print(f"Checking {bazel_dir}/ ({len(bazel_targets)} Bazel targets)...")
            else:
                print(f"Checking {bazel_dir}/ (no production targets)")

        issues = audit_directory(bazel_dir, verbose=args.verbose)

        for issue in issues:
            category = issue.get("category", "unknown")
            category_counts[category] += 1

            # Apply filters
            if args.category and category != args.category:
                continue
            if category == "fake" and not args.include_fake:
                continue
            if category == "proto" and not args.include_proto:
                continue

            all_issues.append(issue)

        if args.verbose and not issues:
            bazel_targets = parse_bazel_targets(os.path.join(bazel_dir, "BUILD.bazel"))
            if bazel_targets:
                print(f"  ✅ {len(bazel_targets)} targets OK")

    print()
    print(f"{'=' * 60}")
    print("Category Summary:")
    for cat, count in sorted(category_counts.items()):
        excluded = ""
        if cat == "fake" and not args.include_fake:
            excluded = " (excluded from report — use --include-fake)"
        elif cat == "proto" and not args.include_proto:
            excluded = " (excluded from report — use --include-proto)"
        print(f"  {cat:20s}: {count:3d}{excluded}")
    print(f"{'=' * 60}")

    if all_issues:
        print(f"\nISSUES ({len(all_issues)} after filtering):")
        print(f"{'-' * 60}")

        # Group by directory
        by_dir = defaultdict(list)
        for issue in all_issues:
            msg = issue["message"]
            dir_match = re.match(r".*?(src/ray/\S+?)/", msg)
            dir_key = dir_match.group(1) if dir_match else "other"
            by_dir[dir_key].append(issue)

        for dir_key in sorted(by_dir.keys()):
            print(f"\n  {dir_key}/:")
            for issue in by_dir[dir_key]:
                cat = issue.get("category", "?")
                print(f"    [{cat:12s}] {issue['message']}")

        print()
        sys.exit(1)
    else:
        print("\nAll BUILD.bazel targets have corresponding meson.build targets. Great job! 🎉")
        sys.exit(0)


if __name__ == "__main__":
    main()
