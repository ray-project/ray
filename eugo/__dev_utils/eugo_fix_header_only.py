#!/usr/bin/env python3
"""Fix header-only declare_dependency() blocks to match AGENTS.md Pattern 2.

Three passes:
  Pass A: Extract inlined deps from header-only declare_dependency blocks
  Pass B: Add '# Header-only' comment after @begin markers for header-only targets with deps
  Pass C: Add section comments to ALL _dependencies variables missing them
"""

import re
from pathlib import Path

# Exact Eugo-managed dependency variable names (from eugo/dependencies/meson.build)
EUGO_MANAGED_EXACT = {
    "grpc",
    "protobuf",
    "flatbuffers",
    "spdlog",
    "msgpack_cxx",
    "nlohmann_json",
    "gflags",
    "hiredis",
    "hiredis_ssl",
    "threads",
}

# Eugo-managed dependency variable name prefixes
EUGO_MANAGED_PREFIXES = ("absl_", "boost_", "prometheus_cpp_", "opencensus_cpp_")


def is_eugo_managed(dep_name):
    """Check if a dependency variable is Eugo-managed (external)."""
    if dep_name in EUGO_MANAGED_EXACT:
        return True
    return dep_name.startswith(EUGO_MANAGED_PREFIXES)


def classify_deps(deps_list):
    """Split deps into (package_managed, eugo_managed)."""
    pkg = []
    eugo = []
    for d in deps_list:
        if is_eugo_managed(d):
            eugo.append(d)
        else:
            pkg.append(d)
    return pkg, eugo


def format_deps_block(indent, var_name, deps):
    """Format a _dependencies = [...] block with section comments."""
    pkg, eugo = classify_deps(deps)
    lines = [f"{indent}{var_name} = ["]
    if pkg and eugo:
        lines.append(f"{indent}    # Package-managed")
        for d in pkg:
            lines.append(f"{indent}    {d},")
        lines.append(f"{indent}")
        lines.append(f"{indent}    # Eugo-managed")
        for d in eugo:
            lines.append(f"{indent}    {d},")
    elif pkg:
        lines.append(f"{indent}    # Package-managed")
        for d in pkg:
            lines.append(f"{indent}    {d},")
    elif eugo:
        lines.append(f"{indent}    # Eugo-managed")
        for d in eugo:
            lines.append(f"{indent}    {d},")
    lines.append(f"{indent}]")
    return lines


def pass_a(lines):
    """Pass A: Extract inlined deps from header-only declare_dependency blocks."""
    output = []
    changes = 0
    i = 0

    while i < len(lines):
        # Look for: foo_cpp_lib_dep = declare_dependency(
        m = re.match(r"^(\s*)(\w+_cpp_lib_dep)\s*=\s*declare_dependency\(\s*$", lines[i])
        if not m:
            output.append(lines[i])
            i += 1
            continue

        indent = m.group(1)
        var_name = m.group(2)

        # Collect entire declare_dependency block until closing )
        decl_lines = [lines[i]]
        j = i + 1
        while j < len(lines):
            decl_lines.append(lines[j])
            if re.match(r"^\s*\)\s*$", lines[j]):
                j += 1
                break
            j += 1

        block_text = "\n".join(decl_lines)

        # Is this a header-only target?
        # Header-only: has sources: with .h string literals, no link_with:
        is_header_only = (".h'" in block_text) and ("link_with:" not in block_text)

        if not is_header_only:
            output.extend(decl_lines)
            i = j
            continue

        # Extract sources value
        sources_val = None
        for dl in decl_lines:
            sm = re.search(r"sources:\s*(\[.*?\])", dl)
            if sm:
                sources_val = sm.group(1)
                break

        # Check if deps are already a variable reference
        is_var_ref = any(re.match(r"\s*dependencies:\s*\w+_dependencies\s*$", dl) for dl in decl_lines)

        if is_var_ref:
            output.extend(decl_lines)
            i = j
            continue

        # Extract inlined dependencies (single-line or multi-line)
        deps = []
        for dl in decl_lines:
            # Single-line: dependencies: [dep1, dep2]
            dm = re.match(r"\s*dependencies:\s*\[([^\]]*)\]\s*$", dl)
            if dm:
                raw = dm.group(1).strip()
                if raw:
                    deps = [x.strip().rstrip(",") for x in raw.split(",") if x.strip().rstrip(",")]
                break

        # Multi-line deps: dependencies: [\n    dep1,\n]
        if not deps:
            in_deps = False
            for dl in decl_lines:
                if re.match(r"\s*dependencies:\s*\[\s*$", dl):
                    in_deps = True
                    continue
                if in_deps:
                    if re.match(r"\s*\],?\s*$", dl):
                        break
                    stripped = dl.strip().rstrip(",")
                    if stripped and not stripped.startswith("#"):
                        deps.append(stripped)

        if not deps or not sources_val:
            output.extend(decl_lines)
            i = j
            continue

        # Build extracted _dependencies variable and new declare_dependency
        dep_var = re.sub(r"_dep$", "_dependencies", var_name)

        dep_block = format_deps_block(indent, dep_var, deps)
        output.extend(dep_block)
        output.append("")
        output.append(f"{indent}{var_name} = declare_dependency(")
        output.append(f"{indent}    sources: {sources_val},")
        output.append(f"{indent}    dependencies: {dep_var}")
        output.append(f"{indent})")
        changes += 1
        i = j

    return output, changes


def pass_b(lines):
    """Pass B: Add '# Header-only' comment after @begin markers for header-only targets with deps."""
    output = []
    changes = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        # Look for @begin markers
        if "=== @begin:" not in line:
            output.append(line)
            i += 1
            continue

        # Find the @end for this block
        block_end = None
        for k in range(i + 1, min(i + 50, len(lines))):
            if "=== @end:" in lines[k]:
                block_end = k
                break

        if block_end is None:
            output.append(line)
            i += 1
            continue

        block_text = "\n".join(lines[i + 1 : block_end])

        # Check if # Header-only already present
        has_header_only = False
        look = i + 1
        while look < block_end and lines[look].strip() == "":
            look += 1
        if look < block_end and lines[look].strip() == "# Header-only":
            has_header_only = True

        # Is this a header-only block with deps?
        has_dep_var = bool(re.search(r"\w+_cpp_lib_dependencies\s*=\s*\[", block_text))
        has_header_source = ".h'" in block_text
        has_static = "static_library" in block_text
        is_ho_with_deps = has_dep_var and has_header_source and not has_static

        if is_ho_with_deps and not has_header_only:
            output.append(line)
            output.append("# Header-only")
            changes += 1
            i += 1
            continue

        output.append(line)
        i += 1

    return output, changes


def pass_c(lines):
    """Pass C: Add section comments to _dependencies variables missing them."""
    output = []
    changes = 0
    i = 0

    while i < len(lines):
        line = lines[i]

        # Look for: foo_dependencies = [  (on its own line)
        dep_match = re.match(r"^(\s*)(\w+_dependencies)\s*=\s*\[\s*$", line)
        if not dep_match:
            output.append(line)
            i += 1
            continue

        indent = dep_match.group(1)
        var_name = dep_match.group(2)

        # Collect block until ]
        block = [line]
        j = i + 1
        while j < len(lines):
            if re.match(r"^\s*\]\s*$", lines[j]):
                block.append(lines[j])
                break
            block.append(lines[j])
            j += 1
        close_idx = j

        block_text = "\n".join(block)

        # Already has section comments?
        has_section = "# Package-managed" in block_text or "# Eugo-managed" in block_text
        if has_section:
            output.extend(block)
            i = close_idx + 1
            continue

        # Has non-section comments? Skip to be safe.
        has_other_comments = any(bl.strip().startswith("#") for bl in block[1:-1] if bl.strip())
        if has_other_comments:
            output.extend(block)
            i = close_idx + 1
            continue

        # Extract deps
        deps = []
        for bl in block[1:-1]:
            stripped = bl.strip().rstrip(",")
            if stripped:
                deps.append(stripped)

        if not deps:
            output.extend(block)
            i = close_idx + 1
            continue

        # Rebuild with section comments
        new_block = format_deps_block(indent, var_name, deps)
        output.extend(new_block)
        changes += 1
        i = close_idx + 1

    return output, changes


def fix_file(filepath):
    """Process a single meson.build file through all three passes."""
    text = Path(filepath).read_text()
    lines = text.split("\n")

    lines, ca = pass_a(lines)
    lines, cb = pass_b(lines)
    lines, cc = pass_c(lines)

    total = ca + cb + cc
    new_text = "\n".join(lines)

    if new_text != text:
        Path(filepath).write_text(new_text)
        print(f"  Fixed {total} ({ca}A+{cb}B+{cc}C) in {Path(filepath).name}")
        return total
    else:
        return 0


def main():
    src_ray = Path("/Users/benjaminleff/repos/eugo/ray-meson/src/ray")
    meson_files = sorted(src_ray.rglob("meson.build"))

    total = 0
    files_changed = 0
    for f in meson_files:
        n = fix_file(f)
        total += n
        if n > 0:
            files_changed += 1

    print(f"\nTotal: {total} fixes across {files_changed} files")


if __name__ == "__main__":
    main()
