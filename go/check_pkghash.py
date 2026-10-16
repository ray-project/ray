#!/usr/bin/env python3
"""D4: verify that raygo and go_runtime.so expose the same package hash for
github.com/ray-project/ray/go/pkg/runtime/api.

The Go runtime rejects a plugin built with a different version of a package
("plugin was built with a different version of package ..."), so raygo and
go_runtime.so must be built from the same source revision. This script turns
the manual readelf diagnosis in CLAUDE.md into a build-time error.

Usage:
    check_pkghash.py <binary> [<binary> ...]

Exits 0 when all binaries share the same hash, 1 on mismatch, 2 on usage error.
"""

import re
import subprocess
import sys

SYMBOL = "pkghashbytes"
PACKAGE = "runtime/api"

# readelf -SW pads single-digit section numbers with a space ("[ 1]") while
# multi-digit numbers are not padded ("[10]"), so a naive split() misaligns
# the columns for sections 0-9. Match the number bracket explicitly instead.
_SECTION_RE = re.compile(
    r"\s*\[\s*(\d+)\]\s+(?P<name>\S+)\s+\S+\s+"
    r"(?P<addr>[0-9a-fA-F]+)\s+(?P<off>[0-9a-fA-F]+)\s+"
    r"(?P<size>[0-9a-fA-F]+)"
)


def _parse_sections(sec_out: str) -> list[tuple[str, int, int, int]]:
    """Parse readelf -SW section headers into (name, addr, off, size) tuples.

    Section 0 is the unnamed NULL section; readelf renders its type in the
    first column, so it is skipped rather than mis-parsed as a real section.
    """
    sections = []
    for line in sec_out.splitlines():
        m = _SECTION_RE.match(line)
        if m is None:
            continue
        if m.group(1) == "0":
            continue
        sections.append(
            (
                m.group("name"),
                int(m.group("addr"), 16),
                int(m.group("off"), 16),
                int(m.group("size"), 16),
            )
        )
    return sections


def package_hash(binary: str) -> tuple[str, int]:
    """Return (8-byte package hash as hex, file offset of the hash bytes)."""
    sym_out = subprocess.run(
        ["readelf", "-sW", binary],
        capture_output=True,
        text=True,
        errors="replace",
        check=True,
    ).stdout
    sym_line = next(
        (l for l in sym_out.splitlines() if SYMBOL in l and PACKAGE in l), None
    )
    if sym_line is None:
        raise RuntimeError(f"{SYMBOL}.*{PACKAGE} symbol not found in {binary}")
    cols = sym_line.split()
    # readelf -sW: Num: Value Size Type Bind Vis Ndx Name
    sym_addr = int(cols[1], 16)

    sec_out = subprocess.run(
        ["readelf", "-SW", binary],
        capture_output=True,
        text=True,
        errors="replace",
        check=True,
    ).stdout
    sections = _parse_sections(sec_out)

    for _name, addr, off, size in sections:
        if addr <= sym_addr < addr + size:
            file_off = off + (sym_addr - addr)
            with open(binary, "rb") as f:
                f.seek(file_off)
                raw = f.read(8)
            return raw.hex(), file_off
    raise RuntimeError(f"could not locate {SYMBOL} bytes in any section of {binary}")


def verify_binaries(binaries: list[str]) -> None:
    """Raise RuntimeError when any binary's package hash differs from the first."""
    hashes = [(b, package_hash(b)[0]) for b in binaries]
    for b, h in hashes:
        print(f"{b}: {h}")
    first = hashes[0][1]
    for b, h in hashes[1:]:
        if h != first:
            raise RuntimeError(
                f"package hash mismatch: {hashes[0][0]}={first}, {b}={h}"
            )


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2
    try:
        verify_binaries(sys.argv[1:])
    except (RuntimeError, subprocess.CalledProcessError, OSError) as exc:
        print(f"FAIL: {exc}", file=sys.stderr)
        return 1
    print("OK: all binaries share the same package hash")
    return 0


if __name__ == "__main__":
    sys.exit(main())
