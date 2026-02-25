#!/usr/bin/env python3
"""Remove consecutive duplicate '# Header-only' lines and fix remaining lowercase."""

import os

total_fixes = 0
for root, dirs, files in os.walk("src/ray"):
    for f in files:
        if f != "meson.build":
            continue
        path = os.path.join(root, f)
        with open(path) as fh:
            lines = fh.readlines()

        new_lines = []
        changed = False
        for i, line in enumerate(lines):
            stripped = line.strip()

            # Fix remaining lowercase "# header-only" at start of line
            if stripped == "# header-only":
                line = line.replace("# header-only", "# Header-only")
                changed = True

            # Skip this line if it's a pure "# Header-only" and the previous
            # kept line is also a pure "# Header-only"
            if stripped == "# Header-only" and new_lines and new_lines[-1].strip() == "# Header-only":
                changed = True
                total_fixes += 1
                print(f"  Removed duplicate at {path}:{i + 1}")
                continue

            new_lines.append(line)

        if changed:
            with open(path, "w") as fh:
                fh.writelines(new_lines)

print(f"\nTotal duplicate lines removed: {total_fixes}")
