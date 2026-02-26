r"""Verify that python/ray/serve no longer references ray._private.

Run from repo root after migration:
  cd python && python -c "
  from ray._common.tests.verify_serve_no_private_imports import verify
  verify()
  "
Or with grep (no ray needed):
  grep -R 'ray\._private' ray/serve --include='*.py' && echo 'FAIL' || echo 'PASS'
"""

import os


def verify() -> bool:
    """Return True if serve has no ``ray._private`` references."""
    serve_dir = os.path.join(os.path.dirname(__file__), "..", "..", "serve")
    if not os.path.isdir(serve_dir):
        return True
    failures = []
    for root, _, files in os.walk(serve_dir):
        for f in files:
            if not f.endswith(".py"):
                continue
            path = os.path.join(root, f)
            with open(path, "r", encoding="utf-8", errors="ignore") as fp:
                for i, line in enumerate(fp, 1):
                    if "ray._private" in line:
                        failures.append((path, i, line.strip()))
    if failures:
        for path, line_no, line in failures:
            print(f"{path}:{line_no}: {line}")
        return False
    return True


if __name__ == "__main__":
    import sys

    ok = verify()
    print("PASS: serve has no ray._private imports" if ok else "FAIL: see above")
    sys.exit(0 if ok else 1)
