# shellcheck shell=bash
# Shared helpers for switching between the pristine ray-2.55.0 source tree
# and this fork's M6 tree, used by both the perf driver
# (run_optimization_bench.sh) and the correctness gate driver
# (run_optimization_gate.sh).
#
# Sourcing this file sets up:
#   SCRIPT_DIR       — directory of the sourcing script
#   REPO_ROOT        — the fork's repository root (one level above SCRIPT_DIR)
#   M6_TREE          — path to this fork's python/ray (defaults to REPO_ROOT/python/ray)
#   PRISTINE_TREE    — required env var; path to a pristine ray-2.55.0 python/ray
#   FINDER           — path to the setuptools editable-install finder
#                      (__editable___ray_<ver>_finder.py in site-packages)
#   FINDER_MODULE    — basename of FINDER without .py suffix
#   FINDER_CACHE_DIR — __pycache__ dir next to FINDER
#
# After sourcing, call ``set_tree <path>`` to point the editable-install
# MAPPING at the given python/ray directory and verify the switch took
# effect.

# SCRIPT_DIR / REPO_ROOT are expected to be set by the sourcing script
# before calling functions defined here, because BASH_SOURCE[0] inside a
# sourced file resolves to this helper, not the caller.

# Verify a python/ray source tree exists and has its compiled artifacts
# staged (see stage_ray_artifacts.sh). Missing artifacts produce a clear
# error instead of a confusing ``No module named 'ray._raylet'`` later.
_require_tree() {
    local label="$1" tree="$2"
    if [ ! -f "$tree/__init__.py" ]; then
        echo "ERROR: ${label}=${tree} does not contain python/ray/__init__.py" >&2
        exit 1
    fi
    if [ ! -e "$tree/_raylet.so" ]; then
        echo "ERROR: ${label}=${tree} has no _raylet.so — the source tree is missing ray's compiled artifacts." >&2
        echo "       Run: ${SCRIPT_DIR}/stage_ray_artifacts.sh ${tree}" >&2
        exit 1
    fi
}

# Derive M6 tree from the caller's REPO_ROOT.
M6_TREE="${M6_TREE:-$REPO_ROOT/python/ray}"
_require_tree M6_TREE "$M6_TREE"

# Pristine tree: no sensible default — user must point at their clone of
# ray-2.55.0. See the header comment of the sourcing script for guidance.
: "${PRISTINE_TREE:?PRISTINE_TREE must be set to a pristine ray-2.55.0 python/ray directory}"
_require_tree PRISTINE_TREE "$PRISTINE_TREE"

# Locate the setuptools editable-install finder. ``pip install -e python/``
# creates ``<site-packages>/__editable___ray_<ver>_finder.py`` whose
# ``MAPPING`` variable decides which source tree ``import ray`` resolves
# to. set_tree() rewrites that variable to swap trees.
_find_finder() {
    local site hits
    site=$(python -c 'import sysconfig; print(sysconfig.get_paths()["purelib"])')
    mapfile -t hits < <(ls "$site"/__editable___ray_*_finder.py 2>/dev/null)
    if [ "${#hits[@]}" -eq 0 ]; then
        echo "ERROR: no __editable___ray_*_finder.py found in $site." >&2
        echo "       Did you 'pip install -e python/' from the fork's repo root?" >&2
        exit 1
    fi
    if [ "${#hits[@]}" -gt 1 ]; then
        echo "ERROR: multiple editable ray finders in $site; set FINDER to pick one:" >&2
        printf '         %s\n' "${hits[@]}" >&2
        exit 1
    fi
    echo "${hits[0]}"
}
FINDER="${FINDER:-$(_find_finder)}"
FINDER_MODULE="$(basename "$FINDER" .py)"
FINDER_CACHE_DIR="$(dirname "$FINDER")/__pycache__"

set_tree() {
    local path="$1"
    sed -i "s|MAPPING: dict\[str, str\] = {'ray': '[^']*'}|MAPPING: dict[str, str] = {'ray': '${path}'}|" "$FINDER"
    # Purge bytecode cache — sed rewrites the .py but Python may still pick
    # up a stale .pyc with the old MAPPING baked in.
    rm -f "$FINDER_CACHE_DIR/${FINDER_MODULE}.cpython-"*.pyc
    # Verify from a clean cwd. Any directory containing a `ray/` subdir
    # (e.g. /tmp if `ray.init()` has run there, or the repo root if
    # imported from above python/ray) is treated by Python as a namespace
    # package entry and shadows the editable finder via PathFinder
    # (earlier in meta_path). Using mktemp guarantees no such shadow.
    local got clean
    clean=$(mktemp -d)
    got=$(cd "$clean" && python -c "import ray; print(ray.__file__)")
    rmdir "$clean"
    if [[ "$got" != "${path}/__init__.py" ]]; then
        echo "ERROR: tree switch failed, ray resolves to $got not ${path}/__init__.py" >&2
        exit 1
    fi
}
