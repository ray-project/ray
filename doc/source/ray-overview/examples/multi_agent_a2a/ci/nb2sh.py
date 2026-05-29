#!/usr/bin/env python3
"""Convert README.ipynb code cells into a README.sh shell script.

Extracts shell commands from code cells (lines prefixed with ``!`` in
Jupyter) and plain shell-looking cells, skipping pure Python illustration
cells that contain imports, decorators, or async/await syntax.

Also extracts fenced ``bash`` / ``shell`` / ``sh`` code blocks from
markdown cells so that commands documented in prose are not lost.

Post-processing rewrites applied to the raw extracted lines:
  * ``serve run …`` gets ``--non-blocking``, a readiness poll, and a cleanup trap.
  * ``%env VAR=val`` Jupyter magic is converted to ``export VAR=val``.
  * Hardcoded ``%env BASE_URL`` / ``%env ANYSCALE_API_TOKEN`` lines are replaced
    with dynamic extraction via ``anyscale service status``.
  * ``anyscale service wait`` is injected after ``anyscale service deploy``.

Usage:
    python nb2sh.py <notebook.ipynb> <output.sh>
"""

import re
import sys

import nbformat

# Patterns that indicate a code cell is pure Python (not shell).
_PYTHON_RE = re.compile(
    r"(?:^\s*(?:from\s|import\s|@\w|(?:async\s+)?def\s|class\s))|(?:\bawait\s)",
    re.MULTILINE,
)

# Matches fenced code blocks tagged as bash / shell / sh.
_BASH_FENCE_RE = re.compile(
    r"```(?:bash|shell|sh)\s*\n(.*?)```", re.DOTALL
)

_PLACEHOLDER_RE = re.compile(r"<[^>]+>")

_SERVE_RUN_RE = re.compile(r"^serve run\s+(.+)$")

_SERVE_RUN_BLOCK = """\
# Start Serve (returns immediately with --non-blocking)
serve run {config} --non-blocking --working-dir .

# Wait for all applications to be RUNNING
echo "Waiting for all Serve applications to be ready..."
for i in $(seq 1 600); do
    STATUS=$(serve status 2>/dev/null || true)
    if echo "$STATUS" | grep -q 'status: RUNNING' && \\
       ! echo "$STATUS" | grep -qE 'status: (DEPLOYING|NOT_STARTED)'; then
        echo "All applications RUNNING after ${{i}}s"
        break
    fi
    if echo "$STATUS" | grep -qE 'status: DEPLOY_FAILED'; then
        echo "ERROR: an application failed to deploy"
        serve status
        exit 1
    fi
    sleep 1
done

# Cleanup on exit
trap 'serve shutdown -y 2>/dev/null || true' EXIT"""

_DEPLOY_RE = re.compile(r"^anyscale service deploy\s+(.+)$")

# Extract the file path from flags like ``-f path.yaml`` or ``--config-file path.yaml``.
_CONFIG_PATH_RE = re.compile(r"(?:-f|--config-file)\s+(\S+)")


def _extract_config_path(flags: str) -> str:
    """Return the config file path from deploy-style CLI flags."""
    m = _CONFIG_PATH_RE.search(flags)
    return m.group(1) if m else flags.strip()

# Matches hardcoded %env / export lines for BASE_URL or ANYSCALE_API_TOKEN.
_HARDCODED_ENV_RE = re.compile(
    r"^(?:%env|export)\s+(?:BASE_URL|ANYSCALE_API_TOKEN)="
)

_DYNAMIC_ENV_BLOCK = """\
# Extract BASE_URL and ANYSCALE_API_TOKEN from the deployed service
SERVICE_STATUS=$(anyscale service status {flags} --json)
export BASE_URL=$(echo "$SERVICE_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['query_url'])")
export ANYSCALE_API_TOKEN=$(echo "$SERVICE_STATUS" | python3 -c "import sys,json; print(json.load(sys.stdin)['query_auth_token'])")"""


def _is_shell_cell(source: str) -> bool:
    """Return True if the cell contains shell commands."""
    if any(line.startswith("!") for line in source.splitlines()):
        return True
    # No bang lines — check whether the cell looks like Python code.
    code_lines = [
        l.strip()
        for l in source.splitlines()
        if l.strip() and not l.strip().startswith("#")
    ]
    if not code_lines:
        return False
    return not _PYTHON_RE.search(source)


def _extract_bash_fences(source: str) -> list[str]:
    """Return shell lines from fenced ```bash blocks in markdown.

    Blocks that contain angle-bracket placeholders (e.g. ``<your-key>``)
    are skipped because they are documentation, not runnable commands.
    """
    lines: list[str] = []
    for match in _BASH_FENCE_RE.finditer(source):
        block = match.group(1).strip()
        if not block or _PLACEHOLDER_RE.search(block):
            continue
        lines.extend(block.splitlines())
    return lines


def _postprocess(lines: list[str]) -> list[str]:
    """Rewrite raw extracted lines for CI-friendly execution."""
    out: list[str] = []
    deploy_flags: str | None = None  # set after we see anyscale service deploy
    env_block_emitted = False

    for line in lines:
        # Drop hardcoded BASE_URL / ANYSCALE_API_TOKEN assignments.
        # They are replaced by a dynamic block emitted once.
        if _HARDCODED_ENV_RE.match(line):
            if deploy_flags and not env_block_emitted:
                out.append(_DYNAMIC_ENV_BLOCK.format(flags=deploy_flags))
                env_block_emitted = True
            continue

        # %env VAR=val  →  export VAR=val
        if line.startswith("%env "):
            out.append("export " + line[len("%env "):])
            continue

        # serve run <config>  →  --non-blocking + poll + trap
        m = _SERVE_RUN_RE.match(line)
        if m:
            out.append(_SERVE_RUN_BLOCK.format(config=m.group(1)))
            continue

        # anyscale service deploy <flags>  →  deploy + wait
        m = _DEPLOY_RE.match(line)
        if m:
            deploy_flags = m.group(1)
            out.append(line)
            out.append(f"anyscale service wait {deploy_flags} --timeout-s 1200")
            continue

        out.append(line)

    # Terminate the Anyscale service at the end of the script.
    # Use --name instead of -f because `terminate` rejects extra fields
    # (applications, logging_config, …) in the full service config YAML.
    if deploy_flags:
        out.append("")
        out.append("# Tear down the Anyscale service")
        out.append(
            f"SERVICE_NAME=$(python3 -c \"import yaml; print(yaml.safe_load(open('{_extract_config_path(deploy_flags)}'))['name'])\")"
        )
        out.append("anyscale service terminate --name \"$SERVICE_NAME\"")

    return out


def nb2sh(notebook_path: str, output_path: str) -> None:
    nb = nbformat.read(notebook_path, as_version=4)

    lines = [
        "#!/usr/bin/env bash",
        "# Auto-generated from README.ipynb — do not edit manually.",
        "set -euo pipefail",
        "",
    ]

    for cell in nb.cells:
        source = cell.source.strip()
        if not source:
            continue

        if cell.cell_type == "markdown":
            bash_lines = _extract_bash_fences(source)
            if bash_lines:
                lines.extend(bash_lines)
                lines.append("")
            continue

        if cell.cell_type != "code":
            continue

        if not _is_shell_cell(source):
            continue

        for line in source.splitlines():
            lines.append(line[1:] if line.startswith("!") else line)

        lines.append("")

    lines = _postprocess(lines)

    with open(output_path, "w") as f:
        f.write("\n".join(lines))

    print(f"Wrote {output_path}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(f"Usage: {sys.argv[0]} <notebook.ipynb> <output.sh>", file=sys.stderr)
        sys.exit(1)
    nb2sh(sys.argv[1], sys.argv[2])
