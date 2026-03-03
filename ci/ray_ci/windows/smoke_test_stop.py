"""Smoke test: verify ray stop kills all Ray Python processes on Windows.

Installs via the built wheel (not source), so this faithfully reproduces
the environment a user would have after pip-installing Ray.

Expected to be invoked by run_smoke_test_stop.py, which handles
installing the wheel and setting up the container environment.
"""
import subprocess
import sys

_PS_FILTER = (
    "Get-CimInstance Win32_Process -Filter \"name='python.exe'\""
    " | Where-Object { $_.CommandLine -like '*ray*' }"
)


def _ps_ray_python_pids():
    """Return PIDs of python.exe processes with 'ray' in their command line."""
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            f"{_PS_FILTER} | Select-Object -ExpandProperty ProcessId",
        ],
        capture_output=True,
        text=True,
    )
    pids = set()
    for line in result.stdout.splitlines():
        line = line.strip()
        if line.isdigit():
            pids.add(int(line))
    return pids


def _ps_ray_python_info():
    """Return a PowerShell listing of Ray Python processes (PIDs + command lines)."""
    result = subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            f"{_PS_FILTER} | Select-Object ProcessId,CommandLine | Format-List",
        ],
        capture_output=True,
        text=True,
    )
    return result.stdout.strip()


def main():
    import ray  # imported here so the import itself doesn't affect pids_before

    pids_before = _ps_ray_python_pids()

    ray.init(include_dashboard=True)
    subprocess.run(["ray", "stop"], check=False)

    pids_after = _ps_ray_python_pids()
    hanging = pids_after - pids_before

    if not hanging:
        print("OK: no Ray Python processes remain after ray stop")
        sys.exit(0)

    print(
        f"FAIL: ray stop left {len(hanging)} Ray Python process(es) running "
        f"(PIDs: {sorted(hanging)}):",
        file=sys.stderr,
    )
    print(_ps_ray_python_info(), file=sys.stderr)

    # Clean up so the CI machine isn't left dirty.
    subprocess.run(
        [
            "powershell",
            "-NoProfile",
            "-Command",
            f"{_PS_FILTER} | ForEach-Object {{ $_.Terminate() }}",
        ],
        capture_output=True,
    )
    sys.exit(1)


if __name__ == "__main__":
    main()
