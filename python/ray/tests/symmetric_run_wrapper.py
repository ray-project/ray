"""Wrapper to run symmetric_run with mocked network interfaces.

This wrapper is needed because symmetric_run spawns subprocesses (ray start,
entrypoint), so mocks applied in pytest don't propagate. This wrapper applies
patches before invoking symmetric_run, allowing us to control what each
'node' sees as its local IP.

If MOCK_HIDE_IP is set, filter out this IP from network interfaces so the
process thinks it's not on the head node.
"""
import os
from unittest.mock import patch

import ray  # noqa: F401 - must import ray first, psutil is vendored
from ray.scripts.symmetric_run import symmetric_run

import psutil

_real_net_if_addrs = psutil.net_if_addrs


def _mocked_net_if_addrs():
    """Return network interfaces, optionally hiding the head IP."""
    real_addrs = _real_net_if_addrs()
    hide_ip = os.environ.get("MOCK_HIDE_IP")
    if not hide_ip:
        return real_addrs

    new_addrs = {}
    for iface, addrs in real_addrs.items():
        # Filter out the IP to be hidden from the list of addresses for this interface.
        filtered_addrs = [addr for addr in addrs if addr.address != hide_ip]
        if filtered_addrs:
            new_addrs[iface] = filtered_addrs
    return new_addrs


if __name__ == "__main__":
    with (
        patch("ray._private.services.find_gcs_addresses", return_value=[]),
        patch("psutil.net_if_addrs", side_effect=_mocked_net_if_addrs),
    ):
        symmetric_run()
