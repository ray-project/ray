"""Helpers shared by the Neuron (Trainium / Inferentia) release tests."""
import glob
import json
import os
from typing import Any, Dict

EFA_TX_BYTES_GLOB = "/sys/class/infiniband/*/ports/1/hw_counters/tx_bytes"


def efa_tx_bytes() -> int:
    """Return the bytes sent over all EFA devices of this node, or -1 if unreadable."""
    total, found = 0, False
    for path in glob.glob(EFA_TX_BYTES_GLOB):
        try:
            with open(path) as f:
                total += int(f.read().strip())
            found = True
        except (OSError, ValueError):
            pass
    return total if found else -1


def write_result(result: Dict[str, Any]) -> None:
    """Write the test result where the release test harness picks it up."""
    out = os.environ.get("TEST_OUTPUT_JSON", "/tmp/neuron_test_result.json")
    with open(out, "wt") as f:
        json.dump(result, f, indent=2)
    print(json.dumps(result, indent=2))
