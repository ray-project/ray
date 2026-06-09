"""Result sinks: where the driver writes the final benchmark metrics.

Thin wrapper over the release-test results path so the harness produces output
that the release test infra picks up, while staying runnable locally (where
the Ray test utility / TEST_OUTPUT_JSON env var may be absent).
"""

import json
import logging
import os
from typing import Any, Dict

logger = logging.getLogger(__name__)


def write_results(metrics: Dict[str, Any], experiment_name: str) -> None:
    """Persist final metrics for the release-test harness and for local runs.

    Writes via ``safe_write_to_results_json`` when available (release infra),
    and always mirrors to a local file so prototype runs have an artifact.
    """
    payload = {"experiment": experiment_name, **metrics}

    try:
        from ray._private.test_utils import safe_write_to_results_json

        safe_write_to_results_json(payload)
        logger.info("Wrote results via safe_write_to_results_json.")
    except Exception as e:
        logger.warning(f"safe_write_to_results_json unavailable ({e}); local only.")

    local_root = (
        "/mnt/cluster_storage"
        if os.path.isdir("/mnt/cluster_storage")
        else "/tmp/train_benchmark"
    )
    os.makedirs(local_root, exist_ok=True)
    local_path = os.path.join(local_root, f"{experiment_name}_results.json")
    with open(local_path, "w") as f:
        json.dump(payload, f, indent=2)
    logger.info(f"Wrote results to {local_path}")
