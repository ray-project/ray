import os
import sys
from unittest.mock import patch

import pytest
from jsonschema import validate

import ray
import ray._private.usage.usage_lib as ray_usage_lib
from ray._private.test_utils import wait_for_condition
from ray.tests.conftest import maybe_external_redis, ray_start_cluster  # noqa: F401
from ray.tests.test_usage_stats import (  # noqa: F401
    print_dashboard_log,
    schema,
    start_usage_stats_server,
)


def test_usage_report_e2e_with_anyscale_specific_tags(
    monkeypatch,
    ray_start_cluster,  # noqa: F811
    tmp_path,
    start_usage_stats_server,  # noqa: F811
):
    """
    Test usage report works e2e with env vars.
    """
    cluster_config_file_path = tmp_path / "ray_bootstrap_config.yaml"
    cluster_config_file_path.write_text(
        """
cluster_name: minimal
max_workers: 1
provider:
    type: aws
    region: us-west-2
    availability_zone: us-west-2a
"""
    )
    with patch.object(
        ray._private.utils, "get_current_node_cpu_model_name", return_value="TestCPU"
    ), monkeypatch.context() as m:
        m.setenv("HOME", str(tmp_path))
        m.setenv("RAY_USAGE_STATS_ENABLED", "1")
        m.setenv("RAY_USAGE_STATS_REPORT_URL", "http://127.0.0.1:8000")
        m.setenv("RAY_USAGE_STATS_REPORT_INTERVAL_S", "1")
        m.setenv("RAY_USAGE_STATS_EXTRA_TAGS", "extra_k1=extra_v1")

        usage_stats_server = start_usage_stats_server

        cluster = ray_start_cluster
        cluster.add_node(num_cpus=3)
        ray.init(address=cluster.address)

        # record Anyscale specific tag
        ray_usage_lib.record_extra_usage_tag(
            ray_usage_lib.TagKey.RAYLLM_VERSION, "fake_version"
        )
        ray_usage_lib.record_extra_usage_tag(
            ray_usage_lib.TagKey.RAYLLM_COMMIT, "fake_commit"
        )
        ray_usage_lib.record_extra_usage_tag(
            ray_usage_lib.TagKey.LLMFORGE_VERSION, "fake_version"
        )

        """
        Verify the usage stats are reported to the server.
        """
        print("Verifying usage stats report.")
        # Since the interval is 1 second, there must have been
        # more than 5 requests sent within 30 seconds.
        try:
            wait_for_condition(lambda: usage_stats_server.num_reports > 5, timeout=30)
        except Exception:
            print_dashboard_log()
            raise
        payload = usage_stats_server.report_payload

        assert payload["extra_usage_tags"]["rayllm_version"] == "fake_version"
        assert payload["extra_usage_tags"]["rayllm_commit"] == "fake_commit"
        assert payload["extra_usage_tags"]["llmforge_version"] == "fake_version"
        validate(instance=payload, schema=schema)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
