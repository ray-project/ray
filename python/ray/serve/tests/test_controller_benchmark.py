import sys

import pytest

from ray.serve._private.benchmarks.common import run_controller_benchmark


@pytest.mark.asyncio
async def test_run_controller_benchmark(ray_start_stop):
    """Test that run_controller_benchmark runs and returns valid samples."""
    config = {
        "checkpoints": [1, 2],
        "marination_period_s": 15,
        "sample_interval_s": 5,
    }
    samples = await run_controller_benchmark(config=config)

    assert len(samples) > 0, "Expected at least one sample"

    expected_keys = [
        "target_replicas",
        "autoscale_duration_s",
        "loop_duration_mean_s",
        "loops_per_second",
        "event_loop_delay_s",
        "num_asyncio_tasks",
        "deployment_state_update_mean_s",
        "application_state_update_mean_s",
        "proxy_state_update_mean_s",
        "node_update_min_s",
        "handle_metrics_delay_mean_ms",
        "replica_metrics_delay_mean_ms",
        "process_memory_mb",
    ]

    for sample in samples:
        for key in expected_keys:
            assert key in sample, f"Sample missing expected key: {key}"

    # Verify we have samples for each checkpoint
    replicas_seen = {s["target_replicas"] for s in samples}
    assert 1 in replicas_seen
    assert 2 in replicas_seen

    # Verify numeric fields are reasonable
    for sample in samples:
        assert sample["loops_per_second"] >= 0
        assert sample["process_memory_mb"] >= 0
        assert sample["autoscale_duration_s"] >= 0


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-s", __file__]))
