"""Local repro for ray-project/ray#63241 (label selector half).

Exercises FixedScalingPolicy._send_resource_request directly in-process with a
mocked AutoscalingCoordinator, so we can inspect what gets forwarded without
the cross-actor process boundaries that defeat module-level monkeypatching.

Usage:
    python repro_63241_label_selector.py
"""
from unittest.mock import MagicMock, patch

import ray
import ray.train
from ray.train.v2._internal.execution.scaling_policy import FixedScalingPolicy


def run_case(label_selector, num_workers=1):
    scaling_config = ray.train.ScalingConfig(
        num_workers=num_workers,
        resources_per_worker={"CPU": 1},
        label_selector=label_selector,
    )
    policy = FixedScalingPolicy(scaling_config)

    mock_coordinator = MagicMock()
    # Inject mock into the cached_property to skip actor creation.
    policy.__dict__["_autoscaling_coordinator"] = mock_coordinator

    # The real after_controller_start does ray.get() on the .remote() call;
    # short-circuit that with a pass-through patch.
    mock_run_context = MagicMock()
    mock_run_context.run_id = "repro-run"

    with patch("ray.get", side_effect=lambda x, **_: x):
        policy.after_controller_start(mock_run_context)

    call = mock_coordinator.request_resources.remote.call_args
    return call.kwargs


def main():
    # Case 1: no label_selector — expect [{}] * num_workers.
    kwargs = run_case(label_selector=None, num_workers=2)
    print(f"[case 1 None]      label_selectors={kwargs['label_selectors']!r}")
    assert kwargs["label_selectors"] == [{}, {}], kwargs

    # Case 2: single dict — expect it replicated per worker.
    kwargs = run_case(label_selector={"instance-type": "m6i.xlarge"}, num_workers=2)
    print(f"[case 2 dict]      label_selectors={kwargs['label_selectors']!r}")
    assert kwargs["label_selectors"] == [
        {"instance-type": "m6i.xlarge"},
        {"instance-type": "m6i.xlarge"},
    ], kwargs

    # Case 3: per-worker list — expect it forwarded as-is.
    kwargs = run_case(
        label_selector=[{"zone": "a"}, {"zone": "b"}], num_workers=2
    )
    print(f"[case 3 list]      label_selectors={kwargs['label_selectors']!r}")
    assert kwargs["label_selectors"] == [{"zone": "a"}, {"zone": "b"}], kwargs

    print("\n✓ Label selectors are forwarded through to the coordinator.")


if __name__ == "__main__":
    main()
