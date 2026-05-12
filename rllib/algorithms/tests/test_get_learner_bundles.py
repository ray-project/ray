"""Pure-Python unit tests for `_get_learner_bundles` and
`get_learner_bundle_offset`.

These cover the bundle-resource math that `Algorithm.setup` relies on
when pinning aggregator actors to specific PG bundle indices. The tests
do not spin up Ray — they only inspect the dicts returned by the helper
functions.
"""
import pytest

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.utils import (
    _get_learner_bundles,
    get_learner_bundle_offset,
)

# Silence unused-import warnings; `get_learner_bundle_offset` is exercised
# transitively via the assertion inside `Algorithm.default_resource_request`.
_ = get_learner_bundle_offset


def _config(**learners_kwargs):
    cfg = AlgorithmConfig().learners(**learners_kwargs)
    # Disable env runners to keep things minimal; the helpers under test
    # only read learner-related fields.
    cfg = cfg.env_runners(num_env_runners=0)
    return cfg


def test_get_learner_bundles_local_learner_no_aggregator():
    """num_learners=0, num_aggregator_actors_per_learner=0 -> no bundles
    (the local learner runs in the main process bundle, no extras)."""
    bundles = _get_learner_bundles(_config(num_learners=0))
    assert bundles == []


def test_get_learner_bundles_local_learner_with_aggregators():
    """num_learners=0 + N aggregators: each aggregator gets its own
    dedicated bundle of size num_cpus_per_aggregator_actor."""
    bundles = _get_learner_bundles(
        _config(num_learners=0, num_aggregator_actors_per_learner=2)
    )
    assert bundles == [{"CPU": 1}, {"CPU": 1}]


def test_get_learner_bundles_local_learner_aggregator_custom_resources():
    """custom_resources_per_aggregator_actor must appear in each
    aggregator bundle when num_learners=0."""
    bundles = _get_learner_bundles(
        _config(
            num_learners=0,
            num_aggregator_actors_per_learner=2,
            num_cpus_per_aggregator_actor=2,
            custom_resources_per_aggregator_actor={"my_label": 0.5},
        )
    )
    assert bundles == [
        {"CPU": 2, "my_label": 0.5},
        {"CPU": 2, "my_label": 0.5},
    ]


def test_get_learner_bundles_remote_learner_no_aggregator():
    """num_learners=2, no aggregators: one CPU bundle per learner."""
    bundles = _get_learner_bundles(_config(num_learners=2, num_cpus_per_learner=1))
    assert bundles == [{"CPU": 1, "GPU": 0}, {"CPU": 1, "GPU": 0}]


def test_get_learner_bundles_remote_learner_with_aggregators():
    """Aggregator CPU is baked into the learner bundle so co-located
    aggregators can claim from it via PG bundle pinning. With
    num_cpus_per_learner=1 and 2 aggregators @ 1 CPU each, each learner
    bundle is {CPU: 3, GPU: 0}."""
    bundles = _get_learner_bundles(
        _config(
            num_learners=2,
            num_cpus_per_learner=1,
            num_aggregator_actors_per_learner=2,
        )
    )
    assert bundles == [{"CPU": 3, "GPU": 0}, {"CPU": 3, "GPU": 0}]


def test_get_learner_bundles_remote_learner_aggregator_cpu_override():
    """num_cpus_per_aggregator_actor=2: bundle reserves CPU for
    `n_agg * num_cpus_per_aggregator_actor` aggregator CPUs."""
    bundles = _get_learner_bundles(
        _config(
            num_learners=1,
            num_cpus_per_learner=1,
            num_aggregator_actors_per_learner=3,
            num_cpus_per_aggregator_actor=2,
        )
    )
    # 1 (learner) + 3 aggregators * 2 CPU each = 7 CPU.
    assert bundles == [{"CPU": 7, "GPU": 0}]


def test_get_learner_bundles_remote_learner_aggregator_custom_resources():
    """custom_resources_per_aggregator_actor is summed across the
    aggregators sharing this bundle. 2 aggregators * 0.5 each = 1.0."""
    bundles = _get_learner_bundles(
        _config(
            num_learners=1,
            num_cpus_per_learner=1,
            num_aggregator_actors_per_learner=2,
            custom_resources_per_aggregator_actor={"my_label": 0.5},
        )
    )
    assert bundles == [{"CPU": 3, "GPU": 0, "my_label": 1.0}]


@pytest.mark.parametrize("num_env_runners", [0, 1, 4])
@pytest.mark.parametrize("has_eval", [False, True])
def test_get_learner_bundle_offset_matches_actual_bundle_layout(
    num_env_runners, has_eval
):
    """The offset returned by `get_learner_bundle_offset` must match
    the actual position of the first learner bundle in the placement
    group factory produced by `Algorithm.default_resource_request`.

    This is the invariant `Algorithm.setup` relies on when pinning
    aggregators to specific bundle indices. If the bundle order ever
    drifts, `default_resource_request` itself raises an assertion --
    this test exercises that assertion path across a few config shapes.
    """
    # Use a concrete algo config (PPO) so `algo_class` is set --
    # `default_resource_request` reads it during validation.
    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .env_runners(num_env_runners=num_env_runners)
    )
    if has_eval:
        config = config.evaluation(evaluation_num_env_runners=2, evaluation_interval=1)
    # The assertion inside `default_resource_request` checks that
    # `get_learner_bundle_offset` agrees with the actual bundle layout.
    # If it ever drifts, this call raises AssertionError.
    pg = config.algo_class.default_resource_request(config)
    bundles = pg.bundles
    # Sanity: total bundle count = 1 (main) + N (env) + K (eval) + 0
    # (offline_eval, since we don't enable it) + 1 (learner bundle, PPO
    # creates one even with num_learners=0 -- wait, no; num_learners==0
    # produces no learner bundle).
    expected_total = 1 + num_env_runners + (2 if has_eval else 0)
    assert len(bundles) == expected_total, f"Got {len(bundles)} bundles: {bundles}"


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
