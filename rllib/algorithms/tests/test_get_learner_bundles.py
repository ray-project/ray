import pytest

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig
from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.utils import (
    _get_learner_bundles,
    compute_aggregator_bundle_indices,
)


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


# --- compute_aggregator_bundle_indices -----------------------------------
# These tests fix the load-bearing co-location decision: given each
# Learner's actual node (post-Ray-Train-sort) and the trial PG's
# bundle-to-node table, pick the right bundle index per aggregator.
# Verifying placement end-to-end would need a multi-node fake cluster
# *and* Ray Train cooperating with our bundle pinning, which is brittle
# in a unit test. Testing the decision function directly nails down the
# correctness invariant without that overhead.


def test_compute_aggregator_bundle_indices_no_aggregators():
    """`num_aggregator_actors_per_learner=0` → empty mapping."""
    out = compute_aggregator_bundle_indices(
        num_learners=2,
        num_aggregator_actors_per_learner=0,
        learner_bundle_offset=1,
        learner_node_ids=["A", "B"],
        bundles_to_node={0: "A", 1: "A", 2: "B"},
    )
    assert out == {}


def test_compute_aggregator_bundle_indices_local_learner_same_node():
    """`num_learners=0`: aggregator bundles on the local Learner's
    (driver's) node are picked."""
    out = compute_aggregator_bundle_indices(
        num_learners=0,
        num_aggregator_actors_per_learner=2,
        learner_bundle_offset=1,
        learner_node_ids=["A"],
        # Bundle 0 = main_process (on A); bundles 1,2 = aggregator
        # bundles, both also on A under PACK.
        bundles_to_node={0: "A", 1: "A", 2: "A"},
    )
    assert out == {0: [1, 2]}


def test_compute_aggregator_bundle_indices_local_learner_split_nodes():
    """`num_learners=0`: only bundles co-located with the driver are
    picked; bundles that drifted to another node are dropped."""
    out = compute_aggregator_bundle_indices(
        num_learners=0,
        num_aggregator_actors_per_learner=2,
        learner_bundle_offset=1,
        learner_node_ids=["A"],
        # Aggregator bundle 2 ended up on node B -> not co-located.
        bundles_to_node={0: "A", 1: "A", 2: "B"},
    )
    assert out == {0: [1]}


def test_compute_aggregator_bundle_indices_two_learners_one_node_each():
    """`num_learners=2` with one Learner per node: aggregators for each
    Learner pin to the bundle on the Learner's node, *regardless of the
    Learner's logical index*. This is the post-sort scenario."""
    # Bundle layout: 0=main, 1=learner_bundle_A, 2=learner_bundle_B.
    # Ray Train sorted: logical learner 0 ended up on node B, logical
    # learner 1 on node A (reversed from bundle order). The helper must
    # pick bundle 2 for learner 0 and bundle 1 for learner 1.
    out = compute_aggregator_bundle_indices(
        num_learners=2,
        num_aggregator_actors_per_learner=2,
        learner_bundle_offset=1,
        learner_node_ids=["B", "A"],
        bundles_to_node={0: "A", 1: "A", 2: "B"},
    )
    assert out == {0: [2, 2], 1: [1, 1]}


def test_compute_aggregator_bundle_indices_two_learners_same_node():
    """Two Learners on the same node consume distinct learner bundles
    from the per-node pool."""
    out = compute_aggregator_bundle_indices(
        num_learners=2,
        num_aggregator_actors_per_learner=1,
        learner_bundle_offset=1,
        learner_node_ids=["A", "A"],
        bundles_to_node={0: "A", 1: "A", 2: "A"},
    )
    # Each Learner gets a different learner-bundle.
    assert out == {0: [1], 1: [2]}


def test_compute_aggregator_bundle_indices_no_bundle_on_learner_node():
    """If no learner-bundle is on a Learner's actual node (e.g. PG
    layout diverged from expectations), that Learner is omitted from
    the result — the caller falls back to unhinted scheduling."""
    out = compute_aggregator_bundle_indices(
        num_learners=2,
        num_aggregator_actors_per_learner=2,
        learner_bundle_offset=1,
        learner_node_ids=["A", "Z"],  # "Z" matches no bundle.
        bundles_to_node={0: "A", 1: "A", 2: "B"},
    )
    # Learner 0 on A -> bundle 1; Learner 1 on Z -> no match -> dropped.
    assert out == {0: [1, 1]}


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
