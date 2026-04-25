"""Test how APPO handles per-policy data imbalance in multi-agent setups.

Note: PPO will always use "equalize" data across policies. So each policy will train on the same amount of data.
APPO, in contrast to PPO, will train on varying amounts of data per policy.

When a policy_mapping_fn maps more agents to one policy than another, the resulting
MultiAgentBatch has unequal per-policy data. This test verifies:
1. Default APPO (no minibatch_size): policies train on unequal amounts of data.
2. With minibatch_size set: MiniBatchCyclicIterator equalizes per-policy batch sizes.
"""

from ray.rllib.algorithms.appo import APPOConfig
from ray.rllib.examples.envs.classes.multi_agent import MultiAgentCartPole
from ray.rllib.utils.metrics import NUM_MODULE_STEPS_TRAINED

NUM_AGENTS = 5
BATCH_SIZE = 500


def policy_mapping_fn(agent_id, episode, **kw):
    return "policy_a" if agent_id in (0, 1, 2, 3) else "policy_b"


def _build_config(*, minibatch_size=None, num_epochs=1):
    config = (
        APPOConfig()
        .environment(MultiAgentCartPole, env_config={"num_agents": NUM_AGENTS})
        .multi_agent(
            policies={"policy_a", "policy_b"},
            policy_mapping_fn=policy_mapping_fn,
        )
        .training(
            train_batch_size_per_learner=500,
        )
        .learners(num_learners=0)
        .env_runners(num_env_runners=1)
    )
    if minibatch_size is not None:
        config.training(minibatch_size=minibatch_size, num_epochs=num_epochs)
    return config


def test_default_appo_unequal_data():
    """Without minibatch_size, policy_a trains on more data than policy_b."""
    algo = _build_config().build()

    learners = algo.train()["learners"]
    steps_a = learners["policy_a"][NUM_MODULE_STEPS_TRAINED]
    steps_b = learners["policy_b"][NUM_MODULE_STEPS_TRAINED]
    # steps_a should be 4x more data than steps_b
    assert steps_a / steps_b > 2.5, (
        "Expected policy_a to train on more data than policy_b "
        "with biased policy mapping and no minibatch_size."
    )


def test_minibatch_equalizes_data():
    """With minibatch_size, both policies train on equal amounts of data."""
    algo = _build_config(minibatch_size=50, num_epochs=4).build()

    learners = algo.train()["learners"]
    steps_a = learners["policy_a"][NUM_MODULE_STEPS_TRAINED]
    steps_b = learners["policy_b"][NUM_MODULE_STEPS_TRAINED]
    assert steps_a == steps_b, (
        "Expected equal per-policy training steps when "
        "minibatch_size is set (MiniBatchCyclicIterator)."
    )


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", __file__]))
