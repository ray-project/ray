import pytest

from ray.rllib.algorithms.ppo import PPOConfig
from ray.rllib.algorithms.utils import _get_main_process_bundle


def test_custom_resources_for_main_process_default():
    config = PPOConfig()
    assert config.custom_resources_for_main_process == {}


def test_custom_resources_for_main_process_setter():
    config = PPOConfig().resources(
        custom_resources_for_main_process={"on_demand": 0.001}
    )
    assert config.custom_resources_for_main_process == {"on_demand": 0.001}


def test_main_process_bundle_with_custom_resources_no_learners():
    config = (
        PPOConfig()
        .resources(custom_resources_for_main_process={"on_demand": 0.001})
        .learners(num_learners=0)
    )
    bundle = _get_main_process_bundle(config)
    assert "on_demand" in bundle
    assert bundle["on_demand"] == 0.001


def test_main_process_bundle_with_custom_resources_with_learners():
    config = (
        PPOConfig()
        .resources(custom_resources_for_main_process={"on_demand": 0.001})
        .learners(num_learners=1)
    )
    bundle = _get_main_process_bundle(config)
    assert "on_demand" in bundle
    assert bundle["on_demand"] == 0.001


def test_main_process_bundle_without_custom_resources():
    config = PPOConfig().learners(num_learners=1)
    bundle = _get_main_process_bundle(config)
    assert "CPU" in bundle
    assert "GPU" in bundle
    # No extra keys beyond CPU and GPU.
    assert set(bundle.keys()) == {"CPU", "GPU"}


def test_default_resource_request_includes_custom_resources():
    from ray.rllib.algorithms.ppo import PPO

    config = (
        PPOConfig()
        .environment("CartPole-v1")
        .resources(custom_resources_for_main_process={"on_demand": 0.001})
        .learners(num_learners=1)
        .env_runners(num_env_runners=0)
    )
    pgf = PPO.default_resource_request(config)
    # The first bundle in the placement group is the main process bundle.
    bundles = pgf.bundles
    assert len(bundles) > 0
    main_bundle = bundles[0]
    assert "on_demand" in main_bundle
    assert main_bundle["on_demand"] == 0.001


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
