import gym
import pytest
import ray

from sac import __version__, DEFAULT_CONFIG, SACTrainer


class GymEnv:
    def __init__(self, config: dict):
        import pybullet_envs  # Needed to register PyBullet envs with gym
        assert pybullet_envs  # Just to ensure that the above import is not removed
        self.config = config
        self.env_id = config["env_id"]
        self._env = gym.make(self.env_id)

    def __getattr__(self, item):
        return getattr(self._env, item)


@pytest.fixture
def ray_env():
    if not ray.is_initialized():
        ray.init()
    yield
    ray.shutdown()


@pytest.fixture
def config(env_id):
    config = DEFAULT_CONFIG.copy()
    config["env_config"] = dict(env_id=env_id)
    if env_id == "Pendulum-v0":
        config["timesteps_per_iteration"] = 100
    config["num_workers"] = 8
    config["num_envs_per_worker"] = 8
    return config


@pytest.fixture
def trainer(config):
    return SACTrainer(config=config, env=GymEnv)


@pytest.mark.parametrize(
    "env_id, threshold", [
        ("Pendulum-v0", -750),
    ]
)
@pytest.mark.usefixtures("ray_env")
def test_convergence(trainer, env_id, threshold):
    mean_episode_reward = -float("inf")
    for i in range(300):
        result = trainer.train()
        mean_episode_reward = result["episode_reward_mean"]
        print(f"{i}: {mean_episode_reward}")
    assert mean_episode_reward >= threshold


def test_version():
    assert __version__ == '0.1.0'
