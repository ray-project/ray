from typing import Optional

import gym
import pytest

from ray.rllib.env.wrappers.moab_wrapper import _MoabBaseWrapper
from ray.tune.registry import ENV_CREATOR, _global_registry


@pytest.mark.parametrize("env_name, iterations",
                         [
                             ("MoabMoveToCenterSim-v0", 10),
                             ("MoabMoveToCenterPartialObservableSim-v0", 10),
                              ("MoabMoveToCenterAvoidObstacleSim-v0", 3),],
                         )
@pytest.mark.parametrize("randomize_ball", [True, False])
@pytest.mark.parametrize("randomize_obstacle", [True, False])
@pytest.mark.parametrize("seed", [None, 1])
class TestMoabWrapper:
    @pytest.fixture
    def env_name(self) -> str:
        return "MoabMoveToCenterSim-v0"

    @pytest.fixture
    def randomize_ball(self) -> bool:
        return False

    @pytest.fixture
    def randomize_obstacle(self) -> bool:
        return False

    @pytest.fixture
    def seed(self) -> Optional[int]:
        return None

    @pytest.fixture
    def iterations(self) -> int:
        return 3

    @pytest.fixture
    def moab_env(self,
                 env_name: str,
                 randomize_ball: bool,
                 randomize_obstacle: bool,
                 seed: Optional[int]) -> _MoabBaseWrapper:
        env_creator = _global_registry.get(ENV_CREATOR, env_name)
        env_config = {
            "randomize_ball": randomize_ball,
            "randomize_obstacle": randomize_obstacle,
            "seed": seed,
        }
        return env_creator(env_config)

    def test_observation_space(self, moab_env: _MoabBaseWrapper, iterations: int):
        obs = moab_env.reset()
        assert (moab_env.observation_space.contains(obs),
                f"{moab_env.observation_space} doesn't contain {obs}")
        new_obs, _, _, _ = moab_env.step(moab_env.action_space.sample())
        assert moab_env.observation_space.contains(new_obs)

    def test_action_space_conversion(self, moab_env: _MoabBaseWrapper, iterations: int):
        assert isinstance(moab_env.action_space, gym.spaces.Box)
        moab_env.reset()
        action = moab_env.action_space.sample()
        moab_env.step(action)

    def test_few_iterations(self, moab_env: _MoabBaseWrapper, iterations: int):
        moab_env.reset()
        for _ in range(iterations):
            moab_env.step(moab_env.action_space.sample())
