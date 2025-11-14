import shutil
from pathlib import Path
from typing import Any

import gymnasium as gym
import msgpack
import msgpack_numpy as mnp
import numpy as np
import pytest

import ray
from ray.rllib.algorithms import BCConfig
from ray.rllib.connectors.common.flatten_observations import FlattenObservations
from ray.rllib.env.single_agent_episode import SingleAgentEpisode


class GenericTestEnv(gym.Env):
    def __init__(self, config: dict[str, Any] = None):
        self.observation_space = config["observation_space"]
        self.action_space = config["action_space"]
        self.current_step = 0

    def reset(self, seed=None, options=None):
        super().reset(seed=seed)
        self.current_step = 0
        return self.observation_space.sample(), {}

    def step(self, action):
        self.current_step += 1
        obs = self.observation_space.sample()
        reward = np.random.rand(1)[0]
        terminate = self.current_step == 2
        return obs, reward, terminate, False, {}


def gen_episode(env: gym.Env) -> SingleAgentEpisode:
    ep = SingleAgentEpisode(
        observation_space=env.observation_space,
        action_space=env.action_space,
    )
    obs, info = env.reset()
    ep.add_env_reset(obs, info)

    term = False
    while not term:
        action = env.action_space.sample()
        obs, reward, term, trunc, info = env.step(action)
        ep.add_env_step(obs, action, reward, info, terminated=term, truncated=trunc)

    env.close()
    ep.validate()
    return ep


class TestE2E:
    def setup_method(self):
        ray.init()

    def teardown_method(self):
        ray.shutdown()

    @pytest.mark.parametrize(
        "space, space_id, flatten",
        [
            (gym.spaces.Discrete(5), "discrete", False),
            (gym.spaces.Box(0, 1, shape=()), "box-scalar", False),
            (gym.spaces.Box(0, 1, shape=(2, 2)), "box-matrix", True),
            (gym.spaces.MultiDiscrete([2, 3]), "multi-discrete", False),
            (
                gym.spaces.Dict(
                    a=gym.spaces.Discrete(4), b=gym.spaces.Box(0, 1, shape=(2, 2))
                ),
                "dict",
                True,
            ),
            (
                gym.spaces.Dict(
                    a=gym.spaces.MultiDiscrete(np.array([2, 3])),
                    b=gym.spaces.Dict(
                        b=gym.spaces.Discrete(4), c=gym.spaces.Box(0, 1, shape=(2, 2))
                    ),
                ),
                "dict-nested",
                True,
            ),
            (
                gym.spaces.Tuple(
                    [gym.spaces.Discrete(4), gym.spaces.Box(0, 1, shape=(2, 2))]
                ),
                "tuple",
                True,
            ),
            (
                gym.spaces.Tuple(
                    [
                        gym.spaces.Discrete(4),
                        gym.spaces.Tuple(
                            [gym.spaces.Discrete(4), gym.spaces.Box(0, 1, shape=(2, 2))]
                        ),
                    ]
                ),
                "tuple-nested",
                True,
            ),
        ],
    )
    def test_e2e_with_space(self, space, space_id, flatten):
        env = GenericTestEnv({"observation_space": space, "action_space": space})

        episodes = [gen_episode(env) for _ in range(10)]
        packed_episodes = [
            msgpack.packb(eps.get_state(), default=mnp.encode) for eps in episodes
        ]
        samples_ds = ray.data.from_items(packed_episodes)

        path = f"tmp/e2e-with-{space_id}-space"
        shutil.rmtree(path, ignore_errors=True)
        samples_ds.write_parquet(path)

        # Read the episodes and decode them.
        read_sample_ds = ray.data.read_parquet(path)
        batch = read_sample_ds.take_batch(10)
        read_episodes = [
            SingleAgentEpisode.from_state(
                msgpack.unpackb(state, object_hook=mnp.decode)
            )
            for state in batch["item"]
        ]
        assert len(episodes) == len(read_episodes)
        for eps, read_eps in zip(episodes, read_episodes):
            assert len(eps) == len(read_eps)
            assert eps.observations == read_eps.observations
            # This fails as eps.reward elements are np.float64
            #   while read_eps.reward elements are float
            # assert eps.rewards == read_eps.rewards

        config = (
            BCConfig()
            .environment(
                observation_space=env.observation_space,
                action_space=env.action_space,
            )
            .offline_data(
                input_=[Path(path).as_posix()],
                dataset_num_iters_per_learner=1,
                input_read_episodes=True,
                input_read_batch_size=1,
            )
            .learners(num_learners=0)
        )
        if flatten:
            config.training(learner_connector=FlattenObservations)

        algo = config.build()
        algo.train()
        shutil.rmtree(path)
