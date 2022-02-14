import copy

import gym
import numpy as np
from gym import spaces

DEFAULT_RECO_CONFIG = {
    "num_users": 1,
    "num_items": 100,
    "feature_dim": 16,
    "slate_size": 1,
    "num_candidates": 25,
    "seed": 1,
}


class ParametricItemRecoEnv(gym.Env):
    """A recommendation environment which generates items with visible features
     randomly (parametric actions).
    The environment can be configured to be multi-user, i.e. different models
    will be learned independently for each user.
    To enable slate recommendation, the `slate_size` config parameter can be
    set as > 1.
    """

    def __init__(self, config=None):
        self.config = copy.copy(DEFAULT_RECO_CONFIG)
        if config is not None and type(config) == dict:
            self.config.update(config)

        self.num_users = self.config["num_users"]
        self.num_items = self.config["num_items"]
        self.feature_dim = self.config["feature_dim"]
        self.slate_size = self.config["slate_size"]
        self.num_candidates = self.config["num_candidates"]
        self.seed = self.config["seed"]

        self.slate_q = config.get("slate_q", False)
        self.user_key = config.get("user_key", "user")
        self.item_key = config.get("item_key", "item" if not self.slate_q else "doc")

        assert (
            self.num_candidates <= self.num_items
        ), "Size of candidate pool should be less than total no. of items"
        assert (
            self.slate_size < self.num_candidates
        ), "Slate size should be less than no. of candidate items"

        self.action_space = self._def_action_space()
        self.observation_space = self._def_observation_space()

        self.current_user_id = 0
        self.item_pool = None
        self.item_pool_dict = None  # For SlateQ only.
        self.item_pool_ids = None
        self.total_regret = 0

        self._init_embeddings()

    def _init_embeddings(self):
        self.item_embeddings = self._gen_normalized_embeddings(
            self.num_items, self.feature_dim
        )

        # These are latent user features that will be hidden from the learning
        # agent. They will be used for reward generation only
        self.user_embeddings = self._gen_normalized_embeddings(
            self.num_users, self.feature_dim
        )

    def _sample_user(self):
        self.current_user_id = np.random.randint(0, self.num_users)

    def _gen_item_pool(self):
        # Randomly generate a candidate list of items by sampling without
        # replacement
        self.item_pool_ids = np.random.choice(
            np.arange(self.num_items), self.num_candidates, replace=False
        )
        self.item_pool = self.item_embeddings[self.item_pool_ids].astype(np.float32)
        if self.slate_q:
            self.item_pool_dict = {str(i): self.item_embeddings[id].astype(np.float32) for i, id in enumerate(self.item_pool_ids)}

    @staticmethod
    def _gen_normalized_embeddings(size, dim):
        embeddings = np.random.rand(size, dim)
        embeddings /= np.linalg.norm(embeddings, axis=1, keepdims=True)
        return embeddings.astype(np.float32)

    def _def_action_space(self):
        if self.slate_size == 1 and not self.slate_q:
            return spaces.Discrete(self.num_candidates)
        else:
            return spaces.MultiDiscrete([self.num_candidates] * self.slate_size)

    def _def_observation_space(self):
        # Embeddings for each item in the candidate pool
        if self.slate_q:
            item_obs_space = spaces.Dict({
                str(i): spaces.Box(
                    low=-np.inf, high=np.inf, shape=(self.feature_dim,)
                ) for i in range(self.num_candidates)
            })
        else:
            item_obs_space = spaces.Box(
                low=-np.inf, high=np.inf, shape=(self.num_candidates, self.feature_dim)
            )

        # Can be useful for collaborative filtering based agents
        item_ids_obs_space = spaces.MultiDiscrete(
            [self.num_items] * self.num_candidates
        )

        # Can be either binary (clicks) or continuous feedback (watch time)
        if self.slate_q:
            resp_space = spaces.Tuple([spaces.Dict({
                "click": spaces.Discrete(2)
            }) for _ in range(self.slate_size)])
        else:
            resp_space = spaces.Box(low=-1, high=1, shape=(self.slate_size,))

        if self.num_users == 1 and not self.slate_q:
            return spaces.Dict(
                {
                    self.item_key: item_obs_space,
                    "item_id": item_ids_obs_space,
                    "response": resp_space,
                }
            )
        else:
            if self.slate_q:
                user_obs_space = spaces.Box(
                    low=-np.inf, high=np.inf, shape=(self.feature_dim,)
                )
            else:
                user_obs_space = spaces.Discrete(self.num_users)
            return spaces.Dict(
                {
                    self.user_key: user_obs_space,
                    self.item_key: item_obs_space,
                    "item_id": item_ids_obs_space,
                    "response": resp_space,
                }
            )

    def step(self, action):
        # Action can be a single action or a slate depending on slate size
        assert self.action_space.contains(
            action
        ), "Action cannot be recognized. Please check the type and bounds."

        if self.slate_size == 1:
            scores = self.item_pool.dot(self.user_embeddings[self.current_user_id])
            reward = scores[action[0] if self.slate_q else action]
            regret = np.max(scores) - reward
            self.total_regret += regret

            info = {"regret": regret}

            self.current_user_id = np.random.randint(0, self.num_users)
            self._gen_item_pool()

            obs = {
                self.item_key: self.item_pool_dict if self.slate_q else self.item_pool,
                "item_id": self.item_pool_ids,
                "response": [reward] if not self.slate_q else tuple([{"click": 1}])
            }
            if self.num_users > 1 or self.slate_q:
                obs[self.user_key] = self.current_user_id if not self.slate_q else self.user_embeddings[self.current_user_id]
            return obs, reward, True, info
        else:
            # TODO(saurabh3949):Handle slate recommendation using a click model
            return None

    def reset(self):
        self._sample_user()
        self._gen_item_pool()
        obs = {
            self.item_key: self.item_pool_dict if self.slate_q else self.item_pool,
            "item_id": self.item_pool_ids,
            "response": [0] * self.slate_size if not self.slate_q else tuple([{"click": 0} for _ in range(self.slate_size)])
        }
        if self.num_users > 1 or self.slate_q:
            obs[self.user_key] = self.current_user_id if not self.slate_q else self.user_embeddings[self.current_user_id]
        return obs

    def render(self, mode="human"):
        raise NotImplementedError
