import logging
import time
from gym import spaces

import ray
from ray.rllib.algorithms.bandit.bandit_torch_model import (
    DiscreteLinearModelThompsonSampling,
    DiscreteLinearModelUCB,
    DiscreteLinearModel,
    ParametricLinearModelThompsonSampling,
    ParametricLinearModelUCB,
)
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.modelv2 import restore_original_dimensions
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.util.debug import log_once
from ray.rllib.policy.torch_policy_v2 import TorchPolicyV2

logger = logging.getLogger(__name__)


class BanditTorchPolicy(TorchPolicyV2):
    def __init__(self, observation_space, action_space, config):
        config = dict(ray.rllib.algorithms.bandit.bandit.DEFAULT_CONFIG, **config)

        TorchPolicyV2.__init__(
            self,
            observation_space,
            action_space,
            config,
            max_seq_len=config["model"]["max_seq_len"],
        )
        self.regrets = []

    @override(TorchPolicyV2)
    def make_model_and_action_dist(self):
        dist_class, logit_dim = ModelCatalog.get_action_dist(
            self.action_space, self.config["model"], framework="torch"
        )
        model_cls = DiscreteLinearModel

        if hasattr(self.observation_space, "original_space"):
            original_space = self.observation_space.original_space
        else:
            original_space = self.observation_space

        exploration_config = self.config.get("exploration_config")
        # Model is dependent on exploration strategy because of its implicitness

        # TODO: Have a separate model catalogue for bandits
        if exploration_config:
            if exploration_config["type"] == "ThompsonSampling":
                if isinstance(original_space, spaces.Dict):
                    assert (
                        "item" in original_space.spaces
                    ), "Cannot find 'item' key in observation space"
                    model_cls = ParametricLinearModelThompsonSampling
                else:
                    model_cls = DiscreteLinearModelThompsonSampling
            elif exploration_config["type"] == "UpperConfidenceBound":
                if isinstance(original_space, spaces.Dict):
                    assert (
                        "item" in original_space.spaces
                    ), "Cannot find 'item' key in observation space"
                    model_cls = ParametricLinearModelUCB
                else:
                    model_cls = DiscreteLinearModelUCB

        model = model_cls(
            self.observation_space,
            self.action_space,
            logit_dim,
            self.config["model"],
            name="LinearModel",
        )
        return model, dist_class

    @override(TorchPolicyV2)
    def learn_on_batch(self, postprocessed_batch):
        train_batch = self._lazy_tensor_dict(postprocessed_batch)
        unflattened_obs = restore_original_dimensions(
            train_batch[SampleBatch.CUR_OBS], self.observation_space, self.framework
        )

        info = {}

        start = time.time()
        self.model.partial_fit(
            unflattened_obs,
            train_batch[SampleBatch.REWARDS],
            train_batch[SampleBatch.ACTIONS],
        )

        infos = postprocessed_batch["infos"]
        if "regret" in infos[0]:
            regret = sum(row["infos"]["regret"] for row in postprocessed_batch.rows())
            self.regrets.append(regret)
            info["cumulative_regret"] = sum(self.regrets)
        else:
            if log_once("no_regrets"):
                logger.warning(
                    "The env did not report `regret` values in "
                    "its `info` return, ignoring."
                )
        info["update_latency"] = time.time() - start
        return {LEARNER_STATS_KEY: info}
