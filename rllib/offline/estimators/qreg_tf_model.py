from ray.rllib.models.utils import get_initializer
from ray.rllib.policy import Policy
from ray.rllib.policy.sample_batch import SampleBatch
from typing import Dict, List, Union

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.tf.tf_modelv2 import TFModelV2
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.typing import TensorType

tf1, tf, tfv = try_import_tf()


class QRegTFModel:
    def __init__(self, policy: Policy, gamma: float, config: Dict) -> None:
        self.policy = policy
        self.gamma = gamma
        self.observation_space = policy.observation_space
        self.action_space = policy.action_space

        self.q_model: TFModelV2 = ModelCatalog.get_model_v2(
            self.observation_space,
            self.action_space,
            self.action_space.n,
            config.get("model", {}),
            framework=policy.config["framework"],
            name="TFQModel",
        )
        self.n_iters = config.get("n_iters", 80)
        self.lr = config.get("lr", 1e-3)
        self.delta = config.get("delta", 1e-4)
        self.initializer = get_initializer("xavier_uniform", framework="tf")

    def reset(self) -> None:
        raise NotImplementedError

    def train_q(self, batch: SampleBatch) -> TensorType:
        raise NotImplementedError

    def estimate_q(
        self,
        obs: Union[TensorType, List[TensorType]],
        actions: Union[TensorType, List[TensorType]] = None,
    ) -> TensorType:
        raise NotImplementedError

    def estimate_v(
        self,
        obs: Union[TensorType, List[TensorType]],
        action_probs: Union[TensorType, List[TensorType]],
    ) -> TensorType:
        raise NotImplementedError
