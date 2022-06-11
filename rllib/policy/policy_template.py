from typing import (
    TYPE_CHECKING,
)

from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.models.jax.jax_modelv2 import JAXModelV2
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.torch_policy import TorchPolicy
from ray.rllib.utils import add_mixins, NullContextManager
from ray.rllib.utils.annotations import Deprecated, override
from ray.rllib.utils.framework import try_import_torch, try_import_jax
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY
from ray.rllib.utils.numpy import convert_to_numpy

if TYPE_CHECKING:
    from ray.rllib.evaluation.episode import Episode  # noqa

jax, _ = try_import_jax()
torch, _ = try_import_torch()


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.torch_policy_v2::TorchPolicyV2` "
        "and override needed methods",
    error=True,
)
def build_policy_class(*args, **kwargs):
    pass
