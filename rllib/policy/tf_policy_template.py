from ray.rllib.policy.dynamic_tf_policy import DynamicTFPolicy
from ray.rllib.policy import eager_tf_policy
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.tf_policy import TFPolicy
from ray.rllib.utils import add_mixins, force_list
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import (
    deprecation_warning,
    Deprecated,
    DEPRECATED_VALUE,
)
from ray.rllib.utils.framework import try_import_tf
from ray.rllib.utils.metrics.learner_info import LEARNER_STATS_KEY

tf1, tf, tfv = try_import_tf()


@Deprecated(
    new="sub-class directly from `ray.rllib.policy.[eager|"
        "dynamic]_tf_policy_v2::[Eager|Dynamic]TFPolicyV2` "
        "and override needed methods",
    error=True,
)
def build_tf_policy(*args, **kwargs):
    pass
