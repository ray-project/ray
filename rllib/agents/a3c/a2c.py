from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.a3c.a3c import DEFAULT_CONFIG as A3C_CONFIG, \
    validate_config, get_policy_class
from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.utils import merge_dicts

A2C_DEFAULT_CONFIG = merge_dicts(
    A3C_CONFIG,
    {
        "sample_batch_size": 20,
        "min_iter_time_s": 10,
        "sample_async": False,
    },
)

A2CTrainer = build_trainer(
    name="A2C",
    default_config=A2C_DEFAULT_CONFIG,
    default_policy=A3CTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config)
