# Code in this file is adapted from:
# https://github.com/openai/evolution-strategies-starter.

import ray
from ray.rllib.agents.es.es_torch_policy import after_init, before_init, \
    make_model_and_action_dist
from ray.rllib.policy.torch_policy_template import build_torch_policy

ARSTorchPolicy = build_torch_policy(
    name="ARSTorchPolicy",
    loss_fn=None,
    get_default_config=lambda: ray.rllib.agents.ars.ars.DEFAULT_CONFIG,
    before_init=before_init,
    after_init=after_init,
    make_model_and_action_dist=make_model_and_action_dist)
