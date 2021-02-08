# Code in this file is adapted from:
# https://github.com/openai/evolution-strategies-starter.
from ray.rllib.agents.evolutionary.es.es_torch_policy import after_init, \
 before_init, make_model_and_action_dist
from ray.rllib.policy.policy_template import build_policy_class
from ray.rllib.agents.evolutionary.ars.ars import DEFAULT_CONFIG

ARSTorchPolicy = build_policy_class(
    name="ARSTorchPolicy",
    framework="torch",
    loss_fn=None,
    get_default_config=lambda: DEFAULT_CONFIG,
    before_init=before_init,
    after_init=after_init,
    make_model_and_action_dist=make_model_and_action_dist)
