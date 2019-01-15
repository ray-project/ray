from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ppo.vtrace_surrogate_policy_graph \
    import VTraceSurrogatePolicyGraph
from ray.rllib.agents.agent import with_base_config
from ray.rllib.agents import impala

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(impala.DEFAULT_CONFIG, {
    # Whether to use V-trace advantages. If false, the normal GAE advantages
    # for PPO will be used.
    "vtrace": True,
    # PPO surrogate loss options
    "clip_param": 0.4,
    "kl_coeff": 0.2,
    "kl_target": 0.01,
})
# __sphinx_doc_end__
# yapf: enable


class APPOAgent(impala.ImpalaAgent):
    """PPO surrogate loss with IMPALA-architecture."""

    _agent_name = "APPO"
    _default_config = DEFAULT_CONFIG
    _policy_graph = VTraceSurrogatePolicyGraph
