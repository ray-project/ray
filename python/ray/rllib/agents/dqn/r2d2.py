from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.dqn.apex import ApexAgent, APEX_DEFAULT_CONFIG as APEX_CONFIG
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override

# yapf: disable
# __sphinx_doc_begin__
R2D2_DEFAULT_CONFIG = merge_dicts(
    APEX_CONFIG,  # see also the options in apex.py, which are also supported
    {
        "n_step": 5,
        # for these, we can use the same defaults as APEX,
        # but I have these here for testing
        "num_gpus": 0, # should really be 1
        "num_workers": 2, # should really be 32

        "gamma": 0.997,
        "lr": 1e-4,
        "adam_epsilon": 1e-3,
        "train_batch_size": 64,
        "target_network_update_freq": 2500,

        "model": merge_dicts(
            APEX_CONFIG["model"], {
                "use_lstm": True,
                "max_seq_len": 80,
            }),
    },
)
# __sphinx_doc_end__
# yapf: enable


class R2D2Agent(ApexAgent):
    """DQN variant that uses the Ape-X distributed policy optimizer.

    By default, this is configured for a large single node (32 cores). For
    running in a large cluster, increase the `num_workers` config var.
    """

    _agent_name = "R2D2"
    _default_config = R2D2_DEFAULT_CONFIG
