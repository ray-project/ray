import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.contrib.bandits.agents.policy import BanditPolicy

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
UCB_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    "framework": "torch",  # Only PyTorch supported so far.

    # Do online learning one step at a time.
    "rollout_fragment_length": 1,
    "train_batch_size": 1,

    # Bandits cant afford to do one timestep per iteration as it is extremely
    # slow because of metrics collection overhead. This setting means that the
    # agent will be trained for 100 times in one iteration of Rllib
    "timesteps_per_iteration": 100,

    "exploration_config": {
        "type": "ray.rllib.contrib.bandits.exploration.UCB"
    }
})
# __sphinx_doc_end__
# yapf: enable

LinUCBTrainer = build_trainer(
    name="LinUCB", default_config=UCB_CONFIG, default_policy=BanditPolicy)
