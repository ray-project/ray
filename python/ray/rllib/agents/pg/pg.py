from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.pg_policy_graph import PGTFPolicy
from ray.rllib.optimizers import SyncSamplesOptimizer

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default
    "num_workers": 0,
    # Learning rate
    "lr": 0.0004,
    # Use PyTorch as backend
    "use_pytorch": False,
})
# __sphinx_doc_end__
# yapf: enable


def make_policy_optimizer(local_ev, remote_evs, config):
    optimizer_config = dict(config["optimizer"],
                            **{"train_batch_size": config["train_batch_size"]})
    return SyncSamplesOptimizer(local_ev, remote_evs, **optimizer_config)


def get_policy_class(config):
    if config["use_pytorch"]:
        from ray.rllib.agents.pg.torch_pg_policy_graph import PGTorchPolicy
        return PGTorchPolicy
    else:
        return PGTFPolicy


PGTrainer = build_trainer(
    "PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class,
    make_policy_optimizer=make_policy_optimizer)
