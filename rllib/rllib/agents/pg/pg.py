from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ...agents.trainer import with_common_config
from ...agents.trainer_template import build_trainer
from ...agents.pg.pg_policy import PGTFPolicy

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


def get_policy_class(config):
    if config["use_pytorch"]:
        from ...agents.pg.torch_pg_policy import PGTorchPolicy
        return PGTorchPolicy
    else:
        return PGTFPolicy


PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class)
