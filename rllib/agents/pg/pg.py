from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.agents.pg.pg_tf_policy import PGTFPolicy

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # No remote workers by default.
    "num_workers": 0,
    # Learning rate.
    "lr": 0.0004,
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
        return PGTorchPolicy
    else:
        return PGTFPolicy


PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class)
