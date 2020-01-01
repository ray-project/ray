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
    # Use PyTorch as framework?
    "use_pytorch": False
})
# __sphinx_doc_end__
# yapf: enable


def post_process_advantages(policy, sample_batch, other_agent_batches=None,
                            episode=None):
    """This adds the "advantages" column to the sample train_batch."""
    return compute_advantages(sample_batch, 0.0, policy.config["gamma"],
                              use_gae=False)


def get_policy_class(config):
    if config["use_pytorch"]:
        from ray.rllib.agents.pg.pg_torch_policy import PGTorchPolicy
        return PGTorchPolicy
    else:
        return PGTFPolicy


PGTrainer = build_trainer(
    name="PG",
    default_config=DEFAULT_CONFIG,
    default_policy=PGTFPolicy,
    get_policy_class=get_policy_class)
