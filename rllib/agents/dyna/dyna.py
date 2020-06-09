from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    "num_workers": 10,
    "buffer_size": 1000,
    "train_batch_size": 512,
    "rollout_fragment_length": 10,
    "exploration_config": {"type": "Random"},
    # Ratio of train set size over validation set size for dynamics learning.
    # Will be used to decide, which collected batches will be stored in
    # which replay buffer (2 per worker: train and validation). Training of
    # a dynamics model over some epochs (over the entire training set) stops
    # when the validation performance starts to decrease again.
    "train_vs_validation_ratio": 3.0,
    "dynamics_model": {
        "fcnet_hiddens": [512, 512],
    },

    # TODO: (sven) allow for having a default model config over many
    #  sub-models: e.g. "model": {"ModelA": {[default_config]},
    #  "ModelB": [default_config]}
})
# __sphinx_doc_end__
# yapf: enable


def get_policy_class(config):
    if config["framework"] == "torch":
        from ray.rllib.agents.dyna.dyna_torch_policy import DYNATorchPolicy
        return DYNATorchPolicy
    else:
        raise ValueError("tf not supported yet!")


DYNATrainer = build_trainer(
    name="DYNA",
    default_policy=None,
    get_policy_class=get_policy_class,
    default_config=DEFAULT_CONFIG,
)
