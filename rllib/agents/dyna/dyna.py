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
    # Fraction of the entire data that should be used for training the dynamics
    # model. The validation fraction is 1.0 - `training_set_ratio`. Training of
    # a dynamics model over n some epochs (1 epoch = entire training set) stops
    # when the validation set's performance starts to decrease.
    "training_set_ratio": 0.8,
    # Whether the dynamics model shouldo predict the reward, given obs(t)+a(t).
    "predict_rewards": False,
    # Whether to predict `obs(t+1) - obs(t)` instead of `obs(t+1)` directly.
    # NOTE: This only works for 1D Box observation spaces, e.g. Box(5,).
    "predict_obs_delta": True,
    # TODO: loss function types: neg_log_llh, etc..?
    "loss_function": "l2",
    # Config for the dynamics learning model architecture.
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
