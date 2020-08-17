import logging

from ray.rllib.agents.trainer import with_common_config
from ray.rllib.agents.trainer_template import build_trainer

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Default Trainer setting overrides.
    "num_workers": 1,
    "num_envs_per_worker": 1,

    # The size of an entire epoch (for supervised learning the dynamics).
    # The train-batch will be split into training and validation sets according
    # to `training_set_ratio`, then n epochs (with minibatch
    # size=`sgd_minibatch_size`) will be trained until the sliding average
    # of the validation performance decreases.
    "train_batch_size": 10000,
    "sgd_minibatch_size": 500,
    "rollout_fragment_length": 200,
    # Learning rate for the dynamics optimizer.
    "lr": 0.0003,

    # Fraction of the entire data that should be used for training the dynamics
    # model. The validation fraction is 1.0 - `training_set_ratio`. Training of
    # a dynamics model over n some epochs (1 epoch = entire training set) stops
    # when the validation set's performance starts to decrease.
    "train_set_ratio": 0.8,

    # The exploration strategy to apply on top of the (acting) policy.
    # TODO: (sven) Use random for testing purposes for now.
    "exploration_config": {"type": "Random"},

    # Whether to predict the action that lead from obs(t) to obs(t+1), instead
    # of predicting obs(t+1).
    "predict_action": False,

    # Whether the dynamics model should predict the reward, given obs(t)+a(t).
    # NOTE: Only supported if `predict_action`=False.
    "predict_reward": False,

    # Whether to use the same network for predicting rewards than for
    # predicting the next observation.
    "reward_share_layers": True,

    # TODO: (sven) figure out API to query the latent space vector given
    #  some observation (not needed for MBMPO).
    "learn_latent_space": False,

    # Whether to predict `obs(t+1) - obs(t)` instead of `obs(t+1)` directly.
    # NOTE: This only works for 1D Box observation spaces, e.g. Box(5,) and
    # if `predict_action`=False.
    "predict_obs_delta": True,
    # TODO: loss function types: neg_log_llh, etc..?
    "loss_function": "l2",

    # Config for the dynamics learning model architecture.
    "dynamics_model": {
        "fcnet_hiddens": [512, 512],
        "fcnet_activation": "relu",
    },

    # TODO: (sven) allow for having a default model config over many
    #  sub-models: e.g. "model": {"ModelA": {[default_config]},
    #  "ModelB": [default_config]}
})
# __sphinx_doc_end__
# yapf: enable


def validate_config(config):
    if config["train_set_ratio"] <= 0.0 or \
            config["train_set_ratio"] >= 1.0:
        raise ValueError("`train_set_ratio` must be within (0.0, 1.0)!")
    if config["predict_action"] or config["predict_reward"]:
        raise ValueError(
            "`predict_action`=True or `predict_reward`=True not supported "
            "yet!")
    if config["learn_latent_space"]:
        raise ValueError("`learn_latent_space` not supported yet!")
    if config["loss_function"] != "l2":
        raise ValueError("`loss_function` other than 'l2' not supported yet!")


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
    validate_config=validate_config,
)
