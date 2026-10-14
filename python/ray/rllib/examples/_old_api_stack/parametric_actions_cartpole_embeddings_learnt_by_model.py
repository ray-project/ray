# @OldAPIStack
"""Example of handling variable length or parametric action spaces.

This is a toy example of the action-embedding based approach for handling large
discrete action spaces (potentially infinite in size), similar to this:

    https://neuro.cs.ut.ee/the-use-of-embeddings-in-openai-five/

This currently works with RLlib's policy gradient style algorithms
(e.g., PG, PPO, IMPALA, A2C) and also DQN.

Note that since the model outputs now include "-inf" tf.float32.min
values, not all algorithm options are supported at the moment. For example,
algorithms might crash if they don't properly ignore the -inf action scores.
Working configurations are given below.
"""

import argparse
import os

import ray
from ray import tune
from ray.rllib.examples._old_api_stack.models.parametric_actions_model import (
    ParametricActionsModelThatLearnsEmbeddings,
)
from ray.rllib.examples.envs.classes.parametric_actions_cartpole import (
    ParametricActionsCartPoleNoEmbeddings,
)
from ray.rllib.models import ModelCatalog
from ray.rllib.utils.metrics import (
    ENV_RUNNER_RESULTS,
    EPISODE_RETURN_MEAN,
    NUM_ENV_STEPS_SAMPLED_LIFETIME,
)
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.tune.registry import register_env
from ray.tune.result import TRAINING_ITERATION

parser = argparse.ArgumentParser()
parser.add_argument("--run", type=str, default="PPO")
parser.add_argument(
    "--framework",
    choices=["tf", "tf2"],
    default="tf",
    help="The DL framework specifier (Torch not supported "
    "due to the lack of a model).",
)
parser.add_argument("--as-test", action="store_true")
parser.add_argument("--stop-iters", type=int, default=200)
parser.add_argument("--stop-reward", type=float, default=150.0)
parser.add_argument("--stop-timesteps", type=int, default=100000)

if __name__ == "__main__":
    args = parser.parse_args()
    ray.init()

    register_env("pa_cartpole", lambda _: ParametricActionsCartPoleNoEmbeddings(10))

    ModelCatalog.register_custom_model(
        "pa_model", ParametricActionsModelThatLearnsEmbeddings
    )

    if args.run == "DQN":
        cfg = {
            # TODO(ekl) we need to set these to prevent the masked values
            # from being further processed in DistributionalQModel, which
            # would mess up the masking. It is possible to support these if we
            # defined a custom DistributionalQModel that is aware of masking.
            "hiddens": [],
            "dueling": False,
            "enable_rl_module_and_learner": False,
            "enable_env_runner_and_connector_v2": False,
        }
    else:
        cfg = {}

    config = dict(
        {
            "env": "pa_cartpole",
            "model": {
                "custom_model": "pa_model",
            },
            # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
            "num_gpus": int(os.environ.get("RLLIB_NUM_GPUS", "0")),
            "num_env_runners": 0,
            "framework": args.framework,
            "action_mask_key": "valid_avail_actions_mask",
        },
        **cfg,
    )

    stop = {
        TRAINING_ITERATION: args.stop_iters,
        NUM_ENV_STEPS_SAMPLED_LIFETIME: args.stop_timesteps,
        f"{ENV_RUNNER_RESULTS}/{EPISODE_RETURN_MEAN}": args.stop_reward,
    }

    results = tune.Tuner(
        args.run,
        run_config=tune.RunConfig(stop=stop, verbose=2),
        param_space=config,
    ).fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)

    ray.shutdown()
