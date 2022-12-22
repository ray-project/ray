"""An example of customizing PPO to leverage a centralized critic.

Here the model and policy are hard-coded to implement a centralized critic
for TwoStepGame, but you can adapt this for your own use cases.

Compared to simply running `rllib/examples/two_step_game.py --run=PPO`,
this centralized critic version reaches vf_explained_variance=1.0 more stably
since it takes into account the opponent actions as well as the policy's.
Note that this is also using two independent policies instead of weight-sharing
with one.

See also: centralized_critic_2.py for a simpler approach that instead
modifies the environment.
"""

import argparse
import numpy as np
from gym.spaces import Discrete
import os

import ray
from ray import air, tune
from ray.rllib.algorithms.ppo.ppo import PPO, PPOConfig
from ray.rllib.algorithms.ppo.ppo_tf_policy import (
    PPOTF1Policy,
    PPOTF2Policy,
)
from ray.rllib.algorithms.ppo.ppo_torch_policy import PPOTorchPolicy
from ray.rllib.evaluation.postprocessing import compute_advantages, Postprocessing
from ray.rllib.examples.env.two_step_game import TwoStepGame
from ray.rllib.examples.models.centralized_critic_models import (
    CentralizedCriticModel,
    TorchCentralizedCriticModel,
)
from ray.rllib.models import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import override
from ray.rllib.utils.framework import try_import_tf, try_import_torch
from ray.rllib.utils.numpy import convert_to_numpy
from ray.rllib.utils.test_utils import check_learning_achieved
from ray.rllib.utils.tf_utils import explained_variance, make_tf_callable
from ray.rllib.utils.torch_utils import convert_to_torch_tensor

tf1, tf, tfv = try_import_tf()
torch, nn = try_import_torch()

OPPONENT_OBS = "opponent_obs"
OPPONENT_ACTION = "opponent_action"

parser = argparse.ArgumentParser()
parser.add_argument(
    "--framework",
    choices=["tf", "tf2", "torch"],
    default="tf",
    help="The DL framework specifier.",
)
parser.add_argument(
    "--as-test",
    action="store_true",
    help="Whether this script should be run as a test: --stop-reward must "
    "be achieved within --stop-timesteps AND --stop-iters.",
)
parser.add_argument(
    "--stop-iters", type=int, default=100, help="Number of iterations to train."
)
parser.add_argument(
    "--stop-timesteps", type=int, default=100000, help="Number of timesteps to train."
)
parser.add_argument(
    "--stop-reward", type=float, default=7.99, help="Reward at which we stop training."
)


class CentralizedValueMixin:
    """Add method to evaluate the central value function from the model."""

    def __init__(self):
        if self.config["framework"] != "torch":
            self.compute_central_vf = make_tf_callable(self.get_session())(
                self.model.central_value_function
            )
        else:
            self.compute_central_vf = self.model.central_value_function


# Grabs the opponent obs/act and includes it in the experience train_batch,
# and computes GAE using the central vf predictions.
def centralized_critic_postprocessing(
    policy, sample_batch, other_agent_batches=None, episode=None
):
    pytorch = policy.config["framework"] == "torch"
    if (pytorch and hasattr(policy, "compute_central_vf")) or (
        not pytorch and policy.loss_initialized()
    ):
        assert other_agent_batches is not None
        [(_, opponent_batch)] = list(other_agent_batches.values())

        # also record the opponent obs and actions in the trajectory
        sample_batch[OPPONENT_OBS] = opponent_batch[SampleBatch.CUR_OBS]
        sample_batch[OPPONENT_ACTION] = opponent_batch[SampleBatch.ACTIONS]

        # overwrite default VF prediction with the central VF
        if args.framework == "torch":
            sample_batch[SampleBatch.VF_PREDS] = (
                policy.compute_central_vf(
                    convert_to_torch_tensor(
                        sample_batch[SampleBatch.CUR_OBS], policy.device
                    ),
                    convert_to_torch_tensor(sample_batch[OPPONENT_OBS], policy.device),
                    convert_to_torch_tensor(
                        sample_batch[OPPONENT_ACTION], policy.device
                    ),
                )
                .cpu()
                .detach()
                .numpy()
            )
        else:
            sample_batch[SampleBatch.VF_PREDS] = convert_to_numpy(
                policy.compute_central_vf(
                    sample_batch[SampleBatch.CUR_OBS],
                    sample_batch[OPPONENT_OBS],
                    sample_batch[OPPONENT_ACTION],
                )
            )
    else:
        # Policy hasn't been initialized yet, use zeros.
        sample_batch[OPPONENT_OBS] = np.zeros_like(sample_batch[SampleBatch.CUR_OBS])
        sample_batch[OPPONENT_ACTION] = np.zeros_like(sample_batch[SampleBatch.ACTIONS])
        sample_batch[SampleBatch.VF_PREDS] = np.zeros_like(
            sample_batch[SampleBatch.REWARDS], dtype=np.float32
        )

    completed = sample_batch["dones"][-1]
    if completed:
        last_r = 0.0
    else:
        last_r = sample_batch[SampleBatch.VF_PREDS][-1]

    train_batch = compute_advantages(
        sample_batch,
        last_r,
        policy.config["gamma"],
        policy.config["lambda"],
        use_gae=policy.config["use_gae"],
    )
    return train_batch


# Copied from PPO but optimizing the central value function.
def loss_with_central_critic(policy, base_policy, model, dist_class, train_batch):
    # Save original value function.
    vf_saved = model.value_function

    # Calculate loss with a custom value function.
    model.value_function = lambda: policy.model.central_value_function(
        train_batch[SampleBatch.CUR_OBS],
        train_batch[OPPONENT_OBS],
        train_batch[OPPONENT_ACTION],
    )
    policy._central_value_out = model.value_function()
    loss = base_policy.loss(model, dist_class, train_batch)

    # Restore original value function.
    model.value_function = vf_saved

    return loss


def central_vf_stats(policy, train_batch):
    # Report the explained variance of the central value function.
    return {
        "vf_explained_var": explained_variance(
            train_batch[Postprocessing.VALUE_TARGETS], policy._central_value_out
        )
    }


def get_ccppo_policy(base):
    class CCPPOTFPolicy(CentralizedValueMixin, base):
        def __init__(self, observation_space, action_space, config):
            base.__init__(self, observation_space, action_space, config)
            CentralizedValueMixin.__init__(self)

        @override(base)
        def loss(self, model, dist_class, train_batch):
            # Use super() to get to the base PPO policy.
            # This special loss function utilizes a shared
            # value function defined on self, and the loss function
            # defined on PPO policies.
            return loss_with_central_critic(
                self, super(), model, dist_class, train_batch
            )

        @override(base)
        def postprocess_trajectory(
            self, sample_batch, other_agent_batches=None, episode=None
        ):
            return centralized_critic_postprocessing(
                self, sample_batch, other_agent_batches, episode
            )

        @override(base)
        def stats_fn(self, train_batch: SampleBatch):
            stats = super().stats_fn(train_batch)
            stats.update(central_vf_stats(self, train_batch))
            return stats

    return CCPPOTFPolicy


CCPPOStaticGraphTFPolicy = get_ccppo_policy(PPOTF1Policy)
CCPPOEagerTFPolicy = get_ccppo_policy(PPOTF2Policy)


class CCPPOTorchPolicy(CentralizedValueMixin, PPOTorchPolicy):
    def __init__(self, observation_space, action_space, config):
        PPOTorchPolicy.__init__(self, observation_space, action_space, config)
        CentralizedValueMixin.__init__(self)

    @override(PPOTorchPolicy)
    def loss(self, model, dist_class, train_batch):
        return loss_with_central_critic(self, super(), model, dist_class, train_batch)

    @override(PPOTorchPolicy)
    def postprocess_trajectory(
        self, sample_batch, other_agent_batches=None, episode=None
    ):
        return centralized_critic_postprocessing(
            self, sample_batch, other_agent_batches, episode
        )


class CentralizedCritic(PPO):
    @classmethod
    @override(PPO)
    def get_default_policy_class(cls, config):
        if config["framework"] == "torch":
            return CCPPOTorchPolicy
        elif config["framework"] == "tf":
            return CCPPOStaticGraphTFPolicy
        else:
            return CCPPOEagerTFPolicy


if __name__ == "__main__":
    ray.init()
    args = parser.parse_args()

    ModelCatalog.register_custom_model(
        "cc_model",
        TorchCentralizedCriticModel
        if args.framework == "torch"
        else CentralizedCriticModel,
    )

    config = (
        PPOConfig()
        .environment(TwoStepGame)
        .framework(args.framework)
        .rollouts(batch_mode="complete_episodes", num_rollout_workers=0)
        .training(model={"custom_model": "cc_model"})
        .multi_agent(
            policies={
                "pol1": (
                    None,
                    Discrete(6),
                    TwoStepGame.action_space,
                    {
                        "framework": args.framework,
                    },
                ),
                "pol2": (
                    None,
                    Discrete(6),
                    TwoStepGame.action_space,
                    {
                        "framework": args.framework,
                    },
                ),
            },
            policy_mapping_fn=lambda agent_id, **kwargs: "pol1"
            if agent_id == 0
            else "pol2",
        )
        # Use GPUs iff `RLLIB_NUM_GPUS` env var set to > 0.
        .resources(num_gpus=int(os.environ.get("RLLIB_NUM_GPUS", "0")))
    )

    stop = {
        "training_iteration": args.stop_iters,
        "timesteps_total": args.stop_timesteps,
        "episode_reward_mean": args.stop_reward,
    }

    tuner = tune.Tuner(
        CentralizedCritic,
        param_space=config.to_dict(),
        run_config=air.RunConfig(stop=stop, verbose=1),
    )
    results = tuner.fit()

    if args.as_test:
        check_learning_achieved(results, args.stop_reward)
