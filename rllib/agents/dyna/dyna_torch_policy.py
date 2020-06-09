from functools import partial
import logging

import ray
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.utils import try_import_torch
from ray.rllib.models.torch.torch_action_dist import \
    TorchMultiActionDistribution

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def make_model_and_dist(policy, obs_space, action_space, config):
    # Get the output distribution class for predicting rewards and next-obs.
    # distr_cls_next_obs = ModelCatalog.get_action_dist(
    #    obs_space, config, dist_type="deterministic")
    # distr_cls_rewards = ModelCatalog.get_action_dist(gym.spaces.Box(
    #     float("-inf"), float("inf"), ()
    # ), config, dist_type=)

    # Build one dynamics model if we are a Worker.
    # If we are the main MAML learner, build n (num_workers) dynamics Models
    # for being able to create checkpoints for the current state of training.
    policy.dynamics_model = ModelCatalog.get_model_v2(
        input_space=obs_space,
        output_space=obs_space,
        num_outputs=None,
        model_config=config["dynamics_model"],
        framework="torch",
        name="dynamics_model",
    )

    # Create the pi-model and register it with the Policy.
    policy.pi = ModelCatalog.get_model_v2(
        input_space=obs_space,
        output_space=action_space,
        model_config=config["model"],
        framework="torch",
        name="policy_model",
    )

    dist_cls = partial(TorchMultiActionDistribution)
    return policy.pi, dist_cls


def dyna_torch_loss(policy, model, dist_class, train_batch):
    # Get the predictions on the next state.
    # `predictions` will be a Tuple of
    predictions, _ = model.from_batch(train_batch)
    predicted_next_state_deltas, predicted_rewards = predictions
    labels = train_batch[SampleBatch.NEXT_OBS] - train_batch[SampleBatch.
                                                             CUR_OBS]
    loss = torch.mean(torch.pow(labels - predicted_next_state_deltas, 2.0))
    # TODO: (michael) what about rewards-loss?
    policy.dynamics_loss = loss

    return policy.dynamics_loss


def stats_fn(policy, train_batch):
    return {
        "dynamics_loss": policy.dynamics_loss,
    }


DYNATorchPolicy = build_torch_policy(
    name="DYNATorchPolicy",
    get_default_config=lambda: ray.rllib.agents.dyna.dyna.DEFAULT_CONFIG,
    loss_fn=dyna_torch_loss,
    stats_fn=stats_fn,
    make_model_and_action_dist=make_model_and_dist,
)
