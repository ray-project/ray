from functools import partial
import logging

import ray
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.models.catalog import ModelCatalog
#from ray.rllib.agents.ppo.ppo_tf_policy import setup_config
#from ray.rllib.evaluation.postprocessing import Postprocessing
from ray.rllib.policy.sample_batch import SampleBatch
#from ray.rllib.policy.torch_policy import EntropyCoeffSchedule, \
#    LearningRateSchedule
from ray.rllib.policy.torch_policy_template import build_torch_policy
#from ray.rllib.utils.explained_variance import explained_variance
#from ray.rllib.utils.torch_ops import sequence_mask
from ray.rllib.utils import try_import_torch
from ray.rllib.models.torch.torch_action_dist import \
    TorchMultiActionDistribution

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def make_model_and_dist(policy, obs_space, action_space, config):
    # Build one dynamics model if we are a Worker.
    # If we are the main MAML learner, build n (num_workers) dynamics Models
    # for being able to create checkpoints for the current state of training.
    policy.dynamics_model = ModelCatalog.get_model_v2()

    # Create the pi-model and register it with the Policy.
    policy.pi = ModelCatalog.get_model_v2()

    dist_cls = partial(TorchMultiActionDistribution)
    return policy.pi, dist_cls


def dynamics_model_loss(policy, model, dist_class, train_batch):
    # Get the predictions on the next state.
    # `predictions` will be a Tuple of
    predictions, _ = model.from_batch(train_batch)
    predicted_next_state_deltas, predicted_rewards = predictions
    labels = train_batch[SampleBatch.NEXT_OBS] - \
             train_batch[SampleBatch.CUR_OBS]
    loss = torch.mean(torch.pow(
        labels - predicted_next_state_deltas, 2.0))
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
    loss_fn=dynamics_model_loss,
    stats_fn=stats_fn,
    #extra_action_out_fn=vf_preds_fetches,
    #postprocess_fn=postprocess_ppo_gae,
    #extra_grad_process_fn=apply_grad_clipping,
    #before_init=setup_config,
    #after_init=setup_mixins,
    #mixins=[
    #    LearningRateSchedule, EntropyCoeffSchedule, KLCoeffMixin,
    #    ValueNetworkMixin
    #]
)
