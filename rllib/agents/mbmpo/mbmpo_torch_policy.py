import logging

import ray
from ray.rllib.policy.torch_policy_template import build_torch_policy
from ray.rllib.agents.ppo.ppo_tf_policy import postprocess_ppo_gae, \
    setup_config
from ray.rllib.agents.ppo.ppo_torch_policy import vf_preds_fetches
from ray.rllib.agents.a3c.a3c_torch_policy import apply_grad_clipping
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.catalog import ModelCatalog
from ray.rllib.agents.maml.maml_torch_policy import setup_mixins, \
    maml_loss, maml_stats, maml_optimizer_fn, KLCoeffMixin

torch, nn = try_import_torch()

logger = logging.getLogger(__name__)


def make_model_and_action_dist(policy, obs_space, action_space, config):
    # Get the output distribution class for predicting rewards and next-obs.
    policy.distr_cls_next_obs, num_outputs = ModelCatalog.get_action_dist(
        obs_space, config, dist_type="deterministic", framework="torch")

    # Build one dynamics model if we are a Worker.
    # If we are the main MAML learner, build n (num_workers) dynamics Models
    # for being able to create checkpoints for the current state of training.
    device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))
    policy.dynamics_model = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["dynamics_model"],
        framework="torch",
        name="dynamics_ensemble",
    ).to(device)

    action_dist, num_outputs = ModelCatalog.get_action_dist(
        action_space, config, framework="torch")
    # Create the pi-model and register it with the Policy.
    policy.pi = ModelCatalog.get_model_v2(
        obs_space,
        action_space,
        num_outputs=num_outputs,
        model_config=config["model"],
        framework="torch",
        name="policy_model",
    )

    return policy.pi, action_dist


MBMPOTorchPolicy = build_torch_policy(
    name="MBMPOTorchPolicy",
    get_default_config=lambda: ray.rllib.agents.mbmpo.mbmpo.DEFAULT_CONFIG,
    make_model_and_action_dist=make_model_and_action_dist,
    loss_fn=maml_loss,
    stats_fn=maml_stats,
    optimizer_fn=maml_optimizer_fn,
    extra_action_out_fn=vf_preds_fetches,
    postprocess_fn=postprocess_ppo_gae,
    extra_grad_process_fn=apply_grad_clipping,
    before_init=setup_config,
    after_init=setup_mixins,
    mixins=[KLCoeffMixin])
