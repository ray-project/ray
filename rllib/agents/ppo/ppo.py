import logging

from ray.rllib.agents import with_common_config
from ray.rllib.agents.ppo.ppo_tf_policy import PPOTFPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.optimizers import SyncSamplesOptimizer, LocalMultiGPUOptimizer
from ray.rllib.utils import try_import_tf

tf = try_import_tf()

logger = logging.getLogger(__name__)

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Should use a critic as a baseline (otherwise don't use value baseline;
    # required for using GAE).
    "use_critic": True,
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # The GAE(lambda) parameter.
    "lambda": 1.0,
    # Initial coefficient for KL divergence.
    "kl_coeff": 0.2,
    # Size of batches collected from each worker.
    "rollout_fragment_length": 200,
    # Number of timesteps collected for each SGD round. This defines the size
    # of each SGD epoch.
    "train_batch_size": 4000,
    # Total SGD batch size across all devices for SGD. This defines the
    # minibatch size within each epoch.
    "sgd_minibatch_size": 128,
    # Whether to shuffle sequences in the batch when training (recommended).
    "shuffle_sequences": True,
    # Number of SGD iterations in each outer loop (i.e., number of epochs to
    # execute per train batch).
    "num_sgd_iter": 30,
    # Stepsize of SGD.
    "lr": 5e-5,
    # Learning rate schedule.
    "lr_schedule": None,
    # Share layers for value function. If you set this to True, it's important
    # to tune vf_loss_coeff.
    "vf_share_layers": False,
    # Coefficient of the value function loss. IMPORTANT: you must tune this if
    # you set vf_share_layers: True.
    "vf_loss_coeff": 1.0,
    # Coefficient of the entropy regularizer.
    "entropy_coeff": 0.0,
    # Decay schedule for the entropy regularizer.
    "entropy_coeff_schedule": None,
    # PPO clip parameter.
    "clip_param": 0.3,
    # Clip param for the value function. Note that this is sensitive to the
    # scale of the rewards. If your expected V is large, increase this.
    "vf_clip_param": 10.0,
    # If specified, clip the global norm of gradients by this amount.
    "grad_clip": None,
    # Target value for KL divergence.
    "kl_target": 0.01,
    # Whether to rollout "complete_episodes" or "truncate_episodes".
    "batch_mode": "truncate_episodes",
    # Which observation filter to apply to the observation.
    "observation_filter": "NoFilter",
    # Uses the sync samples optimizer instead of the multi-gpu one. This is
    # usually slower, but you might want to try it if you run into issues with
    # the default optimizer.
    "simple_optimizer": False,
    # Whether to fake GPUs (using CPUs).
    # Set this to True for debugging on non-GPU machines (set `num_gpus` > 0).
    "_fake_gpus": False,
    # Use PyTorch as framework?
    "use_pytorch": False
})
# __sphinx_doc_end__
# yapf: enable


def choose_policy_optimizer(workers, config):
    if config["simple_optimizer"]:
        return SyncSamplesOptimizer(
            workers,
            num_sgd_iter=config["num_sgd_iter"],
            train_batch_size=config["train_batch_size"],
            sgd_minibatch_size=config["sgd_minibatch_size"],
            standardize_fields=["advantages"])

    return LocalMultiGPUOptimizer(
        workers,
        sgd_batch_size=config["sgd_minibatch_size"],
        num_sgd_iter=config["num_sgd_iter"],
        num_gpus=config["num_gpus"],
        rollout_fragment_length=config["rollout_fragment_length"],
        num_envs_per_worker=config["num_envs_per_worker"],
        train_batch_size=config["train_batch_size"],
        standardize_fields=["advantages"],
        shuffle_sequences=config["shuffle_sequences"],
        _fake_gpus=config["_fake_gpus"])


def update_kl(trainer, fetches):
    # Single-agent.
    if "kl" in fetches:
        trainer.workers.local_worker().for_policy(
            lambda pi: pi.update_kl(fetches["kl"]))

    # Multi-agent.
    else:

        def update(pi, pi_id):
            if pi_id in fetches:
                pi.update_kl(fetches[pi_id]["kl"])
            else:
                logger.debug("No data for {}, not updating kl".format(pi_id))

        trainer.workers.local_worker().foreach_trainable_policy(update)


def warn_about_bad_reward_scales(trainer, result):
    if result["policy_reward_mean"]:
        return  # Punt on handling multiagent case.

    # Warn about excessively high VF loss.
    learner_stats = result["info"]["learner"]
    if "default_policy" in learner_stats:
        scaled_vf_loss = (trainer.config["vf_loss_coeff"] *
                          learner_stats["default_policy"]["vf_loss"])
        policy_loss = learner_stats["default_policy"]["policy_loss"]
        if trainer.config["vf_share_layers"] and scaled_vf_loss > 100:
            logger.warning(
                "The magnitude of your value function loss is extremely large "
                "({}) compared to the policy loss ({}). This can prevent the "
                "policy from learning. Consider scaling down the VF loss by "
                "reducing vf_loss_coeff, or disabling vf_share_layers.".format(
                    scaled_vf_loss, policy_loss))

    # Warn about bad clipping configs
    if trainer.config["vf_clip_param"] <= 0:
        rew_scale = float("inf")
    else:
        rew_scale = round(
            abs(result["episode_reward_mean"]) /
            trainer.config["vf_clip_param"], 0)
    if rew_scale > 200:
        logger.warning(
            "The magnitude of your environment rewards are more than "
            "{}x the scale of `vf_clip_param`. ".format(rew_scale) +
            "This means that it will take more than "
            "{} iterations for your value ".format(rew_scale) +
            "function to converge. If this is not intended, consider "
            "increasing `vf_clip_param`.")


def validate_config(config):
    if config["entropy_coeff"] < 0:
        raise DeprecationWarning("entropy_coeff must be >= 0")
    if isinstance(config["entropy_coeff"], int):
        config["entropy_coeff"] = float(config["entropy_coeff"])
    if config["sgd_minibatch_size"] > config["train_batch_size"]:
        raise ValueError(
            "Minibatch size {} must be <= train batch size {}.".format(
                config["sgd_minibatch_size"], config["train_batch_size"]))
    if config["batch_mode"] == "truncate_episodes" and not config["use_gae"]:
        raise ValueError(
            "Episode truncation is not supported without a value "
            "function. Consider setting batch_mode=complete_episodes.")
    if config["multiagent"]["policies"] and not config["simple_optimizer"]:
        logger.info(
            "In multi-agent mode, policies will be optimized sequentially "
            "by the multi-GPU optimizer. Consider setting "
            "simple_optimizer=True if this doesn't work for you.")
    if config["simple_optimizer"]:
        logger.warning(
            "Using the simple minibatch optimizer. This will significantly "
            "reduce performance, consider simple_optimizer=False.")
    elif config["use_pytorch"] or (tf and tf.executing_eagerly()):
        config["simple_optimizer"] = True  # multi-gpu not supported


def get_policy_class(config):
    if config["use_pytorch"]:
        from ray.rllib.agents.ppo.ppo_torch_policy import PPOTorchPolicy
        return PPOTorchPolicy
    else:
        return PPOTFPolicy


PPOTrainer = build_trainer(
    name="PPO",
    default_config=DEFAULT_CONFIG,
    default_policy=PPOTFPolicy,
    get_policy_class=get_policy_class,
    make_policy_optimizer=choose_policy_optimizer,
    validate_config=validate_config,
    after_optimizer_step=update_kl,
    after_train_result=warn_about_bad_reward_scales)
