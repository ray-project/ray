from ray.rllib.agents.ppo.appo_tf_policy import AsyncPPOTFPolicy
from ray.rllib.agents.trainer import with_base_config
from ray.rllib.agents.ppo.ppo import update_kl
from ray.rllib.agents import impala

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(impala.DEFAULT_CONFIG, {
    # Whether to use V-trace weighted advantages. If false, PPO GAE advantages
    # will be used instead.
    "vtrace": False,

    # == These two options only apply if vtrace: False ==
    # Should use a critic as a baseline (otherwise don't use value baseline;
    # required for using GAE).
    "use_critic": True,
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,

    # == PPO surrogate loss options ==
    "clip_param": 0.4,

    # == PPO KL Loss options ==
    "use_kl_loss": False,
    "kl_coeff": 1.0,
    "kl_target": 0.01,

    # == IMPALA optimizer params (see documentation in impala.py) ==
    "rollout_fragment_length": 50,
    "train_batch_size": 500,
    "min_iter_time_s": 10,
    "num_workers": 2,
    "num_gpus": 0,
    "num_data_loader_buffers": 1,
    "minibatch_buffer_size": 1,
    "num_sgd_iter": 1,
    "replay_proportion": 0.0,
    "replay_buffer_num_slots": 100,
    "learner_queue_size": 16,
    "learner_queue_timeout": 300,
    "max_sample_requests_in_flight_per_worker": 2,
    "broadcast_interval": 1,
    "grad_clip": 40.0,
    "opt_type": "adam",
    "lr": 0.0005,
    "lr_schedule": None,
    "decay": 0.99,
    "momentum": 0.0,
    "epsilon": 0.1,
    "vf_loss_coeff": 0.5,
    "entropy_coeff": 0.01,
    "entropy_coeff_schedule": None,
})
# __sphinx_doc_end__
# yapf: enable


def update_target_and_kl(trainer, fetches):
    # Update the KL coeff depending on how many steps LearnerThread has stepped
    # through
    learner_steps = trainer.optimizer.learner.num_steps
    if learner_steps >= trainer.target_update_frequency:

        # Update Target Network
        trainer.optimizer.learner.num_steps = 0
        trainer.workers.local_worker().foreach_trainable_policy(
            lambda p, _: p.update_target())

        # Also update KL Coeff
        if trainer.config["use_kl_loss"]:
            update_kl(trainer, trainer.optimizer.learner.stats)


def initialize_target(trainer):
    trainer.workers.local_worker().foreach_trainable_policy(
        lambda p, _: p.update_target())
    trainer.target_update_frequency = trainer.config["num_sgd_iter"] \
        * trainer.config["minibatch_buffer_size"]


def get_policy_class(config):
    if config.get("use_pytorch") is True:
        from ray.rllib.agents.ppo.appo_torch_policy import AsyncPPOTorchPolicy
        return AsyncPPOTorchPolicy
    else:
        return AsyncPPOTFPolicy


def validate_config(config):
    if config["entropy_coeff"] < 0:
        raise ValueError("`entropy_coeff` must be >= 0.0!")


APPOTrainer = impala.ImpalaTrainer.with_updates(
    name="APPO",
    default_config=DEFAULT_CONFIG,
    validate_config=validate_config,
    default_policy=AsyncPPOTFPolicy,
    get_policy_class=get_policy_class,
    after_init=initialize_target,
    after_optimizer_step=update_target_and_kl)
