from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

from ray.rllib.agents.ppo.appo_policy import AsyncPPOTFPolicy
from ray.rllib.agents.trainer import with_base_config
from ray.rllib.agents import impala
from ray.rllib.optimizers import AsyncSamplesOptimizer
from ray.rllib.evaluation.rollout_worker import RolloutWorker

# yapf: disable
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_base_config(impala.DEFAULT_CONFIG, {
    # Whether to use V-trace weighted advantages. If false, PPO GAE advantages
    # will be used instead.
    "vtrace": True,

    # == These two options only apply if vtrace: False ==
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # GAE(lambda) parameter
    "lambda": 1.0,

    # == PPO surrogate loss options ==
    "clip_param": 0.4,

    # == PPO KL Loss options ==
    "use_kl_loss": True,
    "kl_coeff": 1.0,
    "kl_target": 0.01,

    # == IMPALA optimizer params (see documentation in impala.py) ==
    "sample_batch_size": 50,
    "train_batch_size": 500,
    "min_iter_time_s": 10,
    "num_workers": 2,
    "num_gpus": 1,
    "num_data_loader_buffers": 1,
    "minibatch_buffer_size": 1,
    "num_sgd_iter": 1,
    "replay_proportion": 0.0,
    "replay_buffer_num_slots": 100,
    "learner_queue_size": 16,
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


def make_aggregators_and_optimizer(workers, config):
    if config["num_aggregation_workers"] > 0:
        # Create co-located aggregator actors first for placement pref
        aggregators = TreeAggregator.precreate_aggregators(
            config["num_aggregation_workers"])
    else:
        aggregators = None
    workers.add_workers(config["num_workers"])

    optimizer = AsyncSamplesOptimizer(
        workers,
        use_importance_sampling=True,
        lr=config["lr"],
        num_envs_per_worker=config["num_envs_per_worker"],
        num_gpus=config["num_gpus"],
        sample_batch_size=config["sample_batch_size"],
        train_batch_size=config["train_batch_size"],
        replay_buffer_num_slots=config["replay_buffer_num_slots"],
        replay_proportion=config["replay_proportion"],
        num_data_loader_buffers=config["num_data_loader_buffers"],
        max_sample_requests_in_flight_per_worker=config[
            "max_sample_requests_in_flight_per_worker"],
        broadcast_interval=config["broadcast_interval"],
        num_sgd_iter=config["num_sgd_iter"],
        minibatch_buffer_size=config["minibatch_buffer_size"],
        num_aggregation_workers=config["num_aggregation_workers"],
        **config["optimizer"])

    if aggregators:
        # Assign the pre-created aggregators to the optimizer
        optimizer.aggregator.init(aggregators)
    return optimizer


APPOTrainer = impala.ImpalaTrainer.with_updates(
    name="APPO",
    default_config=DEFAULT_CONFIG,
    default_policy=AsyncPPOTFPolicy,
    get_policy_class=lambda _: AsyncPPOTFPolicy,
    make_policy_optimizer=make_aggregators_and_optimizer)
