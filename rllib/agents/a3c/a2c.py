import math

from ray.rllib.agents.a3c.a3c import DEFAULT_CONFIG as A3C_CONFIG, \
    validate_config, get_policy_class
from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer_template import build_trainer
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import ComputeGradients, AverageGradients, \
    ApplyGradients, TrainTFMultiGPU, TrainOneStep
from ray.rllib.utils import merge_dicts

A2C_DEFAULT_CONFIG = merge_dicts(
    A3C_CONFIG,
    {
        "rollout_fragment_length": 20,
        "min_iter_time_s": 10,
        "sample_async": False,

        # A2C supports microbatching, in which we accumulate gradients over
        # batch of this size until the train batch size is reached. This allows
        # training with batch sizes much larger than can fit in GPU memory.
        # To enable, set this to a value less than the train batch size.
        "microbatch_size": None,
    },
)


def execution_plan(workers, config):
    rollouts = ParallelRollouts(workers, mode="bulk_sync")

    if config["microbatch_size"]:
        num_microbatches = math.ceil(
            config["train_batch_size"] / config["microbatch_size"])
        # In microbatch mode, we want to compute gradients on experience
        # microbatches, average a number of these microbatches, and then apply
        # the averaged gradient in one SGD step. This conserves GPU memory,
        # allowing for extremely large experience batches to be used.
        train_op = (
            rollouts.combine(
                ConcatBatches(
                    min_batch_size=config["microbatch_size"],
                    count_steps_by=config["multiagent"]["count_steps_by"]))
            .for_each(ComputeGradients(workers))  # (grads, info)
            .batch(num_microbatches)  # List[(grads, info)]
            .for_each(AverageGradients())  # (avg_grads, info)
            .for_each(ApplyGradients(workers)))
    else:
        # In normal mode, we execute one SGD step per each train batch.
        if config["simple_optimizer"]:
            train_step_op = TrainOneStep(workers)
        else:
            train_step_op = TrainTFMultiGPU(
                workers=workers,
                sgd_minibatch_size=config["train_batch_size"],
                num_sgd_iter=1,
                num_gpus=config["num_gpus"],
                shuffle_sequences=True,
                _fake_gpus=config["_fake_gpus"],
                framework=config.get("framework"))

        train_op = rollouts.combine(
            ConcatBatches(
                min_batch_size=config["train_batch_size"],
                count_steps_by=config["multiagent"][
                    "count_steps_by"])).for_each(train_step_op)

    return StandardMetricsReporting(train_op, workers, config)


A2CTrainer = build_trainer(
    name="A2C",
    default_config=A2C_DEFAULT_CONFIG,
    default_policy=A3CTFPolicy,
    get_policy_class=get_policy_class,
    validate_config=validate_config,
    execution_plan=execution_plan)
