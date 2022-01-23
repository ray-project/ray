import math

from ray.util.iter import LocalIterator
from ray.rllib.agents.a3c.a3c import DEFAULT_CONFIG as A3C_CONFIG, \
    A3CTrainer
from ray.rllib.agents.trainer import Trainer
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.rollout_ops import ParallelRollouts, ConcatBatches
from ray.rllib.execution.train_ops import ComputeGradients, AverageGradients, \
    ApplyGradients, MultiGPUTrainOneStep, TrainOneStep
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import TrainerConfigDict
from ray.rllib.evaluation.worker_set import WorkerSet

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


class A2CTrainer(A3CTrainer):
    @classmethod
    @override(A3CTrainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return A2C_DEFAULT_CONFIG

    @staticmethod
    @override(Trainer)
    def execution_plan(workers: WorkerSet, config: TrainerConfigDict,
                       **kwargs) -> LocalIterator[dict]:
        assert len(kwargs) == 0, (
            "A2C execution_plan does NOT take any additional parameters")

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        if config["microbatch_size"]:
            num_microbatches = math.ceil(
                config["train_batch_size"] / config["microbatch_size"])
            # In microbatch mode, we want to compute gradients on experience
            # microbatches, average a number of these microbatches, and then
            # apply the averaged gradient in one SGD step. This conserves GPU
            # memory, allowing for extremely large experience batches to be
            # used.
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
                train_step_op = MultiGPUTrainOneStep(
                    workers=workers,
                    sgd_minibatch_size=config["train_batch_size"],
                    num_sgd_iter=1,
                    num_gpus=config["num_gpus"],
                    _fake_gpus=config["_fake_gpus"])

            train_op = rollouts.combine(
                ConcatBatches(
                    min_batch_size=config["train_batch_size"],
                    count_steps_by=config["multiagent"][
                        "count_steps_by"])).for_each(train_step_op)

        return StandardMetricsReporting(train_op, workers, config)
