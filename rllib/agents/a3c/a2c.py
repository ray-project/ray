import math

from ray.rllib.agents.a3c.a3c import DEFAULT_CONFIG as A3C_CONFIG, A3CTrainer
from ray.rllib.agents.trainer import Trainer
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.common import (
    STEPS_TRAINED_COUNTER,
    STEPS_TRAINED_THIS_ITER_COUNTER,
)
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.rollout_ops import (
    ParallelRollouts,
    ConcatBatches,
    synchronous_parallel_sample,
)
from ray.rllib.execution.train_ops import (
    ComputeGradients,
    AverageGradients,
    ApplyGradients,
    MultiGPUTrainOneStep,
    TrainOneStep,
)
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID, SampleBatch
from ray.rllib.utils import merge_dicts
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    APPLY_GRADS_TIMER,
    COMPUTE_GRADS_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    WORKER_UPDATE_TIMER,
)
from ray.rllib.utils.typing import (
    PartialTrainerConfigDict,
    ResultDict,
    TrainerConfigDict,
)
from ray.util.iter import LocalIterator

A2C_DEFAULT_CONFIG = merge_dicts(
    A3C_CONFIG,
    {
        "rollout_fragment_length": 20,
        "min_time_s_per_reporting": 10,
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

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)

        # Create a microbatch buffer for collecting gradients on microbatches'.
        # These gradients will later be accumulated and applied at once (once train
        # batch size has been collected) to the model.
        if self.config["_disable_execution_plan_api"] is True and \
                self.config["microbatch_size"]:
            self._microbatches = []

    @override(Trainer)
    def training_iteration(self) -> ResultDict:
        # W/o microbatching: Identical to Trainer's default implementation.
        # Only difference to a default Trainer being the value function loss term
        # and its value computations alongside each action.
        if self.config["microbatch_size"] is None:
            return Trainer.training_iteration(self)

        # In microbatch mode, we want to compute gradients on experience
        # microbatches, average a number of these microbatches, and then
        # apply the averaged gradient in one SGD step. This conserves GPU
        # memory, allowing for extremely large experience batches to be
        # used.
        sample_batches = []
        num_env_steps = 0
        num_agent_steps = 0
        while (
            not self._by_agent_steps and num_env_steps < self.config["microbatch_size"]
        ) or (
            self._by_agent_steps and num_agent_steps < self.config["microbatch_size"]
        ):
            new_sample_batches = synchronous_parallel_sample(self.workers)
            sample_batches.extend(new_sample_batches)
            num_env_steps += sum(len(s) for s in new_sample_batches)
            num_agent_steps += sum(
                len(s) if isinstance(s, SampleBatch) else s.agent_steps()
                for s in new_sample_batches
            )
        self._counters[NUM_ENV_STEPS_SAMPLED] += num_env_steps
        self._counters[NUM_AGENT_STEPS_SAMPLED] += num_agent_steps

        # Combine all batches at once
        train_batch = SampleBatch.concat_samples(sample_batches)

        with self._timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(
                train_batch, single_agent=True
            )
            self._microbatches.append((grad, train_batch.count))

        # If `train_batch_size` reached: Accumulate gradients and apply.
        num_microbatches = math.ceil(
            self.config["train_batch_size"] / self.config["microbatch_size"]
        )
        if len(self._microbatches) > num_microbatches:
            # Accumulate gradients.
            acc_gradients = None
            sum_count = 0
            for grad, count in self._microbatches:
                if acc_gradients is None:
                    acc_gradients = grad
                else:
                    acc_gradients = [a + b for a, b in zip(acc_gradients, grad)]
                sum_count += count

            # Empty microbatch buffer.
            self._microbatches = []

            # Apply gradients.
            self._counters[STEPS_TRAINED_COUNTER] += sum_count
            self._counters[STEPS_TRAINED_THIS_ITER_COUNTER] = sum_count

            apply_timer = self._timers[APPLY_GRADS_TIMER]
            with apply_timer:
                self.workers.local_worker().apply_gradients(acc_gradients)
                apply_timer.push_units_processed(sum_count)

            # Also update global vars of the local worker.
            # Create current global vars.
            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }
            with self._timers[WORKER_UPDATE_TIMER]:
                self.workers.sync_weights(
                    policies=self.workers.local_worker().get_policies_to_train(),
                    global_vars=global_vars,
                )

        train_results = {DEFAULT_POLICY_ID: info}

        return train_results

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "A2C execution_plan does NOT take any additional parameters"

        rollouts = ParallelRollouts(workers, mode="bulk_sync")

        if config["microbatch_size"]:
            num_microbatches = math.ceil(
                config["train_batch_size"] / config["microbatch_size"]
            )
            # In microbatch mode, we want to compute gradients on experience
            # microbatches, average a number of these microbatches, and then
            # apply the averaged gradient in one SGD step. This conserves GPU
            # memory, allowing for extremely large experience batches to be
            # used.
            train_op = (
                rollouts.combine(
                    ConcatBatches(
                        min_batch_size=config["microbatch_size"],
                        count_steps_by=config["multiagent"]["count_steps_by"],
                    )
                )
                .for_each(ComputeGradients(workers))  # (grads, info)
                .batch(num_microbatches)  # List[(grads, info)]
                .for_each(AverageGradients())  # (avg_grads, info)
                .for_each(ApplyGradients(workers))
            )
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
                    _fake_gpus=config["_fake_gpus"],
                )

            train_op = rollouts.combine(
                ConcatBatches(
                    min_batch_size=config["train_batch_size"],
                    count_steps_by=config["multiagent"]["count_steps_by"],
                )
            ).for_each(train_step_op)

        return StandardMetricsReporting(train_op, workers, config)
