import logging
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
from ray.rllib.policy.sample_batch import DEFAULT_POLICY_ID
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

logger = logging.getLogger(__name__)

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

    @override(A3CTrainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["microbatch_size"]:
            # Train batch size needs to be significantly larger than microbatch_size.
            if config["train_batch_size"] / config["microbatch_size"] < 3:
                logger.warning(
                    "`train_batch_size` should be considerably larger (at least 3x) "
                    "than `microbatch_size` for a microbatching setup to make sense!"
                )
            # Rollout fragment length needs to be less than microbatch_size.
            if config["rollout_fragment_length"] > config["microbatch_size"]:
                logger.warning(
                    "`rollout_fragment_length` should not be larger than "
                    "`microbatch_size` (try setting them to the same value)! "
                    "Otherwise, microbatches of desired size won't be achievable."
                )

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)

        # Create a microbatch variable for collecting gradients on microbatches'.
        # These gradients will be accumulated on-the-fly and applied at once (once train
        # batch size has been collected) to the model.
        if (
            self.config["_disable_execution_plan_api"] is True
            and self.config["microbatch_size"]
        ):
            self._microbatches_grads = None
            self._microbatches_counts = self._num_microbatches = 0

    @override(A3CTrainer)
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
        if self._by_agent_steps:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_agent_steps=self.config["microbatch_size"]
            )
        else:
            train_batch = synchronous_parallel_sample(
                worker_set=self.workers, max_env_steps=self.config["microbatch_size"]
            )
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()

        with self._timers[COMPUTE_GRADS_TIMER]:
            grad, info = self.workers.local_worker().compute_gradients(
                train_batch, single_agent=True
            )
            # New microbatch accumulation phase.
            if self._microbatches_grads is None:
                self._microbatches_grads = grad
            # Existing gradients: Accumulate new gradients on top of existing ones.
            else:
                for i, g in enumerate(grad):
                    self._microbatches_grads[i] += g
            self._microbatches_counts += train_batch.count
            self._num_microbatches += 1

        # If `train_batch_size` reached: Accumulate gradients and apply.
        num_microbatches = math.ceil(
            self.config["train_batch_size"] / self.config["microbatch_size"]
        )
        if self._num_microbatches >= num_microbatches:
            # Update counters.
            self._counters[STEPS_TRAINED_COUNTER] += self._microbatches_counts
            self._counters[STEPS_TRAINED_THIS_ITER_COUNTER] = self._microbatches_counts

            # Apply gradients.
            apply_timer = self._timers[APPLY_GRADS_TIMER]
            with apply_timer:
                self.workers.local_worker().apply_gradients(self._microbatches_grads)
                apply_timer.push_units_processed(self._microbatches_counts)

            # Reset microbatch information.
            self._microbatches_grads = None
            self._microbatches_counts = self._num_microbatches = 0

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
