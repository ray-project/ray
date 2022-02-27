import logging
from typing import Any, Dict, Type

from ray.actor import ActorHandle
from ray.rllib.agents.a3c.a3c_tf_policy import A3CTFPolicy
from ray.rllib.agents.trainer import Trainer, with_common_config
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.evaluation.worker_set import WorkerSet
from ray.rllib.execution.metric_ops import StandardMetricsReporting
from ray.rllib.execution.parallel_requests import asynchronous_parallel_requests
from ray.rllib.execution.rollout_ops import AsyncGradients
from ray.rllib.execution.train_ops import ApplyGradients
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.metrics import (
    APPLY_GRADS_TIMER,
    GRAD_WAIT_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_SAMPLED,
    NUM_ENV_STEPS_TRAINED,
    SYNCH_WORKER_WEIGHTS_TIMER,
)
from ray.rllib.utils.metrics.learner_info import LearnerInfoBuilder
from ray.rllib.utils.typing import ResultDict, TrainerConfigDict
from ray.util.iter import LocalIterator

logger = logging.getLogger(__name__)

# fmt: off
# __sphinx_doc_begin__
DEFAULT_CONFIG = with_common_config({
    # Should use a critic as a baseline (otherwise don't use value baseline;
    # required for using GAE).
    "use_critic": True,
    # If true, use the Generalized Advantage Estimator (GAE)
    # with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
    "use_gae": True,
    # Size of rollout batch
    "rollout_fragment_length": 10,
    # GAE(gamma) parameter
    "lambda": 1.0,
    # Max global norm for each gradient calculated by worker
    "grad_clip": 40.0,
    # Learning rate
    "lr": 0.0001,
    # Learning rate schedule
    "lr_schedule": None,
    # Value Function Loss coefficient
    "vf_loss_coeff": 0.5,
    # Entropy coefficient
    "entropy_coeff": 0.01,
    # Entropy coefficient schedule
    "entropy_coeff_schedule": None,
    # Min time (in seconds) per reporting.
    # This causes not every call to `training_iteration` to be reported,
    # but to wait until n seconds have passed and then to summarize the
    # thus far collected results.
    "min_time_s_per_reporting": 5,
    # Workers sample async. Note that this increases the effective
    # rollout_fragment_length by up to 5x due to async buffering of batches.
    "sample_async": True,

    # Use the Trainer's `training_iteration` function instead of `execution_plan`.
    # Fixes a severe performance problem with A3C. Setting this to True leads to a
    # speedup of up to 3x for a large number of workers and heavier
    # gradient computations (e.g. ray/rllib/tuned_examples/a3c/pong-a3c.yaml)).
    "_disable_execution_plan_api": True,
})
# __sphinx_doc_end__
# fmt: on


class A3CTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return DEFAULT_CONFIG

    @override(Trainer)
    def validate_config(self, config: TrainerConfigDict) -> None:
        # Call super's validation method.
        super().validate_config(config)

        if config["entropy_coeff"] < 0:
            raise ValueError("`entropy_coeff` must be >= 0.0!")
        if config["num_workers"] <= 0 and config["sample_async"]:
            raise ValueError("`num_workers` for A3C must be >= 1!")

    @override(Trainer)
    def get_default_policy_class(self, config: TrainerConfigDict) -> Type[Policy]:
        if config["framework"] == "torch":
            from ray.rllib.agents.a3c.a3c_torch_policy import A3CTorchPolicy

            return A3CTorchPolicy
        else:
            return A3CTFPolicy

    def training_iteration(self) -> ResultDict:
        # Shortcut.
        local_worker = self.workers.local_worker()

        # Define the function executed in parallel by all RolloutWorkers to collect
        # samples + compute and return gradients (and other information).

        def sample_and_compute_grads(worker: RolloutWorker) -> Dict[str, Any]:
            """Call sample() and compute_gradients() remotely on workers."""
            samples = worker.sample()
            grads, infos = worker.compute_gradients(samples)
            return {
                "grads": grads,
                "infos": infos,
                "agent_steps": samples.agent_steps(),
                "env_steps": samples.env_steps(),
            }

        # Perform rollouts and gradient calculations asynchronously.
        with self._timers[GRAD_WAIT_TIMER]:
            # Results are a mapping from ActorHandle (RolloutWorker) to their
            # returned gradient calculation results.
            async_results: Dict[ActorHandle, Dict] = asynchronous_parallel_requests(
                remote_requests_in_flight=self.remote_requests_in_flight,
                actors=self.workers.remote_workers(),
                ray_wait_timeout_s=0.0,
                max_remote_requests_in_flight_per_actor=1,
                remote_fn=sample_and_compute_grads,
            )

        # Loop through all fetched worker-computed gradients (if any)
        # and apply them - one by one - to the local worker's model.
        # After each apply step (one step per worker that returned some gradients),
        # update that particular worker's weights.
        global_vars = None
        learner_info_builder = LearnerInfoBuilder(num_devices=1)
        for worker, result in async_results.items():
            # Apply gradients to local worker.
            with self._timers[APPLY_GRADS_TIMER]:
                local_worker.apply_gradients(result["grads"])
            self._timers[APPLY_GRADS_TIMER].push_units_processed(result["agent_steps"])

            # Update all step counters.
            self._counters[NUM_AGENT_STEPS_SAMPLED] += result["agent_steps"]
            self._counters[NUM_ENV_STEPS_SAMPLED] += result["env_steps"]
            self._counters[NUM_AGENT_STEPS_TRAINED] += result["agent_steps"]
            self._counters[NUM_ENV_STEPS_TRAINED] += result["env_steps"]

            # Create current global vars.
            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }

            # Synch updated weights back to the particular worker.
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                weights = local_worker.get_weights(local_worker.get_policies_to_train())
                worker.set_weights.remote(weights, global_vars)

            learner_info_builder.add_learn_on_batch_results_multi_agent(result["infos"])

        # Update global vars of the local worker.
        if global_vars:
            local_worker.set_global_vars(global_vars)

        return learner_info_builder.finalize()

    @staticmethod
    @override(Trainer)
    def execution_plan(
        workers: WorkerSet, config: TrainerConfigDict, **kwargs
    ) -> LocalIterator[dict]:
        assert (
            len(kwargs) == 0
        ), "A3C execution_plan does NOT take any additional parameters"

        # For A3C, compute policy gradients remotely on the rollout workers.
        grads = AsyncGradients(workers)

        # Apply the gradients as they arrive. We set update_all to False so
        # that only the worker sending the gradient is updated with new
        # weights.
        train_op = grads.for_each(ApplyGradients(workers, update_all=False))

        return StandardMetricsReporting(train_op, workers, config)
