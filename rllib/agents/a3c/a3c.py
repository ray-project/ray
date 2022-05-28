import logging
from typing import Any, Dict, List, Optional, Type, Union

from ray.actor import ActorHandle
from ray.rllib.agents.trainer import Trainer
from ray.rllib.agents.trainer_config import TrainerConfig
from ray.rllib.evaluation.rollout_worker import RolloutWorker
from ray.rllib.execution.parallel_requests import (
    AsyncRequestsManager,
)
from ray.rllib.policy.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated
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
from ray.rllib.utils.typing import (
    ResultDict,
    TrainerConfigDict,
    PartialTrainerConfigDict,
)

logger = logging.getLogger(__name__)


class A3CConfig(TrainerConfig):
    """Defines a PPOTrainer configuration class from which a PPOTrainer can be built.

    Example:
        >>> from ray import tune
        >>> config = A3CConfig().training(lr=0.01, grad_clip=30.0)\
        ...     .resources(num_gpus=0)\
        ...     .rollouts(num_rollout_workers=4)
        >>> print(config.to_dict())
        >>> # Build a Trainer object from the config and run 1 training iteration.
        >>> trainer = config.build(env="CartPole-v1")
        >>> trainer.train()

    Example:
        >>> config = A3CConfig()
        >>> # Print out some default values.
        >>> print(config.sample_async)
        >>> # Update the config object.
        >>> config.training(lr=tune.grid_search([0.001, 0.0001]), use_critic=False)
        >>> # Set the config object's env.
        >>> config.environment(env="CartPole-v1")
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.run(
        ...     "A3C",
        ...     stop={"episode_reward_mean": 200},
        ...     config=config.to_dict(),
        ... )
    """

    def __init__(self, trainer_class=None):
        """Initializes a A3CConfig instance."""
        super().__init__(trainer_class=trainer_class or A3CTrainer)

        # fmt: off
        # __sphinx_doc_begin__
        #
        # A3C specific settings.
        self.use_critic = True
        self.use_gae = True
        self.lambda_ = 1.0
        self.grad_clip = 40.0
        self.lr_schedule = None
        self.vf_loss_coeff = 0.5
        self.entropy_coeff = 0.01
        self.entropy_coeff_schedule = None
        self.sample_async = True

        # Override some of TrainerConfig's default values with PPO-specific values.
        self.rollout_fragment_length = 10
        self.lr = 0.0001
        # Min time (in seconds) per reporting.
        # This causes not every call to `training_iteration` to be reported,
        # but to wait until n seconds have passed and then to summarize the
        # thus far collected results.
        self.min_time_s_per_reporting = 5
        # __sphinx_doc_end__
        # fmt: on

    @override(TrainerConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = None,
        use_critic: Optional[bool] = None,
        use_gae: Optional[bool] = None,
        lambda_: Optional[float] = None,
        grad_clip: Optional[float] = None,
        vf_loss_coeff: Optional[float] = None,
        entropy_coeff: Optional[float] = None,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = None,
        sample_async: Optional[bool] = None,
        **kwargs,
    ) -> "A3CConfig":
        """Sets the training related configuration.

        Args:
            lr_schedule: Learning rate schedule. In the format of
                [[timestep, lr-value], [timestep, lr-value], ...]
                Intermediary timesteps will be assigned to interpolated learning rate
                values. A schedule should normally start from timestep 0.
            use_critic: Should use a critic as a baseline (otherwise don't use value
                baseline; required for using GAE).
            use_gae: If true, use the Generalized Advantage Estimator (GAE)
                with a value function, see https://arxiv.org/pdf/1506.02438.pdf.
            lambda_: GAE(gamma) parameter.
            grad_clip: Max global norm for each gradient calculated by worker.
            vf_loss_coeff: Value Function Loss coefficient.
            entropy_coeff: Coefficient of the entropy regularizer.
            entropy_coeff_schedule: Decay schedule for the entropy regularizer.
            sample_async: Whether workers should sample async. Note that this
                increases the effective rollout_fragment_length by up to 5x due
                to async buffering of batches.

        Returns:
            This updated TrainerConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if lr_schedule is not None:
            self.lr_schedule = lr_schedule
        if use_critic is not None:
            self.lr_schedule = use_critic
        if use_gae is not None:
            self.use_gae = use_gae
        if lambda_ is not None:
            self.lambda_ = lambda_
        if grad_clip is not None:
            self.grad_clip = grad_clip
        if vf_loss_coeff is not None:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not None:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not None:
            self.entropy_coeff_schedule = entropy_coeff_schedule
        if sample_async is not None:
            self.sample_async = sample_async

        return self


class A3CTrainer(Trainer):
    @classmethod
    @override(Trainer)
    def get_default_config(cls) -> TrainerConfigDict:
        return A3CConfig().to_dict()

    @override(Trainer)
    def setup(self, config: PartialTrainerConfigDict):
        super().setup(config)
        self._worker_manager = AsyncRequestsManager(
            self.workers.remote_workers(), max_remote_requests_in_flight_per_worker=1
        )

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
        elif config["framework"] == "tf":
            from ray.rllib.agents.a3c.a3c_tf_policy import A3CStaticGraphTFPolicy

            return A3CStaticGraphTFPolicy
        else:
            from ray.rllib.agents.a3c.a3c_tf_policy import A3CEagerTFPolicy

            return A3CEagerTFPolicy

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
            self._worker_manager.call_on_all_available(sample_and_compute_grads)
            async_results = self._worker_manager.get_ready()

        # Loop through all fetched worker-computed gradients (if any)
        # and apply them - one by one - to the local worker's model.
        # After each apply step (one step per worker that returned some gradients),
        # update that particular worker's weights.
        global_vars = None
        learner_info_builder = LearnerInfoBuilder(num_devices=1)
        for worker, results in async_results.items():
            for result in results:
                # Apply gradients to local worker.
                with self._timers[APPLY_GRADS_TIMER]:
                    local_worker.apply_gradients(result["grads"])
                self._timers[APPLY_GRADS_TIMER].push_units_processed(
                    result["agent_steps"]
                )

                # Update all step counters.
                self._counters[NUM_AGENT_STEPS_SAMPLED] += result["agent_steps"]
                self._counters[NUM_ENV_STEPS_SAMPLED] += result["env_steps"]
                self._counters[NUM_AGENT_STEPS_TRAINED] += result["agent_steps"]
                self._counters[NUM_ENV_STEPS_TRAINED] += result["env_steps"]

                learner_info_builder.add_learn_on_batch_results_multi_agent(
                    result["infos"]
                )

            # Create current global vars.
            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }

            # Synch updated weights back to the particular worker.
            with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
                weights = local_worker.get_weights(local_worker.get_policies_to_train())
                worker.set_weights.remote(weights, global_vars)

        # Update global vars of the local worker.
        if global_vars:
            local_worker.set_global_vars(global_vars)

        return learner_info_builder.finalize()

    @override(Trainer)
    def on_worker_failures(
        self, removed_workers: List[ActorHandle], new_workers: List[ActorHandle]
    ):
        """Handle failures on remote A3C workers.

        Args:
            removed_workers: removed worker ids.
            new_workers: ids of newly created workers.
        """
        self._worker_manager.remove_workers(removed_workers)
        self._worker_manager.add_workers(new_workers)


# Deprecated: Use ray.rllib.agents.a3c.A3CConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(A3CConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.ppo.ppo.DEFAULT_CONFIG",
        new="ray.rllib.agents.ppo.ppo.PPOConfig(...)",
        error=False,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
