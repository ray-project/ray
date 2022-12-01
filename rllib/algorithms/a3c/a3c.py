import logging
from typing import Any, Dict, List, Optional, Type, Union

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.evaluation.rollout_worker import RolloutWorker
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
from ray.rllib.utils.typing import ResultDict

logger = logging.getLogger(__name__)


class A3CConfig(AlgorithmConfig):
    """Defines a configuration class from which a A3C Algorithm can be built.

    Example:
        >>> from ray import tune
        >>> from ray.rllib.algorithms.a3c import A3CConfig
        >>> config = A3CConfig() # doctest: +SKIP
        >>> config = config.training(lr=0.01, grad_clip=30.0) # doctest: +SKIP
        >>> config = config.resources(num_gpus=0) # doctest: +SKIP
        >>> config = config.rollouts(num_rollout_workers=4) # doctest: +SKIP
        >>> config = config.environment("CartPole-v1") # doctest: +SKIP
        >>> print(config.to_dict())  # doctest: +SKIP
        >>> # Build a Algorithm object from the config and run 1 training iteration.
        >>> algo = config.build()  # doctest: +SKIP
        >>> algo.train()  # doctest: +SKIP

    Example:
        >>> from ray.rllib.algorithms.a3c import A3CConfig
        >>> config = A3CConfig()
        >>> # Print out some default values.
        >>> print(config.sample_async)  # doctest: +SKIP
        >>> # Update the config object.
        >>> config = config.training( # doctest: +SKIP
        ...     lr=tune.grid_search([0.001, 0.0001]), use_critic=False)
        >>> # Set the config object's env.
        >>> config = config.environment(env="CartPole-v1") # doctest: +SKIP
        >>> # Use to_dict() to get the old-style python config dict
        >>> # when running with tune.
        >>> tune.Tuner(  # doctest: +SKIP
        ...     "A3C",
        ...     stop={"episode_reward_mean": 200},
        ...     param_space=config.to_dict(),
        ... ).fit()
    """

    def __init__(self, algo_class=None):
        """Initializes a A3CConfig instance."""
        super().__init__(algo_class=algo_class or A3C)

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

        # Override some of AlgorithmConfig's default values with PPO-specific values.
        self.num_rollout_workers = 2
        self.rollout_fragment_length = 10
        self.lr = 0.0001
        # Min time (in seconds) per reporting.
        # This causes not every call to `training_iteration` to be reported,
        # but to wait until n seconds have passed and then to summarize the
        # thus far collected results.
        self.min_time_s_per_iteration = 5
        # __sphinx_doc_end__
        # fmt: on

    @override(AlgorithmConfig)
    def training(
        self,
        *,
        lr_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        use_critic: Optional[bool] = NotProvided,
        use_gae: Optional[bool] = NotProvided,
        lambda_: Optional[float] = NotProvided,
        grad_clip: Optional[float] = NotProvided,
        vf_loss_coeff: Optional[float] = NotProvided,
        entropy_coeff: Optional[float] = NotProvided,
        entropy_coeff_schedule: Optional[List[List[Union[int, float]]]] = NotProvided,
        sample_async: Optional[bool] = NotProvided,
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
            This updated AlgorithmConfig object.
        """
        # Pass kwargs onto super's `training()` method.
        super().training(**kwargs)

        if lr_schedule is not NotProvided:
            self.lr_schedule = lr_schedule
        if use_critic is not NotProvided:
            self.lr_schedule = use_critic
        if use_gae is not NotProvided:
            self.use_gae = use_gae
        if lambda_ is not NotProvided:
            self.lambda_ = lambda_
        if grad_clip is not NotProvided:
            self.grad_clip = grad_clip
        if vf_loss_coeff is not NotProvided:
            self.vf_loss_coeff = vf_loss_coeff
        if entropy_coeff is not NotProvided:
            self.entropy_coeff = entropy_coeff
        if entropy_coeff_schedule is not NotProvided:
            self.entropy_coeff_schedule = entropy_coeff_schedule
        if sample_async is not NotProvided:
            self.sample_async = sample_async

        return self

    @override(AlgorithmConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.entropy_coeff < 0:
            raise ValueError("`entropy_coeff` must be >= 0.0!")
        if self.num_rollout_workers <= 0 and self.sample_async:
            raise ValueError("`num_workers` for A3C must be >= 1!")


class A3C(Algorithm):
    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return A3CConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.a3c.a3c_torch_policy import A3CTorchPolicy

            return A3CTorchPolicy
        elif config["framework"] == "tf":
            from ray.rllib.algorithms.a3c.a3c_tf_policy import A3CTF1Policy

            return A3CTF1Policy
        else:
            from ray.rllib.algorithms.a3c.a3c_tf_policy import A3CTF2Policy

            return A3CTF2Policy

    def training_step(self) -> ResultDict:
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
            self.workers.foreach_worker_async(
                func=sample_and_compute_grads,
                healthy_only=True,
            )
            async_results = self.workers.fetch_ready_async_reqs()

        # Loop through all fetched worker-computed gradients (if any)
        # and apply them - one by one - to the local worker's model.
        # After each apply step (one step per worker that returned some gradients),
        # update that particular worker's weights.
        global_vars = None
        learner_info_builder = LearnerInfoBuilder(num_devices=1)
        to_sync_workers = set()
        for worker_id, result in async_results:
            # Apply gradients to local worker.
            with self._timers[APPLY_GRADS_TIMER]:
                local_worker.apply_gradients(result["grads"])
            self._timers[APPLY_GRADS_TIMER].push_units_processed(result["agent_steps"])

            # Update all step counters.
            self._counters[NUM_AGENT_STEPS_SAMPLED] += result["agent_steps"]
            self._counters[NUM_ENV_STEPS_SAMPLED] += result["env_steps"]
            self._counters[NUM_AGENT_STEPS_TRAINED] += result["agent_steps"]
            self._counters[NUM_ENV_STEPS_TRAINED] += result["env_steps"]

            learner_info_builder.add_learn_on_batch_results_multi_agent(result["infos"])

            # Create current global vars.
            global_vars = {
                "timestep": self._counters[NUM_AGENT_STEPS_SAMPLED],
            }

            # Add this worker to be synced.
            to_sync_workers.add(worker_id)

        # Synch updated weights back to the particular worker
        # (only those policies that are trainable).
        with self._timers[SYNCH_WORKER_WEIGHTS_TIMER]:
            self.workers.sync_weights(
                policies=local_worker.get_policies_to_train(),
                to_worker_indices=list(to_sync_workers),
                global_vars=global_vars,
            )

        return learner_info_builder.finalize()


# Deprecated: Use ray.rllib.algorithms.a3c.A3CConfig instead!
class _deprecated_default_config(dict):
    def __init__(self):
        super().__init__(A3CConfig().to_dict())

    @Deprecated(
        old="ray.rllib.agents.a3c.a3c.DEFAULT_CONFIG",
        new="ray.rllib.algorithms.a3c.a3c.A3CConfig(...)",
        error=True,
    )
    def __getitem__(self, item):
        return super().__getitem__(item)


DEFAULT_CONFIG = _deprecated_default_config()
