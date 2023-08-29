import logging
from typing import List, Optional, Type

from ray.rllib.algorithms.algorithm import Algorithm
from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override
from ray.rllib.utils.deprecation import Deprecated, ALGO_DEPRECATION_WARNING
from ray.rllib.utils.metrics import (
    LAST_TARGET_UPDATE_TS,
    NUM_AGENT_STEPS_TRAINED,
    NUM_ENV_STEPS_TRAINED,
    NUM_TARGET_UPDATES,
    TARGET_NET_UPDATE_TIMER,
    NUM_AGENT_STEPS_SAMPLED,
    NUM_ENV_STEPS_SAMPLED,
    SAMPLE_TIMER,
)
from ray.rllib.utils.typing import ResultDict

logger = logging.getLogger(__name__)


class CRRConfig(AlgorithmConfig):
    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or CRR)

        # fmt: off
        # __sphinx_doc_begin__
        # CRR-specific settings.
        self.weight_type = "bin"
        self.temperature = 1.0
        self.max_weight = 20.0
        self.advantage_type = "mean"
        self.n_action_sample = 4
        self.twin_q = True
        self.train_batch_size = 128

        # target_network_update_freq by default is 100 * train_batch_size
        # if target_network_update_freq is not set. See self.setup for code.
        self.target_network_update_freq = None
        # __sphinx_doc_end__
        # fmt: on
        self.actor_hiddens = [256, 256]
        self.actor_hidden_activation = "relu"
        self.critic_hiddens = [256, 256]
        self.critic_hidden_activation = "relu"
        self.critic_lr = 3e-4
        self.actor_lr = 3e-4
        self.tau = 5e-3

        # Override the AlgorithmConfig default:
        # Only PyTorch supported thus far. Make this the default framework.
        self.framework_str = "torch"
        # If data ingestion/sample_time is slow, increase this
        self.num_rollout_workers = 4
        self.offline_sampling = True
        self.min_time_s_per_iteration = 10.0

        self.td_error_loss_fn = "mse"
        self.categorical_distribution_temperature = 1.0

        # TODO (Artur): CRR should not need an exploration config as an offline
        #  algorithm. However, the current implementation of the CRR algorithm
        #  requires it. Investigate.
        self.exploration_config = {
            # The Exploration class to use. In the simplest case, this is the name
            # (str) of any class present in the `rllib.utils.exploration` package.
            # You can also provide the python class directly or the full location
            # of your class (e.g. "ray.rllib.utils.exploration.epsilon_greedy.
            # EpsilonGreedy").
            "type": "StochasticSampling",
            # Add constructor kwargs here (if any).
        }

    def training(
        self,
        *,
        weight_type: Optional[str] = NotProvided,
        temperature: Optional[float] = NotProvided,
        max_weight: Optional[float] = NotProvided,
        advantage_type: Optional[str] = NotProvided,
        n_action_sample: Optional[int] = NotProvided,
        twin_q: Optional[bool] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        actor_hiddens: Optional[List[int]] = NotProvided,
        actor_hidden_activation: Optional[str] = NotProvided,
        critic_hiddens: Optional[List[int]] = NotProvided,
        critic_hidden_activation: Optional[str] = NotProvided,
        tau: Optional[float] = NotProvided,
        td_error_loss_fn: Optional[str] = NotProvided,
        categorical_distribution_temperature: Optional[float] = NotProvided,
        **kwargs,
    ) -> "CRRConfig":

        r"""
        CRR training configuration

        Args:
            weight_type: weight type to use `bin` | `exp`.
            temperature: the exponent temperature used in exp weight type.
            max_weight: the max weight limit for exp weight type.
            advantage_type: The way we reduce q values to v_t values
                `max` | `mean` | `expectation`. `max` and `mean` work for both
                discrete and continuous action spaces while `expectation` only
                works for discrete action spaces.
                `max`: Uses max over sampled actions to estimate the value.

                .. math::

                    A(s_t, a_t) = Q(s_t, a_t) - \max_{a^j} Q(s_t, a^j)

                where :math:`a^j` is `n_action_sample` times sampled from the
                policy :math:`\pi(a | s_t)`
                `mean`: Uses mean over sampled actions to estimate the value.

                .. math::

                    A(s_t, a_t) = Q(s_t, a_t) - \frac{1}{m}\sum_{j=1}^{m}
                    [Q(s_t, a^j)]

                where :math:`a^j` is `n_action_sample` times sampled from the
                policy :math:`\pi(a | s_t)`
                `expectation`: This uses categorical distribution to evaluate
                the expectation of the q values directly to estimate the value.

                .. math::

                    A(s_t, a_t) = Q(s_t, a_t) - E_{a^j\sim \pi(a|s_t)}[Q(s_t,a^j)]

            n_action_sample: the number of actions to sample for v_t estimation.
            twin_q: if True, uses pessimistic q estimation.
            target_network_update_freq: The frequency at which we update the
                target copy of the model in terms of the number of gradient updates
                applied to the main model.
            actor_hiddens: The number of hidden units in the actor's fc network.
            actor_hidden_activation: The activation used in the actor's fc network.
            critic_hiddens: The number of hidden units in the critic's fc network.
            critic_hidden_activation: The activation used in the critic's fc network.
            tau: Polyak averaging coefficient
                (making it 1 is reduces it to a hard update).
            td_error_loss_fn: "huber" or "mse".
                Loss function for calculating critic error.
            categorical_distribution_temperature: Set the temperature parameter used
                by Categorical action distribution. A valid temperature is in the range
                of [0, 1]. Note that this mostly affects evaluation since critic error
                uses selected action for return calculation.
            **kwargs: forward compatibility kwargs

        Returns:
            This updated CRRConfig object.
        """
        super().training(**kwargs)

        if weight_type is not NotProvided:
            self.weight_type = weight_type
        if temperature is not NotProvided:
            self.temperature = temperature
        if max_weight is not NotProvided:
            self.max_weight = max_weight
        if advantage_type is not NotProvided:
            self.advantage_type = advantage_type
        if n_action_sample is not NotProvided:
            self.n_action_sample = n_action_sample
        if twin_q is not NotProvided:
            self.twin_q = twin_q
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if actor_hiddens is not NotProvided:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not NotProvided:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not NotProvided:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not NotProvided:
            self.critic_hidden_activation = critic_hidden_activation
        if tau is not NotProvided:
            self.tau = tau
        if td_error_loss_fn is not NotProvided:
            self.td_error_loss_fn = td_error_loss_fn
        if categorical_distribution_temperature is not NotProvided:
            self.categorical_distribution_temperature = (
                categorical_distribution_temperature
            )

        return self

    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        if self.td_error_loss_fn not in ["huber", "mse"]:
            raise ValueError("`td_error_loss_fn` must be 'huber' or 'mse'!")


NUM_GRADIENT_UPDATES = "num_grad_updates"


@Deprecated(
    old="rllib/algorithms/crr/",
    new="rllib_contrib/crr/",
    help=ALGO_DEPRECATION_WARNING,
    error=False,
)
class CRR(Algorithm):

    # TODO: we have a circular dependency for get
    #  default config. config -> Algorithm -> config
    #  defining Config class in the same file for now as a workaround.

    def setup(self, config: AlgorithmConfig):
        super().setup(config)

        self.target_network_update_freq = self.config.target_network_update_freq
        if self.target_network_update_freq is None:
            self.target_network_update_freq = self.config.train_batch_size * 100
        # added a counter key for keeping track of number of gradient updates
        self._counters[NUM_GRADIENT_UPDATES] = 0
        # if I don't set this here to zero I won't see zero in the logs (defaultdict)
        self._counters[NUM_TARGET_UPDATES] = 0

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfig:
        return CRRConfig()

    @classmethod
    @override(Algorithm)
    def get_default_policy_class(
        cls, config: AlgorithmConfig
    ) -> Optional[Type[Policy]]:
        if config["framework"] == "torch":
            from ray.rllib.algorithms.crr.torch import CRRTorchPolicy

            return CRRTorchPolicy
        else:
            raise ValueError("Non-torch frameworks are not supported yet!")

    @override(Algorithm)
    def training_step(self) -> ResultDict:
        with self._timers[SAMPLE_TIMER]:
            train_batch = synchronous_parallel_sample(worker_set=self.workers)
        train_batch = train_batch.as_multi_agent()
        self._counters[NUM_AGENT_STEPS_SAMPLED] += train_batch.agent_steps()
        self._counters[NUM_ENV_STEPS_SAMPLED] += train_batch.env_steps()

        # Postprocess batch before we learn on it.
        post_fn = self.config.get("before_learn_on_batch") or (lambda b, *a: b)
        train_batch = post_fn(train_batch, self.workers, self.config)

        # Learn on training batch.
        # Use simple optimizer (only for multi-agent or tf-eager; all other
        # cases should use the multi-GPU optimizer, even if only using 1 GPU)
        if self.config.get("simple_optimizer", False):
            train_results = train_one_step(self, train_batch)
        else:
            train_results = multi_gpu_train_one_step(self, train_batch)

        # update target every few gradient updates
        # Update target network every `target_network_update_freq` training steps.
        cur_ts = self._counters[
            NUM_AGENT_STEPS_TRAINED
            if self.config.count_steps_by == "agent_steps"
            else NUM_ENV_STEPS_TRAINED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]

        if cur_ts - last_update >= self.target_network_update_freq:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        self._counters[NUM_GRADIENT_UPDATES] += 1
        return train_results
