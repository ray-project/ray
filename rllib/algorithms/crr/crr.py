import logging
from typing import List, Optional, Type

from ray.rllib.algorithms.algorithm import Algorithm, AlgorithmConfig
from ray.rllib.execution import synchronous_parallel_sample
from ray.rllib.execution.train_ops import multi_gpu_train_one_step, train_one_step
from ray.rllib.policy import Policy
from ray.rllib.utils.annotations import override
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
from ray.rllib.utils.typing import (
    AlgorithmConfigDict,
    PartialAlgorithmConfigDict,
    ResultDict,
)

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

        # Overriding the trainer config default:
        # Only PyTorch supported thus far. Make this the default framework.
        self.framework_str = "torch"
        # If data ingestion/sample_time is slow, increase this
        self.num_workers = 4
        self.offline_sampling = True
        self.min_time_s_per_iteration = 10.0

    def training(
        self,
        *,
        weight_type: Optional[str] = None,
        temperature: Optional[float] = None,
        max_weight: Optional[float] = None,
        advantage_type: Optional[str] = None,
        n_action_sample: Optional[int] = None,
        twin_q: Optional[bool] = None,
        target_network_update_freq: Optional[int] = None,
        actor_hiddens: Optional[List[int]] = None,
        actor_hidden_activation: Optional[str] = None,
        critic_hiddens: Optional[List[int]] = None,
        critic_hidden_activation: Optional[str] = None,
        tau: Optional[float] = None,
        **kwargs,
    ) -> "CRRConfig":

        """
        === CRR configs

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
                where :math:a^j is `n_action_sample` times sampled from the
                policy :math:\pi(a | s_t)
                `mean`: Uses mean over sampled actions to estimate the value.
                .. math::
                    A(s_t, a_t) = Q(s_t, a_t) - \frac{1}{m}\sum_{j=1}^{m}[Q
                    (s_t, a^j)]
                where :math:a^j is `n_action_sample` times sampled from the
                policy :math:\pi(a | s_t)
                `expectation`: This uses categorical distribution to evaluate
                the expectation of the q values directly to estimate the value.
                .. math::
                    A(s_t, a_t) = Q(s_t, a_t) - E_{a^j\sim \pi(a|s_t)}[Q(s_t,
                    a^j)]
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
            **kwargs: forward compatibility kwargs

        Returns:
            This updated CRRConfig object.
        """
        super().training(**kwargs)

        if weight_type is not None:
            self.weight_type = weight_type
        if temperature is not None:
            self.temperature = temperature
        if max_weight is not None:
            self.max_weight = max_weight
        if advantage_type is not None:
            self.advantage_type = advantage_type
        if n_action_sample is not None:
            self.n_action_sample = n_action_sample
        if twin_q is not None:
            self.twin_q = twin_q
        if target_network_update_freq is not None:
            self.target_network_update_freq = target_network_update_freq
        if actor_hiddens is not None:
            self.actor_hiddens = actor_hiddens
        if actor_hidden_activation is not None:
            self.actor_hidden_activation = actor_hidden_activation
        if critic_hiddens is not None:
            self.critic_hiddens = critic_hiddens
        if critic_hidden_activation is not None:
            self.critic_hidden_activation = critic_hidden_activation
        if tau is not None:
            self.tau = tau

        return self


NUM_GRADIENT_UPDATES = "num_grad_updates"


class CRR(Algorithm):

    # TODO: we have a circular dependency for get
    #  default config. config -> Trainer -> config
    #  defining Config class in the same file for now as a workaround.

    def setup(self, config: PartialAlgorithmConfigDict):
        super().setup(config)
        if self.config.get("target_network_update_freq", None) is None:
            self.config["target_network_update_freq"] = (
                self.config["train_batch_size"] * 100
            )
        # added a counter key for keeping track of number of gradient updates
        self._counters[NUM_GRADIENT_UPDATES] = 0
        # if I don't set this here to zero I won't see zero in the logs (defaultdict)
        self._counters[NUM_TARGET_UPDATES] = 0

    @classmethod
    @override(Algorithm)
    def get_default_config(cls) -> AlgorithmConfigDict:
        return CRRConfig().to_dict()

    @override(Algorithm)
    def get_default_policy_class(self, config: AlgorithmConfigDict) -> Type[Policy]:
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
            NUM_AGENT_STEPS_TRAINED if self._by_agent_steps else NUM_ENV_STEPS_TRAINED
        ]
        last_update = self._counters[LAST_TARGET_UPDATE_TS]

        if cur_ts - last_update >= self.config["target_network_update_freq"]:
            with self._timers[TARGET_NET_UPDATE_TIMER]:
                to_update = self.workers.local_worker().get_policies_to_train()
                self.workers.local_worker().foreach_policy_to_train(
                    lambda p, pid: pid in to_update and p.update_target()
                )
            self._counters[NUM_TARGET_UPDATES] += 1
            self._counters[LAST_TARGET_UPDATE_TS] = cur_ts

        self._counters[NUM_GRADIENT_UPDATES] += 1
        return train_results
