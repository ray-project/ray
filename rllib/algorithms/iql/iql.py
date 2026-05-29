from typing import Optional, Type, Union

from ray.rllib.algorithms.algorithm_config import AlgorithmConfig, NotProvided
from ray.rllib.algorithms.marwil.marwil import MARWIL, MARWILConfig
from ray.rllib.connectors.common.add_observations_from_episodes_to_batch import (
    AddObservationsFromEpisodesToBatch,
)
from ray.rllib.connectors.learner.add_next_observations_from_episodes_to_train_batch import (  # noqa
    AddNextObservationsFromEpisodesToTrainBatch,
)
from ray.rllib.core.learner.learner import Learner
from ray.rllib.core.rl_module.rl_module import RLModuleSpec
from ray.rllib.utils.annotations import override
from ray.rllib.utils.typing import LearningRateOrSchedule, RLModuleSpecType


class IQLConfig(MARWILConfig):
    """Defines a configuration class from which a new IQL Algorithm can be built

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.iql import IQLConfig
        # Run this from the ray directory root.
        config = IQLConfig().training(actor_lr=0.00001, gamma=0.99)
        config = config.offline_data(
            input_="./rllib/offline/tests/data/pendulum/pendulum-v1_enormous")

        # Build an Algorithm object from the config and run 1 training iteration.
        algo = config.build()
        algo.train()

    .. testcode::
        :skipif: True

        from ray.rllib.algorithms.iql import IQLConfig
        from ray import tune
        config = IQLConfig()
        # Print out some default values.
        print(config.beta)
        # Update the config object.
        config.training(
            lr=tune.grid_search([0.001, 0.0001]), beta=0.75
        )
        # Set the config object's data path.
        # Run this from the ray directory root.
        config.offline_data(
            input_="./rllib/offline/tests/data/pendulum/pendulum-v1_enormous"
        )
        # Set the config object's env, used for evaluation.
        config.environment(env="Pendulum-v1")
        # Use to_dict() to get the old-style python config dict
        # when running with tune.
        tune.Tuner(
            "IQL",
            param_space=config.to_dict(),
        ).fit()
    """

    def __init__(self, algo_class=None):
        super().__init__(algo_class=algo_class or IQL)

        # fmt: off
        # __sphinx_doc_begin__
        # The temperature for the actor loss.
        self.beta = 0.1

        # The expectile to use in expectile regression.
        self.expectile = 0.8

        # The learning rates for the actor, critic and value network(s).
        self.actor_lr = 3e-4
        self.critic_lr = 3e-4
        self.value_lr = 3e-4
        # Set `lr` parameter to `None` and ensure it is not used.
        self.lr = None

        # If a twin-Q architecture should be used (advisable).
        self.twin_q = True

        # How often the target network should be updated.
        self.target_network_update_freq = 0
        # The weight for Polyak averaging.
        self.tau = 1.0

        # __sphinx_doc_end__
        # fmt: on

    @override(MARWILConfig)
    def training(
        self,
        *,
        twin_q: Optional[bool] = NotProvided,
        expectile: Optional[float] = NotProvided,
        actor_lr: Optional[LearningRateOrSchedule] = NotProvided,
        critic_lr: Optional[LearningRateOrSchedule] = NotProvided,
        value_lr: Optional[LearningRateOrSchedule] = NotProvided,
        target_network_update_freq: Optional[int] = NotProvided,
        tau: Optional[float] = NotProvided,
        **kwargs,
    ) -> "IQLConfig":
        """Sets the training related configuration.

        Args:
            beta: The temperature to scaling advantages in exponential terms.
                Must be >> 0.0. The higher this parameter the less greedy
                (exploitative) the policy becomes. It also means that the policy
                is fitting less to the best actions in the dataset.
            twin_q: If a twin-Q architecture should be used (advisable).
            expectile: The expectile to use in expectile regression for the value
                function. For high expectiles the value function tries to match
                the upper tail of the Q-value distribution.
            actor_lr: The learning rate for the actor network. Actor learning rates
                greater than critic learning rates work well in experiments.
            critic_lr: The learning rate for the Q-network. Critic learning rates
                greater than value function learning rates work well in experiments.
            value_lr: The learning rate for the value function network.
            target_network_update_freq: The number of timesteps in between the target
                Q-network is fixed. Note, too high values here could harm convergence.
                The target network is updated via Polyak-averaging.
            tau: The update parameter for Polyak-averaging of the target Q-network.
                The higher this value the faster the weights move towards the actual
                Q-network.

        Return:
            This updated `AlgorithmConfig` object.
        """
        super().training(**kwargs)

        if twin_q is not NotProvided:
            self.twin_q = twin_q
        if expectile is not NotProvided:
            self.expectile = expectile
        if actor_lr is not NotProvided:
            self.actor_lr = actor_lr
        if critic_lr is not NotProvided:
            self.critic_lr = critic_lr
        if value_lr is not NotProvided:
            self.value_lr = value_lr
        if target_network_update_freq is not NotProvided:
            self.target_network_update_freq = target_network_update_freq
        if tau is not NotProvided:
            self.tau = tau

        return self

    @override(MARWILConfig)
    def get_default_learner_class(self) -> Union[Type["Learner"], str]:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.iql.torch.iql_torch_learner import IQLTorchLearner

            return IQLTorchLearner
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `'torch'` instead."
            )

    @override(MARWILConfig)
    def get_default_rl_module_spec(self) -> RLModuleSpecType:
        if self.framework_str == "torch":
            from ray.rllib.algorithms.iql.torch.default_iql_torch_rl_module import (
                DefaultIQLTorchRLModule,
            )

            return RLModuleSpec(module_class=DefaultIQLTorchRLModule)
        else:
            raise ValueError(
                f"The framework {self.framework_str} is not supported. "
                "Use `torch` instead."
            )

    @override(MARWILConfig)
    def build_learner_connector(
        self,
        input_observation_space,
        input_action_space,
        device=None,
    ):
        pipeline = super().build_learner_connector(
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
            device=device,
        )

        # Remove unneeded connectors from the MARWIL connector pipeline.
        pipeline.remove("AddOneTsToEpisodesAndTruncate")
        pipeline.remove("GeneralAdvantageEstimation")

        # Prepend the "add-NEXT_OBS-from-episodes-to-train-batch" connector piece (right
        # after the corresponding "add-OBS-..." default piece).
        pipeline.insert_after(
            AddObservationsFromEpisodesToBatch,
            AddNextObservationsFromEpisodesToTrainBatch(),
        )

        return pipeline

    @override(MARWILConfig)
    def validate(self) -> None:
        # Call super's validation method.
        super().validate()

        # Ensure hyperparameters are meaningful.
        if self.beta <= 0.0:
            self._value_error(
                "For meaningful results, `beta` (temperature) parameter must be >> 0.0!"
            )
        if not 0.0 < self.expectile < 1.0:
            self._value_error(
                "For meaningful results, `expectile` parameter must be in (0, 1)."
            )

    @property
    def _model_config_auto_includes(self):
        return super()._model_config_auto_includes | {"twin_q": self.twin_q}


class IQL(MARWIL):
    """Implicit Q-learning (derived from MARWIL).

    Uses MARWIL training step.
    """

    @classmethod
    @override(MARWIL)
    def get_default_config(cls) -> AlgorithmConfig:
        return IQLConfig()
