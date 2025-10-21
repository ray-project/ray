from dataclasses import dataclass, fields
from typing import Callable, List, Optional, Union

import gymnasium as gym

from ray.rllib.connectors.connector_v2 import ConnectorV2
from ray.rllib.core.learner.differentiable_learner import DifferentiableLearner
from ray.rllib.core.rl_module.multi_rl_module import MultiRLModuleSpec
from ray.rllib.core.rl_module.rl_module import RLModule
from ray.rllib.utils.typing import DeviceType, ModuleID


@dataclass
class DifferentiableLearnerConfig:
    """Configures a `DifferentiableLearner`."""

    # TODO (simon): We implement only for `PyTorch`, so maybe we use here directly
    # TorchDifferentiableLearner` and check for this?
    # The `DifferentiableLearner` class. Must be derived from `DifferentiableLearner`.
    learner_class: Callable

    learner_connector: Optional[
        Callable[["RLModule"], Union["ConnectorV2", List["ConnectorV2"]]]
    ] = None

    add_default_connectors_to_learner_pipeline: bool = True

    is_multi_agent: bool = False

    policies_to_update: List[ModuleID] = None

    # The learning rate to use for the nested update. Note, in the default case this
    # learning rate is only used to update parameters in a functional form, i.e. the
    # `RLModule`'s stateful parameters are only updated in the `MetaLearner`. Different
    # logic can be implemented in customized `DifferentiableLearner`s.
    lr: float = 3e-5

    # TODO (simon): Add further hps like clip_grad, ...
    # The total number of minibatches to be formed from the batch per learner, e.g.
    # setting `train_batch_size_per_learner=10` and `num_total_minibatches` to 2
    # runs 2 SGD minibatch updates with a batch of 5 per training iteration.
    num_total_minibatches: int = 0

    # The number of epochs per training iteration.
    num_epochs: int = 1

    # The minibatch size per SGD minibatch update, e.g. with a `train_batch_size_per_learner=10`
    # and a `minibatch_size=2` the training step runs 5 SGD minibatch updates with minibatches
    # of 2.
    minibatch_size: int = None

    # If the batch should be shuffled between epochs.
    shuffle_batch_per_epoch: bool = False

    def __post_init__(self):
        """Additional initialization processes."""

        # Ensure we have a `DifferentiableLearner` class.
        if not issubclass(self.learner_class, DifferentiableLearner):
            raise ValueError(
                "`learner_class` must be a subclass of `DifferentiableLearner "
                f"but is {self.learner_class}."
            )

    def build_learner_connector(
        self,
        input_observation_space: Optional[gym.spaces.Space],
        input_action_space: Optional[gym.spaces.Space],
        device: Optional[DeviceType] = None,
    ):
        from ray.rllib.connectors.learner import (
            AddColumnsFromEpisodesToTrainBatch,
            AddObservationsFromEpisodesToBatch,
            AddStatesFromEpisodesToBatch,
            AddTimeDimToBatchAndZeroPad,
            AgentToModuleMapping,
            BatchIndividualItems,
            LearnerConnectorPipeline,
            NumpyToTensor,
        )

        custom_connectors = []
        # Create a learner connector pipeline (including RLlib's default
        # learner connector piece) and return it.
        if self.learner_connector is not None:
            val_ = self.learner_connector(
                input_observation_space,
                input_action_space,
                # device,  # TODO (sven): Also pass device into custom builder.
            )

            from ray.rllib.connectors.connector_v2 import ConnectorV2

            # ConnectorV2 (piece or pipeline).
            if isinstance(val_, ConnectorV2):
                custom_connectors = [val_]
            # Sequence of individual ConnectorV2 pieces.
            elif isinstance(val_, (list, tuple)):
                custom_connectors = list(val_)
            # Unsupported return value.
            else:
                raise ValueError(
                    "`AlgorithmConfig.training(learner_connector=..)` must return "
                    "a ConnectorV2 object or a list thereof (to be added to a "
                    f"pipeline)! Your function returned {val_}."
                )

        pipeline = LearnerConnectorPipeline(
            connectors=custom_connectors,
            input_observation_space=input_observation_space,
            input_action_space=input_action_space,
        )
        if self.add_default_connectors_to_learner_pipeline:
            # Append OBS handling.
            pipeline.append(
                AddObservationsFromEpisodesToBatch(as_learner_connector=True)
            )
            # Append all other columns handling.
            pipeline.append(AddColumnsFromEpisodesToTrainBatch())
            # Append time-rank handler.
            pipeline.append(AddTimeDimToBatchAndZeroPad(as_learner_connector=True))
            # Append STATE_IN/STATE_OUT handler.
            pipeline.append(AddStatesFromEpisodesToBatch(as_learner_connector=True))
            # If multi-agent -> Map from AgentID-based data to ModuleID based data.
            if self.is_multi_agent:
                pipeline.append(
                    AgentToModuleMapping(
                        rl_module_specs=(
                            self.rl_module_spec.rl_module_specs
                            if isinstance(self.rl_module_spec, MultiRLModuleSpec)
                            else set(self.policies)
                        ),
                        agent_to_module_mapping_fn=self.policy_mapping_fn,
                    )
                )
            # Batch all data.
            pipeline.append(BatchIndividualItems(multi_agent=self.is_multi_agent))
            # Convert to Tensors.
            pipeline.append(NumpyToTensor(as_learner_connector=True, device=device))
        return pipeline

    def update_from_kwargs(self, **kwargs):
        """Sets all slots with values defined in `kwargs`."""
        # Get all field names (i.e., slot names).
        field_names = {f.name for f in fields(self)}
        for key, value in kwargs.items():
            if key in field_names:
                setattr(self, key, value)
