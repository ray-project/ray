from typing import Any, List

import numpy as np
import tree
from gymnasium.spaces import Discrete, MultiDiscrete

from ray.rllib.connectors.agent.synced_filter import SyncedFilterAgentConnector
from ray.rllib.connectors.connector import (
    AgentConnector,
    ConnectorContext,
)
from ray.rllib.connectors.registry import register_connector
from ray.rllib.policy.sample_batch import SampleBatch
from ray.rllib.utils.annotations import OldAPIStack
from ray.rllib.utils.filter import (
    ConcurrentMeanStdFilter,
    Filter,
    MeanStdFilter,
    RunningStat,
)
from ray.rllib.utils.spaces.space_utils import get_base_struct_from_space
from ray.rllib.utils.typing import AgentConnectorDataType


@OldAPIStack
class MeanStdObservationFilterAgentConnector(SyncedFilterAgentConnector):
    """A connector used to mean-std-filter observations.

    Incoming observations are filtered such that the output of this filter is on
    average zero and has a standard deviation of 1. This filtering is applied
    separately per element of the observation space.
    """

    def __init__(
        self,
        ctx: ConnectorContext,
        demean: bool = True,
        destd: bool = True,
        clip: float = 10.0,
    ):
        SyncedFilterAgentConnector.__init__(self, ctx)
        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code

        filter_shape = tree.map_structure(
            lambda s: (
                None
                if isinstance(s, (Discrete, MultiDiscrete))  # noqa
                else np.array(s.shape)
            ),
            get_base_struct_from_space(ctx.observation_space),
        )
        self.filter = MeanStdFilter(filter_shape, demean=demean, destd=destd, clip=clip)

    def transform(self, ac_data: AgentConnectorDataType) -> AgentConnectorDataType:
        d = ac_data.data
        assert (
            type(d) is dict
        ), "Single agent data must be of type Dict[str, TensorStructType]"
        if SampleBatch.OBS in d:
            d[SampleBatch.OBS] = self.filter(
                d[SampleBatch.OBS], update=self._is_training
            )
        if SampleBatch.NEXT_OBS in d:
            d[SampleBatch.NEXT_OBS] = self.filter(
                d[SampleBatch.NEXT_OBS], update=self._is_training
            )

        return ac_data

    def to_state(self):
        # Flattening is deterministic
        flattened_rs = tree.flatten(self.filter.running_stats)
        flattened_buffer = tree.flatten(self.filter.buffer)
        return MeanStdObservationFilterAgentConnector.__name__, {
            "shape": self.filter.shape,
            "no_preprocessor": self.filter.no_preprocessor,
            "demean": self.filter.demean,
            "destd": self.filter.destd,
            "clip": self.filter.clip,
            "running_stats": [s.to_state() for s in flattened_rs],
            "buffer": [s.to_state() for s in flattened_buffer],
        }

    # demean, destd, clip, and a state dict
    @staticmethod
    def from_state(
        ctx: ConnectorContext,
        params: List[Any] = None,
        demean: bool = True,
        destd: bool = True,
        clip: float = 10.0,
    ):
        connector = MeanStdObservationFilterAgentConnector(ctx, demean, destd, clip)
        if params:
            connector.filter.shape = params["shape"]
            connector.filter.no_preprocessor = params["no_preprocessor"]
            connector.filter.demean = params["demean"]
            connector.filter.destd = params["destd"]
            connector.filter.clip = params["clip"]

            # Unflattening is deterministic
            running_stats = [RunningStat.from_state(s) for s in params["running_stats"]]
            connector.filter.running_stats = tree.unflatten_as(
                connector.filter.shape, running_stats
            )

            # Unflattening is deterministic
            buffer = [RunningStat.from_state(s) for s in params["buffer"]]
            connector.filter.buffer = tree.unflatten_as(connector.filter.shape, buffer)

        return connector

    def reset_state(self) -> None:
        """Creates copy of current state and resets accumulated state"""
        if not self._is_training:
            raise ValueError(
                "State of {} can only be changed when trainin.".format(self.__name__)
            )
        self.filter.reset_buffer()

    def apply_changes(self, other: "Filter", *args, **kwargs) -> None:
        """Updates self with state from other filter."""
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        if not self._is_training:
            raise ValueError(
                "Changes can only be applied to {} when trainin.".format(self.__name__)
            )
        return self.filter.apply_changes(other, *args, **kwargs)

    def copy(self) -> "Filter":
        """Creates a new object with same state as self.

        This is a legacy Filter method that we need to keep around for now

        Returns:
            A copy of self.
        """
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        return self.filter.copy()

    def sync(self, other: "AgentConnector") -> None:
        """Copies all state from other filter to self."""
        # inline this as soon as we deprecate ordinary filter with non-connector
        # env_runner
        if not self._is_training:
            raise ValueError(
                "{} can only be synced when trainin.".format(self.__name__)
            )
        return self.filter.sync(other.filter)


@OldAPIStack
class ConcurrentMeanStdObservationFilterAgentConnector(
    MeanStdObservationFilterAgentConnector
):
    """A concurrent version of the MeanStdObservationFilterAgentConnector.

    This version's filter has all operations wrapped by a threading.RLock.
    It can therefore be safely used by multiple threads.
    """

    def __init__(self, ctx: ConnectorContext, demean=True, destd=True, clip=10.0):
        SyncedFilterAgentConnector.__init__(self, ctx)
        # We simply use the old MeanStdFilter until non-connector env_runner is fully
        # deprecated to avoid duplicate code

        filter_shape = tree.map_structure(
            lambda s: (
                None
                if isinstance(s, (Discrete, MultiDiscrete))  # noqa
                else np.array(s.shape)
            ),
            get_base_struct_from_space(ctx.observation_space),
        )
        self.filter = ConcurrentMeanStdFilter(
            filter_shape, demean=True, destd=True, clip=10.0
        )


register_connector(
    MeanStdObservationFilterAgentConnector.__name__,
    MeanStdObservationFilterAgentConnector,
)
register_connector(
    ConcurrentMeanStdObservationFilterAgentConnector.__name__,
    ConcurrentMeanStdObservationFilterAgentConnector,
)
