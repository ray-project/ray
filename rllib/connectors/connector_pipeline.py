from collections import defaultdict
import logging
from typing import Any, List, Union

from ray.rllib.connectors.connector import Connector, ConnectorContext
from ray.rllib.connectors.registry import get_connector
from ray.rllib.utils.annotations import override
from ray.util.annotations import PublicAPI
from ray.util.timer import _Timer

logger = logging.getLogger(__name__)


@PublicAPI(stability="alpha")
class ConnectorPipeline(Connector):
    """Utility class for quick manipulation of a connector pipeline."""

    def __init__(self, ctx: ConnectorContext, connectors: List[Connector]):
        assert len(connectors) > 0

        super().__init__(
            ctx,
            input_type=connectors[0].input_type,
            output_type=connectors[-1].output_type,
        )

        self.connectors = connectors
        self.timers = defaultdict(_Timer)

    # TODO (sven): deprecate
    def in_training(self):
        for c in self.connectors:
            c.in_training()

    # TODO (sven): deprecate
    def in_eval(self):
        for c in self.connectors:
            c.in_eval()

    def remove(self, name: str):
        """Remove a connector piece by <name>.

        Args:
            name: The name of the connector piece to be removed from the pipeline.
        """
        idx = -1
        for i, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                idx = i
                break
        if idx >= 0:
            del self.connectors[idx]
            self._fix_input_output_types()
            logger.info(f"Removed connector {name} from {self.__class__.__name__}.")
        else:
            logger.warning(f"Trying to remove a non-existent connector {name}.")

    def insert_before(self, name: str, connector: Connector):
        """Insert a new connector before connector <name>

        Args:
            name: name of the connector before which a new connector
                will get inserted.
            connector: a new connector to be inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                break
        if idx < 0:
            raise ValueError(f"Can not find connector {name}")
        self.connectors.insert(idx, connector)
        self._fix_input_output_types()

        logger.info(
            f"Inserted {connector.__class__.__name__} before {name} "
            f"to {self.__class__.__name__}."
        )

    def insert_after(self, name: str, connector: Connector):
        """Insert a new connector after connector <name>

        Args:
            name: name of the connector after which a new connector
                will get inserted.
            connector: a new connector to be inserted.
        """
        idx = -1
        for idx, c in enumerate(self.connectors):
            if c.__class__.__name__ == name:
                break
        if idx < 0:
            raise ValueError(f"Can not find connector {name}")
        self.connectors.insert(idx + 1, connector)
        self._fix_input_output_types()

        logger.info(
            f"Inserted {connector.__class__.__name__} after {name} "
            f"to {self.__class__.__name__}."
        )

    def prepend(self, connector: Connector):
        """Append a new connector at the beginning of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.insert(0, connector)
        self._fix_input_output_types()

        logger.info(
            f"Added {connector.__class__.__name__} to the beginning of "
            f"{self.__class__.__name__}."
        )

    def append(self, connector: Connector):
        """Append a new connector at the end of a connector pipeline.

        Args:
            connector: a new connector to be appended.
        """
        self.connectors.append(connector)
        self._fix_input_output_types()

        logger.info(
            f"Added {connector.__class__.__name__} to the end of "
            f"{self.__class__.__name__}."
        )

    @override(Connector)
    def __call__(self, input_: Any, ctx: ConnectorContext) -> Any:
        ret = input_
        for c in self.connectors:
            timer = self.timers[str(c)]
            with timer:
                ret = c(ret)
        return ret

    @override(Connector)
    def serialize(self):
        children = []
        for c in self.connectors:
            state = c.serialize()
            assert isinstance(state, tuple) and len(state) == 2, (
                "Serialized connector state must be in the format of "
                f"Tuple[name: str, params: Any]. Instead we got {state}"
                f"for connector {c.__name__}."
            )
            children.append(state)
        return ConnectorPipeline.__name__, children

    @override(Connector)
    @staticmethod
    def from_state(ctx: ConnectorContext, params: List[Any]):
        assert (
            type(params) == list
        ), "AgentConnectorPipeline takes a list of connector params."
        connectors = []
        for state in params:
            try:
                name, subparams = state
                connectors.append(get_connector(name, ctx, subparams))
            except Exception as e:
                logger.error(f"Failed to de-serialize connector state: {state}")
                raise e
        return ConnectorPipeline(ctx, connectors)

    def __str__(self, indentation: int = 0):
        return "\n".join(
            [" " * indentation + self.__class__.__name__]
            + [c.__str__(indentation + 4) for c in self.connectors]
        )

    def __getitem__(self, key: Union[str, int, type]):
        """Returns a list of connectors that fit 'key'.

        If key is a number n, we return a list with the nth element of this pipeline.
        If key is a Connector class or a string matching the class name of a
        Connector class, we return a list of all connectors in this pipeline matching
        the specified class.

        Args:
            key: The key to index by

        Returns: The Connector at index `key`.
        """
        # In case key is a class
        if not isinstance(key, str):
            if isinstance(key, slice):
                raise NotImplementedError(
                    "Slicing of ConnectorPipeline is currently not supported."
                )
            elif isinstance(key, int):
                return [self.connectors[key]]
            elif isinstance(key, type):
                results = []
                for c in self.connectors:
                    if issubclass(c.__class__, key):
                        results.append(c)
                return results
            else:
                raise NotImplementedError(
                    "Indexing by {} is currently not supported.".format(type(key))
                )

        results = []
        for c in self.connectors:
            if c.__class__.__name__ == key:
                results.append(c)

        return results

    def _fix_input_output_types(self):
        if len(self.connectors) > 0:
            self.input_type = self.connectors[0].input_type
            self.output_type = self.connectors[-1].output_type
        else:
            self.input_type = None
            self.output_type = None
