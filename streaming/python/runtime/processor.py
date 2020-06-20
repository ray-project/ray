import logging
from abc import ABC, abstractmethod

import ray.streaming.context as context
from ray.streaming import message
from ray.streaming.operator import OperatorType

logger = logging.getLogger(__name__)


class Processor(ABC):
    """The base interface for all processors."""

    @abstractmethod
    def open(self, collectors, runtime_context):
        pass

    @abstractmethod
    def process(self, record: message.Record):
        pass

    @abstractmethod
    def close(self):
        pass


class StreamingProcessor(Processor, ABC):
    """StreamingProcessor is a process unit for a operator."""

    def __init__(self, operator):
        self.operator = operator
        self.collectors = None
        self.runtime_context = None

    def open(self, collectors, runtime_context: context.RuntimeContext):
        self.collectors = collectors
        self.runtime_context = runtime_context
        if self.operator is not None:
            self.operator.open(collectors, runtime_context)
        logger.info("Opened Processor {}".format(self))

    def close(self):
        pass


class SourceProcessor(StreamingProcessor):
    """Processor for :class:`ray.streaming.operator.SourceOperator` """

    def __init__(self, operator):
        super().__init__(operator)

    def process(self, record):
        raise Exception("SourceProcessor should not process record")

    def run(self):
        self.operator.run()


class OneInputProcessor(StreamingProcessor):
    """Processor for stream operator with one input"""

    def __init__(self, operator):
        super().__init__(operator)

    def process(self, record):
        self.operator.process_element(record)


class TwoInputProcessor(StreamingProcessor):
    """Processor for stream operator with two inputs"""

    def __init__(self, operator):
        super().__init__(operator)
        self.left_stream = None
        self.right_stream = None

    def process(self, record: message.Record):
        if record.stream == self.left_stream:
            self.operator.process_element(record, None)
        else:
            self.operator.process_element(None, record)

    @property
    def left_stream(self):
        return self.left_stream

    @left_stream.setter
    def left_stream(self, value):
        self._left_stream = value

    @property
    def right_stream(self):
        return self.right_stream

    @right_stream.setter
    def right_stream(self, value):
        self.right_stream = value


def build_processor(operator_instance):
    """Create a processor for the given operator."""
    operator_type = operator_instance.operator_type()
    logger.info(
        "Building StreamProcessor, operator type = {}, operator = {}.".format(
            operator_type, operator_instance))
    if operator_type == OperatorType.SOURCE:
        return SourceProcessor(operator_instance)
    elif operator_type == OperatorType.ONE_INPUT:
        return OneInputProcessor(operator_instance)
    elif operator_type == OperatorType.TWO_INPUT:
        return TwoInputProcessor(operator_instance)
    else:
        raise Exception("Current operator type is not supported")
