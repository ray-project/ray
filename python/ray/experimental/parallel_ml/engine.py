import logging
from collections import deque
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Any, Callable

import torch
from ray.experimental.parallel_ml.communicator.communicator import (
    FULLFILLED_FUTURE,
    Communicator,
)
from ray.experimental.parallel_ml.schedule import (
    Forward,
    Instruction,
    LoadBatch,
    PrintOutput,
    Receive,
    Schedule,
    Send,
)

logger = logging.getLogger(__name__)


@dataclass
class Config(object):
    """A config represents the configuration of a stage replica."""

    world_size: int
    rank: int
    input_tensor_shape: Any
    input_tensor_dtype: torch.Tensor.dtype
    device_name_builder: Callable[[], str]
    communicator_builder: Callable[[], Communicator]
    model_builder: Callable[[], torch.nn.Module]
    data_loader_builder: Callable[[], torch.utils.data.DataLoader]


class ExecutionEngine:
    """A stage replica engine represents a physical replica in the pipeline stage.
    It follows a precomputed schedule to schedule until reconfigured.
    """

    def __init__(self, schedule: Schedule, config: Config):
        self.input_queue = deque()
        self.output_queue = deque()
        self.schedule = schedule
        self.execution_lock = Lock()
        self.stop = False
        self.config = config
        self._initialize_config(config)

    def _initialize_config(self, config: Config):
        self.input_tensor_shape = config.input_tensor_shape
        self.input_tensor_dtype = config.input_tensor_dtype
        self.device = torch.device(config.device_name_builder())
        self.dist = config.communicator_builder(config.world_size, config.rank)
        self.model = config.model_builder().to(self.device).eval()
        self.data_loader = config.data_loader_builder()

    def start(self):
        """Start the engine execution"""
        self.thread = Thread(target=self._execute)
        self.thread.start()

    def stop(self):
        """Stop the engine if it's running."""
        with self.execution_lock:
            self.stop = True
        self.thread.join()

    def check_state(self):
        """Check the state of the engine."""
        pass

    def reconfigure(self, schedule: Schedule, config: Config):
        """Reconfgure the engine with a new schedule."""
        pass

    def _execute(self):
        with self.execution_lock:
            if self.stop:
                return
        for instructions in self.schedule.steps():
            for instruction in instructions:
                self._execute_step(instruction)

    def _execute_step(self, instruction: Instruction):
        logger.info(f"Executing instruction {instruction}")
        if isinstance(instruction, Send):
            for _ in range(instruction.count):
                self.dist.send(
                    self.output_queue.popleft(), instruction.dest_rank, async_op=True
                )
                # TODO: do we need to wait for the future to be completed?
        elif isinstance(instruction, Receive):
            for _ in range(instruction.count):
                tensor = torch.ones(()).new_empty(
                    size=self.input_tensor_shape,
                    dtype=self.input_tensor_dtype,
                    device=self.device,
                )
                future = self.dist.recv(tensor, instruction.src_rank, async_op=True)
                self.input_queue.append((tensor, future))
        elif isinstance(instruction, Forward):
            for _ in range(instruction.count):
                tensor, future = self.input_queue.popleft()
                future.wait()
                self.output_queue.append(self.model.forward(tensor))
        elif isinstance(instruction, LoadBatch):
            for _ in range(instruction.count):
                tensor = torch.ones(()).new_empty(
                    size=self.input_tensor_shape,
                    dtype=self.input_tensor_dtype,
                    device=self.device,
                )
                self.data_loader.next_batch(tensor)
                self.input_queue.append((tensor, FULLFILLED_FUTURE))
        elif isinstance(instruction, PrintOutput):
            logger.info(self.output_queue.popleft())
