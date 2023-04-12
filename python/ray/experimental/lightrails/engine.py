import logging
import socket
from collections import deque
from dataclasses import dataclass
from threading import Lock, Thread
from typing import Any, Callable

import torch
from ray.experimental.lightrails.communicator.communicator import (
    FULLFILLED_FUTURE,
    Communicator,
)
from ray.experimental.lightrails.schedule import (
    Backward,
    Forward,
    Instruction,
    LoadBatch,
    Optimize,
    PrintOutput,
    ReceiveActivation,
    ReceiveGradient,
    Schedule,
    SendActivation,
    SendGradient,
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
    communicator_builder: Callable[[int, int, str], Communicator]
    model_builder: Callable[[], torch.nn.Module]
    data_loader_builder: Callable[[], torch.utils.data.DataLoader]
    optimizer_builder: Callable[[torch.nn.Module], torch.optim.Optimizer]


class ExecutionEngine:
    """A stage replica engine represents a physical replica in the pipeline stage.
    It follows a precomputed schedule to schedule until reconfigured.
    """

    def __init__(
        self,
        schedule: Schedule,
        config: Config,
        is_training: bool = False,
        is_last_trainig_stage: bool = False,
    ):
        self.is_training = is_training
        self.is_last_trainig_stage = is_last_trainig_stage
        self.input_queue = deque()
        self.output_queue = deque()

        self.schedule = schedule
        self.execution_lock = Lock()
        self.stop = False
        self.config = config
        self.communicator_master_address = None

        # The following fields are only used for training
        # gradient if we are doing training.
        if self.is_training:
            self.input_gradient = deque()
            self.input_gradient_tensor_shape = None
            self.input_gradient_tensor_dtype = None
            self.output_gradient = deque()
            self.forward_cache = {}
            self.forward_counter = 0
            self.backward_counter = 0
            self.accumulated_parameters_gards = None
            self.label_queue = deque()

    def _initialize_config(self, config: Config):
        self.input_tensor_shape = config.input_tensor_shape
        self.input_tensor_dtype = config.input_tensor_dtype
        self.device = torch.device(config.device_name_builder())
        self.dist = config.communicator_builder(
            config.world_size, config.rank, self.communicator_master_address
        )
        self.model = config.model_builder().to(self.device)
        if not self.is_training:
            self.model.eval()
        data_loader = config.data_loader_builder()
        self.data_loader = iter(data_loader) if data_loader else None
        self.optimizer = config.optimizer_builder(self.model)

    def get_address(self):
        """Get the address of the engine."""
        return socket.gethostname()

    def start(self, master_address: str):
        """Start the engine execution"""
        self.communicator_master_address = master_address
        self._initialize_config(self.config)
        self.thread = Thread(target=self._execute)
        self.thread.start()

    def stop(self):
        """Stop the engine if it's running."""
        with self.execution_lock:
            self.stop = True
        self.thread.join()

    def wait_until_stopped(self):
        """Stop the engine if it's running."""
        self.thread.join()

    def check_state(self):
        """Check the state of the engine."""
        pass

    def reconfigure(self, schedule: Schedule, config: Config):
        """Reconfgure the engine with a new schedule."""
        pass

    def _execute(self):
        for instructions in self.schedule.steps():
            with self.execution_lock:
                if self.stop:
                    return
            for instruction in instructions:
                self._execute_step(instruction)

    def _execute_step(self, instruction: Instruction):
        logger.debug(f"Executing instruction {instruction}")
        if isinstance(instruction, SendActivation):
            self._execute_send_activation(instruction)
        elif isinstance(instruction, ReceiveActivation):
            self._execute_receive_activation(instruction)
        elif isinstance(instruction, Forward):
            self._execute_forward(instruction)
        elif isinstance(instruction, PrintOutput):
            self._execute_print_output(instruction)
        elif isinstance(instruction, LoadBatch):
            self._execute_load_batch(instruction)
        elif isinstance(instruction, SendGradient):
            self._execute_send_gradient(instruction)
        elif isinstance(instruction, ReceiveGradient):
            self._execute_receive_gradient(instruction)
        elif isinstance(instruction, Optimize):
            self._execute_optimize(instruction)
        elif isinstance(instruction, Backward):
            self._execute_backward(instruction)

    def _execute_send_activation(self, instruction: SendActivation):
        for _ in range(instruction.count):
            self.dist.send(
                self.output_queue.popleft(), instruction.dest_rank, async_op=True
            )
            # TODO: do we need to wait for the future to be completed?

    def _execute_receive_activation(self, instruction: ReceiveActivation):
        for _ in range(instruction.count):
            tensor = torch.ones(()).new_empty(
                size=self.input_tensor_shape,
                dtype=self.input_tensor_dtype,
                device=self.device,
            )
            future = self.dist.recv(tensor, instruction.src_rank, async_op=True)
            self.input_queue.append((tensor, future))

    def _execute_forward(self, instruction: Forward):
        for _ in range(instruction.count):
            tensor, future = self.input_queue.popleft()
            future.wait()
            output = self.model.forward(tensor)
            logger.debug(
                f"step: {self.forward_counter}, input: {tensor}, output: {output}"
            )

            # Optionally compute loss on the last device
            if self.is_last_trainig_stage:
                if hasattr(self.model, "loss_fn"):
                    label, future = self.label_queue.popleft()
                    future.wait()
                    logger.debug(
                        f"step:{self.forward_counter} label: {label}, output: {output}"
                    )
                    output = self.model.loss_fn(output, label)
                else:
                    # Some models just return loss from forward()
                    pass
                print(f"step: {self.forward_counter}, loss: {output}")

            if self.is_training:
                if self.forward_counter == 0:
                    self.received_gradient_tensor_shape = output.shape
                    self.received_gradient_tensor_dtype = output.dtype
                self.forward_cache[self.forward_counter] = (tensor, output)
                self.forward_counter += 1

            # Only send the output if we are not the last training stage
            if not self.is_last_trainig_stage:
                self.output_queue.append(output)

    def _execute_load_batch(self, instruction: LoadBatch):
        for _ in range(instruction.count):
            if instruction.is_label:
                _, label = next(self.data_loader)
                logger.debug(
                    f"loading label, step: {self.forward_counter}, label: {label}"
                )
                self.label_queue.append((label.to(self.device), FULLFILLED_FUTURE))
            else:
                batch, _ = next(self.data_loader)
                logger.debug(
                    f"loading value, step: {self.forward_counter}, value: {batch}"
                )
                self.input_queue.append((batch.to(self.device), FULLFILLED_FUTURE))

    def _execute_print_output(self, instruction: PrintOutput):
        for _ in range(instruction.count):
            logger.info(self.output_queue.popleft())

    def _execute_send_gradient(self, instruction: SendGradient):
        for _ in range(instruction.count):
            self.dist.send(
                self.output_gradient.popleft(), instruction.dest_rank, async_op=True
            )

    def _execute_receive_gradient(self, instruction: ReceiveGradient):
        for _ in range(instruction.count):
            tensor = torch.ones(()).new_empty(
                size=self.received_gradient_tensor_shape,
                dtype=self.received_gradient_tensor_dtype,
                device=self.device,
            )
            future = self.dist.recv(tensor, instruction.src_rank, async_op=True)
            self.input_gradient.append((tensor, future))

    def _execute_optimize(self, instruction: Optimize):
        # TODO: this probably needs to be changed

        # overwrite the gradients in the model with the accumulated gradients
        for i, parameter in enumerate(self.model.parameters()):
            parameter.grad = self.accumulated_parameters_gards[i]

        self.optimizer.step()
        self.optimizer.zero_grad()
        self.accumulated_parameters_gards = None

    def _execute_backward(self, instruction: Backward):
        for _ in range(instruction.count):
            input, output = self.forward_cache.pop(self.backward_counter)
            input.requires_grad_()
            input.retain_grad()

            # last stage the output is loss.
            if self.is_last_trainig_stage:
                output.backward()
            else:
                tensor, future = self.input_gradient.popleft()
                future.wait()
                output.backward(tensor)

            tmp = []
            # accumulate the gradients into self.accumulated_parameters_gards
            for i, parameter in enumerate(self.model.parameters()):
                if self.accumulated_parameters_gards:
                    self.accumulated_parameters_gards[i] += parameter.grad
                else:
                    tmp.append(parameter.grad)
            if tmp:
                self.accumulated_parameters_gards = tmp

            self.output_gradient.append(input.grad)
            # TODO: do we need to do something for optimize?
            self.backward_counter += 1
