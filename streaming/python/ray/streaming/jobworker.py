from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import pickle
import threading

import ray
import ray.streaming.queue.streaming_queue as streaming_queue
from ray.function_manager import FunctionDescriptor
from ray.streaming.communication import DataInput, DataOutput
from ray.streaming.config import Config
from ray.streaming.queue.memory_queue import MemQueueLinkImpl

logger = logging.getLogger(__name__)


@ray.remote
class JobWorker(object):
    """A streaming job worker.

    Attributes:
        worker_id: The id of the instance.
        input_channels: The input gate that manages input channels of
        the instance (see: DataInput in communication.py).
        output_channels (DataOutput): The output gate that manages output channels of
        the instance (see: DataOutput in communication.py).
        the operator instance.
    """
    def __init__(self, worker_id, operator,
                 input_channels, output_channels):
        print("OperatorInstance worker_id", worker_id)
        self.env = None
        self.worker_id = worker_id
        self.operator = operator
        processor_instance = operator.processor_class(operator)
        self.processor_instance = processor_instance
        self.queue_link = None
        self.input_channels = input_channels
        self.output_channels = output_channels
        self.input_gate = None
        self.output_gate = None

    def init(self, env):
        """init streaming actor"""
        env = pickle.loads(env)
        logger.info("init operator instance %s", self.__class__.__name__)
        self.env = env
        if self.env.config.queue_type == Config.MEMORY_QUEUE:
            self.queue_link = MemQueueLinkImpl()
        else:
            sync_func = FunctionDescriptor(__name__, self.on_streaming_transfer_sync.__name__,
                                           self.__class__.__name__)
            async_func = FunctionDescriptor(__name__, self.on_streaming_transfer.__name__,
                                            self.__class__.__name__)
            self.queue_link = streaming_queue.QueueLinkImpl(sync_func, async_func)
        runtime_conf = {Config.TASK_JOB_ID: ray.runtime_context._get_runtime_context().current_driver_id}
        self.queue_link.set_ray_runtime(runtime_conf)
        if len(self.input_channels) > 0:
            self.input_gate = DataInput(env, self.queue_link, self.input_channels)
            self.input_gate.init()
        if len(self.output_channels) > 0:
            self.output_gate = DataOutput(env, self.queue_link, self.output_channels,
                                          self.operator.partitioning_strategies)
            self.output_gate.init()
        logger.info("init operator instance %s succeed", self.__class__.__name__)
        return True

    # Starts the actor
    def start(self):
        self.t = threading.Thread(target=self.run, daemon=True)
        self.t.start()
        processor_name = self.operator.processor_class.__name__
        actor_id = ray.worker.global_worker.actor_id
        logger.info("%s %s started, actor id %s", self.__class__.__name__, processor_name, actor_id)
        # self.t.join()

    def run(self):
        logger.info("start running")
        self.processor_instance.run(self.input_gate, self.output_gate)
        self.close()

    def close(self):
        if self.input_gate:
            self.input_gate.close()
        if self.output_gate:
            self.output_gate.close()

    def is_finished(self):
        return self.t.is_alive()

    def on_streaming_transfer(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        self.queue_link.on_streaming_transfer(buffer.to_pybytes())

    def on_streaming_transfer_sync(self, buffer: ray._raylet.Buffer):
        """used in direct call mode"""
        if self.queue_link is None:
            return b' '*4  # special flag to indicate this actor not ready
        result = self.queue_link.on_streaming_transfer_sync(buffer.to_pybytes())
        return result
