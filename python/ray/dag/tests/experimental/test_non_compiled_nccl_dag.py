# coding: utf-8
import os
import sys

import pytest

from ray.experimental.channel.torch_tensor_type import TorchTensorType
from ray.tests.conftest import *  # noqa


def test_non_compiled_nccl_falls_back_to_cpu(ray_start_regular_shared):
    """
    Test that TorchTensorType(transport='accelerator') in a non-compiled DAG
    falls back to CPU/shared-memory transport with a warning.
    """
    t = TorchTensorType(transport="accelerator")

    with pytest.warns(
        UserWarning, match="Falling back to shared-memory \\(CPU\\) transport"
    ):
        channel = t.create_channel(
            writer=None,
            reader_and_node_list=[],
        )

    from ray.experimental.channel.shared_memory_channel import CompositeChannel

    assert isinstance(channel, CompositeChannel)


def test_non_compiled_custom_communicator_falls_back_to_cpu(ray_start_regular_shared):
    """
    Test that TorchTensorType(transport=custom_comm) in a non-compiled DAG
    falls back to CPU/shared-memory transport with a warning because communicator_id is None.
    """
    from ray.experimental.channel.communicator import Communicator

    # Register as virtual subclass of Communicator to satisfy isinstance check
    # without running into abstract method instantiation TypeErrors.
    @Communicator.register
    class DummyCommunicator:
        def get_transport_name(self) -> str:
            return "accelerator"

    t = TorchTensorType(transport=DummyCommunicator())

    with pytest.warns(
        UserWarning, match="Falling back to shared-memory \\(CPU\\) transport"
    ):
        channel = t.create_channel(
            writer=None,
            reader_and_node_list=[],
        )

    from ray.experimental.channel.shared_memory_channel import CompositeChannel

    assert isinstance(channel, CompositeChannel)


if __name__ == "__main__":
    if os.environ.get("PARALLEL_CI"):
        sys.exit(pytest.main(["-n", "auto", "--boxed", "-vs", __file__]))
    else:
        sys.exit(pytest.main(["-sv", __file__]))
