import time
from typing import TYPE_CHECKING

import numpy as np

import ray.experimental.internal_kv as internal_kv
from ray.util.collective.collective_group.base_collective_group import BaseGroup
from ray.util.collective.types import (
    AllGatherOptions,
    AllReduceOptions,
    BarrierOptions,
    BroadcastOptions,
    RecvOptions,
    ReduceOp,
    ReduceOptions,
    ReduceScatterOptions,
    SendOptions,
)

if TYPE_CHECKING:
    pass


def get_data_key(group_name: str, rank: int, op_name: str):
    return f"collective_mock_{group_name}_{op_name}_rank_{rank}"


def get_barrier_key(group_name: str, barrier_id: int):
    return f"collective_mock_{group_name}_barrier_{barrier_id}"


class MockInternalKVGroup(BaseGroup):
    def __init__(self, world_size: int, rank: int, group_name: str):
        super().__init__(world_size, rank, group_name)
        self._barrier_counter = 0

    @classmethod
    def backend(cls):
        return "MOCK"

    @classmethod
    def check_backend_availability(cls) -> bool:
        return True

    def _check_tensor_input(self, tensor):
        assert isinstance(tensor, list) and len(tensor) == 1
        t = tensor[0]
        if isinstance(t, np.ndarray):
            return t
        try:
            import torch

            if isinstance(t, torch.Tensor):
                return t
        except ImportError:
            pass
        raise ValueError(
            f"MockInternalKVGroup only accepts numpy.ndarray or torch.Tensor, received {type(t)}"
        )

    def _serialize_tensor(self, tensor):
        if isinstance(tensor, np.ndarray):
            return tensor.tobytes(), tensor.shape, tensor.dtype
        try:
            import torch

            if isinstance(tensor, torch.Tensor):
                return (
                    tensor.cpu().numpy().tobytes(),
                    tensor.shape,
                    tensor.cpu().numpy().dtype,
                )
        except ImportError:
            pass
        raise ValueError(f"Unsupported tensor type: {type(tensor)}")

    def _deserialize_tensor(self, data: bytes, shape, dtype, target_tensor):
        if isinstance(target_tensor, np.ndarray):
            np_array = np.frombuffer(data, dtype=dtype).reshape(shape)
            target_tensor[:] = np_array
        else:
            try:
                import torch

                if isinstance(target_tensor, torch.Tensor):
                    np_array = np.frombuffer(data, dtype=dtype).reshape(shape)
                    target_tensor.copy_(torch.from_numpy(np_array))
            except ImportError:
                pass

    def broadcast(self, tensor, broadcast_options=BroadcastOptions()):
        tensor = self._check_tensor_input(tensor)
        root_rank = broadcast_options.root_rank

        data_key = get_data_key(self._group_name, root_rank, "broadcast")

        if self._rank == root_rank:
            data, shape, dtype = self._serialize_tensor(tensor)
            internal_kv._internal_kv_put(data_key, data)
            internal_kv._internal_kv_put(f"{data_key}_shape", str(shape))
            internal_kv._internal_kv_put(f"{data_key}_dtype", dtype.name)
        else:
            deadline_s = time.time() + 30.0
            while True:
                data = internal_kv._internal_kv_get(data_key)
                if data is not None:
                    break
                if time.time() > deadline_s:
                    raise TimeoutError(
                        f"Timed out waiting for broadcast data from rank {root_rank}"
                    )
                time.sleep(0.01)

            # Also wait for metadata to be available
            deadline_s = time.time() + 30.0
            while True:
                shape_data = internal_kv._internal_kv_get(f"{data_key}_shape")
                dtype_data = internal_kv._internal_kv_get(f"{data_key}_dtype")
                if shape_data is not None and dtype_data is not None:
                    break
                if time.time() > deadline_s:
                    raise TimeoutError(
                        f"Timed out waiting for broadcast metadata from rank {root_rank}"
                    )
                time.sleep(0.01)

            shape_str = shape_data.decode()
            shape = eval(shape_str)
            dtype_name = dtype_data.decode()
            dtype = np.dtype(dtype_name)

            self._deserialize_tensor(data, shape, dtype, tensor)

    def allreduce(self, tensor, allreduce_options=AllReduceOptions()):
        tensor = self._check_tensor_input(tensor)
        reduce_op = allreduce_options.reduceOp

        data_key = get_data_key(self._group_name, self._rank, "allreduce")
        done_key = (
            f"collective_mock_{self._group_name}_allreduce_done_rank_{self._rank}"
        )

        data, shape, dtype = self._serialize_tensor(tensor)
        internal_kv._internal_kv_put(data_key, data)
        internal_kv._internal_kv_put(f"{data_key}_shape", str(shape))
        internal_kv._internal_kv_put(f"{data_key}_dtype", dtype.name)
        internal_kv._internal_kv_put(done_key, b"1")

        deadline_s = time.time() + 30.0
        while True:
            all_done = True
            for r in range(self._world_size):
                key = f"collective_mock_{self._group_name}_allreduce_done_rank_{r}"
                if internal_kv._internal_kv_get(key) is None:
                    all_done = False
                    break
            if all_done:
                break
            if time.time() > deadline_s:
                raise TimeoutError(
                    "Timed out waiting for allreduce data from all ranks"
                )
            time.sleep(0.01)

        result = None

        for r in range(self._world_size):
            rank_data_key = get_data_key(self._group_name, r, "allreduce")
            rank_data = internal_kv._internal_kv_get(rank_data_key)
            rank_shape_data = internal_kv._internal_kv_get(f"{rank_data_key}_shape")
            rank_dtype_data = internal_kv._internal_kv_get(f"{rank_data_key}_dtype")

            rank_shape_str = rank_shape_data.decode()
            rank_shape = eval(rank_shape_str)
            rank_dtype_name = rank_dtype_data.decode()
            rank_dtype = np.dtype(rank_dtype_name)

            if isinstance(tensor, np.ndarray):
                rank_tensor = np.frombuffer(rank_data, dtype=rank_dtype).reshape(
                    rank_shape
                )
            else:
                import torch

                rank_np = np.frombuffer(rank_data, dtype=rank_dtype).reshape(rank_shape)
                rank_tensor = torch.from_numpy(rank_np)

            if result is None:
                result = (
                    rank_tensor.copy()
                    if isinstance(rank_tensor, np.ndarray)
                    else rank_tensor.clone()
                )
            else:
                if reduce_op == ReduceOp.SUM:
                    result += rank_tensor
                elif reduce_op == ReduceOp.PRODUCT:
                    result *= rank_tensor
                elif reduce_op == ReduceOp.MAX:
                    if isinstance(result, np.ndarray):
                        result = np.maximum(result, rank_tensor)
                    else:
                        import torch

                        result = torch.maximum(result, rank_tensor)
                elif reduce_op == ReduceOp.MIN:
                    if isinstance(result, np.ndarray):
                        result = np.minimum(result, rank_tensor)
                    else:
                        import torch

                        result = torch.minimum(result, rank_tensor)

        if isinstance(tensor, np.ndarray):
            tensor[:] = result
        else:
            import torch

            if isinstance(result, np.ndarray):
                tensor.copy_(torch.from_numpy(result))
            else:
                tensor.copy_(result)

    def barrier(self, barrier_options=BarrierOptions()):
        barrier_id = self._barrier_counter
        barrier_key = get_barrier_key(self._group_name, barrier_id)
        rank_key = f"{barrier_key}_rank_{self._rank}"

        internal_kv._internal_kv_put(rank_key, b"1")

        deadline_s = time.time() + 30.0
        while True:
            all_arrived = True
            for r in range(self._world_size):
                key = f"{barrier_key}_rank_{r}"
                if internal_kv._internal_kv_get(key) is None:
                    all_arrived = False
                    break
            if all_arrived:
                break
            if time.time() > deadline_s:
                raise TimeoutError("Timed out waiting for barrier")
            time.sleep(0.01)

        self._barrier_counter += 1

    def reduce(self, tensor, reduce_options=ReduceOptions()):
        raise NotImplementedError("reduce is not implemented in MockInternalKVGroup")

    def allgather(self, tensor_list, tensor, allgather_options=AllGatherOptions()):
        raise NotImplementedError("allgather is not implemented in MockInternalKVGroup")

    def reducescatter(
        self, tensor, tensor_list, reducescatter_options=ReduceScatterOptions()
    ):
        raise NotImplementedError(
            "reducescatter is not implemented in MockInternalKVGroup"
        )

    def send(self, tensor, send_options: SendOptions):
        raise NotImplementedError("send is not implemented in MockInternalKVGroup")

    def recv(self, tensor, recv_options: RecvOptions):
        raise NotImplementedError("recv is not implemented in MockInternalKVGroup")
