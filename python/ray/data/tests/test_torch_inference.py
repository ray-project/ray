import logging
from dataclasses import replace
from typing import Any

import numpy as np
import pandas as pd
import pytest
import torch

import ray
from ray.data._internal.logical.operators.map_operator import MapBatches
from ray.data._internal.utils.torch_inference import (
    is_torch_inference_class,
    is_torch_inference_instance,
    make_torch_inference_callable,
)
from ray.data._internal.utils.torch_utils import find_first_tensor_not_on_device
from ray.data.tests.conftest import *  # noqa
from ray.data.util.torch_inference import TorchInference
from ray.tests.conftest import *  # noqa


class MyInferenceActor(TorchInference):
    # pyrefly: ignore[bad-override]  # narrowing `*args/**kwargs` is the API.
    def initialize(self):
        self.scale = 2.0

    def get_device(self):
        return torch.device("cuda")

    def process_on_device(self, input_batch, collated_tensors, collated_other):
        return {"y": collated_tensors["x"] * self.scale}


class PlainCallableActor:
    def __call__(self, batch):
        return batch


def _make_input_ds(n=32):
    return ray.data.range(n).map_batches(
        lambda b: {"x": b["id"].astype(np.float32)}, batch_size=None
    )


# ===== Base class defaults =====


def test_init_calls_initialize():
    actor = MyInferenceActor()
    assert actor.scale == 2.0


def test_default_collate_converts_numpy_batch():
    actor = MyInferenceActor()
    batch = {
        "x": np.arange(6, dtype=np.float32).reshape(3, 2),
        "id": np.arange(3, dtype=np.int64),
    }
    tensors = actor.collate(batch)
    assert isinstance(tensors, dict)
    assert set(tensors) == {"x", "id"}
    x, ids = tensors["x"], tensors["id"]
    assert isinstance(x, torch.Tensor) and isinstance(ids, torch.Tensor)
    assert torch.equal(x, torch.as_tensor(batch["x"]))
    assert x.dtype == torch.float32
    assert ids.dtype == torch.int64


@pytest.mark.parametrize(
    "bad_batch",
    [
        pd.DataFrame({"x": [1.0, 2.0]}),
        [1, 2, 3],
        {"x": [1.0, 2.0]},
        {"x": torch.zeros(2)},
    ],
)
def test_default_collate_rejects_non_numpy_batch(bad_batch):
    actor = MyInferenceActor()
    with pytest.raises(TypeError, match="override `collate`"):
        actor.collate(bad_batch)


def test_default_finalize_converts_tensor_mapping():
    actor = MyInferenceActor()
    out = actor.finalize({}, {"y": torch.arange(3, dtype=torch.float32)}, None)
    assert isinstance(out["y"], np.ndarray)
    assert np.array_equal(out["y"], np.arange(3, dtype=np.float32))


def test_default_finalize_converts_recursively():
    # The conversion preserves the dict/sequence structure and converts every
    # tensor leaf. (Annotated Any: the nesting is deeper than the declared
    # `TensorBatchType`, which the recursive default supports at runtime.)
    actor = MyInferenceActor()
    nested_output: Any = {
        "flat": torch.zeros(2),
        "chunks": [torch.zeros(2), torch.ones(2)],
        "nested": {"inner": (torch.arange(3),)},
    }
    out = actor.finalize({}, nested_output, None)
    assert isinstance(out["flat"], np.ndarray)
    assert isinstance(out["chunks"], list)
    assert all(isinstance(chunk, np.ndarray) for chunk in out["chunks"])
    assert np.array_equal(out["chunks"][1], np.ones(2))
    assert isinstance(out["nested"]["inner"], tuple)
    assert np.array_equal(out["nested"]["inner"][0], np.arange(3))


@pytest.mark.parametrize(
    "bad_output",
    [
        "not tensors",
        {"y": "not a tensor"},
        {"y": np.zeros(2)},
        {"y": [torch.zeros(2), None]},
    ],
)
def test_default_finalize_rejects_non_tensor_leaves(bad_output):
    actor = MyInferenceActor()
    with pytest.raises(TypeError, match="Override `finalize`"):
        actor.finalize({}, bad_output, None)


def test_default_finalize_rejects_output_other():
    # The default can't know how to fold side data into the batch.
    actor = MyInferenceActor()
    with pytest.raises(TypeError, match="output_other"):
        actor.finalize({}, {"y": torch.zeros(2)}, {"lengths": [1, 2]})


def test_split_batch_and_other():
    from ray.data._internal.utils.torch_inference import split_batch_and_other

    tensors = {"x": torch.zeros(2)}
    # Bare TensorBatchType -> no side data.
    assert split_batch_and_other(tensors) == (tensors, None)
    # A 2-tuple always means (tensors, other) — even when `other` is a
    # tensor. A batch that IS a pair of tensors must be a list instead.
    assert split_batch_and_other((tensors, {"dims": (2, 3)})) == (
        tensors,
        {"dims": (2, 3)},
    )
    assert split_batch_and_other((tensors, None)) == (tensors, None)
    t1, t2 = torch.zeros(2), torch.ones(2)
    assert split_batch_and_other((t1, t2)) == (t1, t2)
    # Non-2-tuples are never split.
    triple = (t1, t2, torch.zeros(2))
    assert split_batch_and_other(triple) == (triple, None)
    assert split_batch_and_other([t1, t2]) == ([t1, t2], None)


def test_default_get_device():
    class DefaultDevice(TorchInference):
        def process_on_device(self, input_batch, collated_tensors, collated_other):
            return collated_tensors

    actor = DefaultDevice()
    if torch.cuda.is_available():
        assert actor.get_device() == torch.device("cuda")
    else:
        with pytest.raises(RuntimeError, match="CUDA is not available"):
            actor.get_device()


def test_process_on_device_not_implemented():
    class NoProcess(TorchInference):
        pass

    with pytest.raises(NotImplementedError, match="process_on_device"):
        NoProcess().process_on_device({}, {}, None)


# ===== Detection =====


def test_detection():
    assert is_torch_inference_class(MyInferenceActor)
    assert is_torch_inference_class(TorchInference)
    assert not is_torch_inference_class(PlainCallableActor)
    assert not is_torch_inference_class(lambda b: b)
    assert not is_torch_inference_class("not a class")
    # Instances don't match the class check, but match the instance check.
    actor = MyInferenceActor()
    assert not is_torch_inference_class(actor)
    assert is_torch_inference_instance(actor)
    assert not is_torch_inference_instance(MyInferenceActor)
    assert not is_torch_inference_instance(PlainCallableActor())


def test_wrapper_not_detected_and_keeps_name():
    wrapper_cls = make_torch_inference_callable(MyInferenceActor)
    # Composition: the wrapper is not a subclass, so plan rewrites re-running
    # __post_init__ can't wrap it again.
    assert not is_torch_inference_class(wrapper_cls)
    assert wrapper_cls.__name__ == "MyInferenceActor"


def test_wrapper_rejects_non_cuda_device():
    # The device comes from the instance's `get_device()`, so the GPU-only
    # check happens at actor init — before any CUDA state is touched, so it
    # is testable without a GPU.
    class CpuDevice(MyInferenceActor):
        def get_device(self):
            return torch.device("cpu")

    wrapper_cls = make_torch_inference_callable(CpuDevice)
    with pytest.raises(ValueError, match="must return a CUDA device"):
        wrapper_cls()


# ===== Tensor helpers =====


def test_find_first_tensor_not_on_device():
    t_cpu = torch.zeros(2)
    assert find_first_tensor_not_on_device({"a": t_cpu}, torch.device("cpu")) is None
    assert find_first_tensor_not_on_device({"a": t_cpu}, torch.device("cuda")) is t_cpu
    # Nested containers are searched; index-less specs match any index.
    assert find_first_tensor_not_on_device([(t_cpu,)], torch.device("cuda:1")) is t_cpu


# ===== map_batches validation =====


def test_instance_rejected(ray_start_regular_shared_2_cpus):
    # `map_batches`'s generic UDF validation rejects the (non-callable)
    # instance before the TorchInference-specific check; either way,
    # passing an instance fails at map_batches time.
    with pytest.raises(ValueError):
        _make_input_ds().map_batches(
            MyInferenceActor(),  # pyrefly: ignore[bad-argument-type]
            batch_size=8,
            compute=ray.data.ActorPoolStrategy(size=1),
        )


@pytest.mark.parametrize(
    "kwargs",
    [
        {"fn_args": (1,)},
        {"fn_kwargs": {"a": 1}},
    ],
)
def test_fn_args_rejected(ray_start_regular_shared_2_cpus, kwargs):
    with pytest.raises(ValueError, match="fn_args"):
        _make_input_ds().map_batches(
            MyInferenceActor,
            batch_size=8,
            compute=ray.data.ActorPoolStrategy(size=1),
            **kwargs,
        )


def test_fn_constructor_args_forwarded_to_initialize(ray_start_regular_shared_2_cpus):
    class Parameterized(TorchInference):
        # pyrefly: ignore[bad-override]  # narrowing `*args/**kwargs` is the API.
        def initialize(self, scale, offset=0.0):
            self.scale = scale
            self.offset = offset

        def process_on_device(self, input_batch, collated_tensors, collated_other):
            return collated_tensors

    # The base __init__ forwards constructor args to initialize.
    actor = Parameterized(2.0, offset=1.0)
    assert actor.scale == 2.0 and actor.offset == 1.0

    # And map_batches accepts them for the wrapped actor.
    ds = _make_input_ds().map_batches(
        Parameterized,
        batch_size=8,
        compute=ray.data.ActorPoolStrategy(size=1),
        fn_constructor_args=(2.0,),
        fn_constructor_kwargs={"offset": 1.0},
        num_gpus=0.001,
    )
    op = ds._logical_plan.dag
    assert isinstance(op, MapBatches)
    assert op.fn_constructor_args == (2.0,)
    assert op.fn_constructor_kwargs == {"offset": 1.0}


def test_async_method_rejected(ray_start_regular_shared_2_cpus):
    class AsyncCollate(MyInferenceActor):
        # pyrefly: ignore[bad-override]  # async is the defect under test.
        async def collate(self, input_batch):
            ...

    with pytest.raises(TypeError, match="async"):
        _make_input_ds().map_batches(
            AsyncCollate,
            batch_size=8,
            compute=ray.data.ActorPoolStrategy(size=1),
        )


def test_call_warns(ray_start_regular_shared_2_cpus, caplog, propagate_logs):
    class WithCall(MyInferenceActor):
        def __call__(self, batch):
            return batch

    with caplog.at_level(logging.WARNING, logger="ray.data"):
        _make_input_ds().map_batches(
            WithCall,
            batch_size=8,
            compute=ray.data.ActorPoolStrategy(size=1),
            num_gpus=0.001,
        )
    assert "will not be used directly" in caplog.text


def test_missing_num_gpus_warns(
    ray_start_regular_shared_2_cpus, caplog, propagate_logs
):
    with caplog.at_level(logging.WARNING, logger="ray.data"):
        _make_input_ds().map_batches(
            MyInferenceActor,
            batch_size=8,
            compute=ray.data.ActorPoolStrategy(size=1),
        )
    assert "num_gpus" in caplog.text


# ===== Wrapping =====


def test_serial_wrap_applied(ray_start_regular_shared_2_cpus):
    strategy = ray.data.ActorPoolStrategy(size=1)
    ds = _make_input_ds().map_batches(
        MyInferenceActor,
        batch_size=8,
        compute=strategy,
        num_gpus=0.001,
    )
    op = ds._logical_plan.dag
    assert isinstance(op, MapBatches)
    # The UDF is replaced by the managed wrapper, keeping the user's name.
    assert op.fn is not MyInferenceActor
    assert isinstance(op.fn, type)
    assert op.fn.__name__ == "MyInferenceActor"
    assert op.name == "MapBatches(MyInferenceActor)"
    # The serial flow needs no concurrency normalization.
    assert "max_concurrency" not in op.ray_remote_args
    assert isinstance(op.compute, ray.data.ActorPoolStrategy)
    assert op.compute.max_tasks_in_flight_per_actor is None


def test_replace_does_not_rewrap(ray_start_regular_shared_2_cpus):
    # Logical-plan rewrites reconstruct operators with dataclasses.replace,
    # which re-runs __post_init__ with the already-wrapped fn.
    ds = _make_input_ds().map_batches(
        MyInferenceActor,
        batch_size=8,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_gpus=0.001,
    )
    op = ds._logical_plan.dag
    assert isinstance(op, MapBatches)
    op2 = replace(op, input_dependencies=op.input_dependencies)
    assert op2.fn is op.fn


# ===== GPU end-to-end (runs in the GPU CI job; skipped without CUDA) =====

GPU_FEATURES = 64
GPU_BATCH_SIZE = 256
GPU_NUM_BATCHES = 8
GPU_NUM_ROWS = GPU_NUM_BATCHES * GPU_BATCH_SIZE


def _make_gpu_source():
    """Rows where every element of row `id` is `1 + id`, so per-row sums are
    unique, nonzero integers (exact in fp32) — a zeroed, torn, or cross-batch
    read after the device transfer changes the checksum."""

    def to_x(batch):
        ids = np.asarray(batch["id"], dtype=np.int64)
        vals = (1.0 + ids).astype(np.float32)
        return {"id": ids, "data": np.repeat(vals[:, None], GPU_FEATURES, axis=1)}

    return (
        ray.data.range(GPU_NUM_ROWS, override_num_blocks=GPU_NUM_BATCHES)
        .map_batches(to_x, batch_size=GPU_BATCH_SIZE)
        .materialize()
    )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_e2e_cuda(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)

    # NOTE: Defined inside the test so cloudpickle serializes it by value
    # (module-level test classes aren't importable from Ray workers).
    class Predictor(TorchInference):
        def collate(self, input_batch):
            # Only the model input takes the tensor path; `id` stays in
            # `input_batch`. The `(tensors, other)` form threads per-batch
            # side data (here, the batch's min id) to process_on_device.
            return (
                {"data": torch.from_numpy(input_batch["data"])},
                {"min_id": int(input_batch["id"].min())},
            )

        def process_on_device(self, input_batch, collated_tensors, collated_other):
            tensor = collated_tensors["data"]
            # The managed flow must hand us device tensors, the untouched
            # pre-collate batch, and collate's side data.
            assert tensor.device.type == "cuda"
            assert isinstance(input_batch["id"], np.ndarray)
            assert collated_other == {"min_id": int(input_batch["id"].min())}
            # Forward the side data on to finalize.
            return (
                {"rowsum": tensor.sum(dim=1), "double": tensor[:, 0] * 2.0},
                collated_other,
            )

        def finalize(self, input_batch, output_tensors, output_other):
            # process_on_device's side data arrives untouched.
            assert output_other == {"min_id": int(input_batch["id"].min())}
            return {
                "id": input_batch["id"],
                "rowsum": output_tensors["rowsum"].numpy(),
                "double": output_tensors["double"].numpy(),
            }

    ds = _make_gpu_source().map_batches(
        Predictor,
        batch_size=GPU_BATCH_SIZE,
        batch_format="numpy",
        zero_copy_batch=True,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_gpus=1,
    )

    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    ids = np.asarray([row["id"] for row in rows], dtype=np.int64)
    rowsums = np.asarray([row["rowsum"] for row in rows])
    doubles = np.asarray([row["double"] for row in rows])

    # Exactly-once id coverage (passthrough via `input_batch` intact).
    assert np.array_equal(ids, np.arange(GPU_NUM_ROWS, dtype=np.int64))
    # Exact per-row checksums: the H2D delivered the right bytes.
    assert np.array_equal(rowsums, (GPU_FEATURES * (1.0 + ids)).astype(np.float32))
    # Compute results survive the D2H.
    assert np.array_equal(doubles, (2.0 * (1.0 + ids)).astype(np.float32))


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_e2e_cuda_default_collate_and_finalize(shutdown_only):
    # Only `process_on_device` implemented: the default `get_device` (cuda),
    # `collate` (numpy -> tensors), and `finalize` (tensors -> numpy) carry
    # the batch through the managed flow. `fn_constructor_args` reach
    # `initialize`.
    ray.init(num_cpus=2, num_gpus=1)

    class MinimalPredictor(TorchInference):
        # pyrefly: ignore[bad-override]  # narrowing `*args/**kwargs` is the API.
        def initialize(self, scale):
            self.scale = scale

        def process_on_device(self, input_batch, collated_tensors, collated_other):
            return {
                "id": collated_tensors["id"],
                "rowsum": collated_tensors["data"].sum(dim=1) * self.scale,
            }

    ds = _make_gpu_source().map_batches(
        MinimalPredictor,
        batch_size=GPU_BATCH_SIZE,
        batch_format="numpy",
        compute=ray.data.ActorPoolStrategy(size=1),
        fn_constructor_args=(2.0,),
        num_gpus=1,
    )

    rows = sorted(ds.take_all(), key=lambda row: row["id"])
    ids = np.asarray([row["id"] for row in rows], dtype=np.int64)
    rowsums = np.asarray([row["rowsum"] for row in rows])
    assert np.array_equal(ids, np.arange(GPU_NUM_ROWS, dtype=np.int64))
    assert np.array_equal(
        rowsums, (2.0 * GPU_FEATURES * (1.0 + ids)).astype(np.float32)
    )


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_collate_output_must_be_cpu(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)

    class GpuCollate(TorchInference):
        def collate(self, input_batch):
            return {"data": torch.from_numpy(input_batch["data"]).cuda()}

        def process_on_device(self, input_batch, collated_tensors, collated_other):
            return collated_tensors

        def finalize(self, input_batch, output_tensors, output_other):
            return {"data": output_tensors["data"].numpy()}

    ds = _make_gpu_source().map_batches(
        GpuCollate,
        batch_size=GPU_BATCH_SIZE,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_gpus=1,
    )
    with pytest.raises(Exception, match="must return CPU tensors"):
        ds.take_all()


@pytest.mark.skipif(not torch.cuda.is_available(), reason="requires CUDA")
def test_process_output_must_be_on_device(shutdown_only):
    ray.init(num_cpus=2, num_gpus=1)

    class CpuProcess(TorchInference):
        def collate(self, input_batch):
            return {"data": torch.from_numpy(input_batch["data"])}

        def process_on_device(self, input_batch, collated_tensors, collated_other):
            return {"data": collated_tensors["data"].cpu()}

        def finalize(self, input_batch, output_tensors, output_other):
            return {"data": output_tensors["data"].numpy()}

    ds = _make_gpu_source().map_batches(
        CpuProcess,
        batch_size=GPU_BATCH_SIZE,
        compute=ray.data.ActorPoolStrategy(size=1),
        num_gpus=1,
    )
    with pytest.raises(Exception, match="must return tensors on"):
        ds.take_all()


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", __file__]))
