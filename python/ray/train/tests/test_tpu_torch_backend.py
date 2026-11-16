import os
import sys
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

os.environ["TORCH_DEVICE_BACKEND_AUTOLOAD"] = "0"

import pytest
import torch

from ray.air._internal.device_manager.tpu import TPUTorchDeviceManager
from ray.train.torch.train_loop_utils import _WrappedDataLoader


def test_tpu_device_manager_stream_methods():
    """Verify TPUTorchDeviceManager stream methods."""
    dm = TPUTorchDeviceManager()

    with (
        patch.object(dm, "is_available", return_value=True),
        patch.object(dm, "register_custom_torch_dist_backend") as mock_register,
    ):
        assert dm.supports_stream() is True
        mock_register.assert_called_once_with()

    mock_tpu = MagicMock()
    mock_stream = MagicMock(name="TpuStream")
    mock_curr_stream = MagicMock(name="CurrentTpuStream")
    mock_ctx = MagicMock(name="TpuStreamContext")

    mock_tpu.Stream.return_value = mock_stream
    mock_tpu.stream.return_value = mock_ctx

    tpu_device = MagicMock(spec=torch.device, type="tpu")
    with (
        patch.object(torch, "tpu", mock_tpu, create=True),
        patch.object(
            torch.accelerator, "current_stream", return_value=mock_curr_stream
        ) as mock_curr_stream_fn,
    ):
        assert dm.supports_stream() is True

        with dm.get_stream_context(None):
            pass
        mock_tpu.stream.assert_not_called()

        created = dm.create_stream(tpu_device)
        assert created is mock_stream
        mock_tpu.Stream.assert_called_once_with(device=tpu_device)

        ctx = dm.get_stream_context(created)
        assert ctx is mock_ctx
        mock_tpu.stream.assert_called_once_with(created)

        curr = dm.get_current_stream()
        assert curr is mock_curr_stream
        mock_curr_stream_fn.assert_called_once_with()


@pytest.mark.parametrize(
    "record_stream_exc",
    [
        NotImplementedError(
            "Could not run 'aten::record_stream' with arguments from the "
            "'PrivateUse1' backend."
        ),
        RuntimeError("unknown parameter type"),
    ],
)
def test_wrapped_dataloader_tpu_auto_transfer_handles_record_stream_errors(
    record_stream_exc,
):
    """Verify _WrappedDataLoader(auto_transfer=True) tolerates record_stream errors."""
    dm = TPUTorchDeviceManager()
    memcpy_stream = MagicMock(name="memcpy_stream")
    curr_stream = MagicMock(name="curr_stream")
    tpu_device = MagicMock(spec=torch.device, type="tpu")
    entered_streams = []

    @contextmanager
    def fake_stream_ctx(s):
        entered_streams.append(s)
        yield

    def make_fake_tpu_tensor(val):
        t = MagicMock(spec=torch.Tensor)
        t.device = tpu_device
        t.value = val
        t.record_stream.side_effect = record_stream_exc
        return t

    cpu_tensor_1 = MagicMock(spec=torch.Tensor)
    tpu_tensor_1 = make_fake_tpu_tensor(1)
    cpu_tensor_1.to.return_value = tpu_tensor_1

    cpu_tensor_2 = MagicMock(spec=torch.Tensor)
    tpu_tensor_2 = make_fake_tpu_tensor(2)
    cpu_tensor_2.to.return_value = tpu_tensor_2

    cpu_tensor_3 = MagicMock(spec=torch.Tensor)
    tpu_tensor_3 = make_fake_tpu_tensor(3)
    cpu_tensor_3.to.return_value = tpu_tensor_3

    batches = [
        {"x": cpu_tensor_1, "nested": [cpu_tensor_2]},
        {"x": cpu_tensor_3},
    ]

    with (
        patch.object(TPUTorchDeviceManager, "supports_stream", return_value=True),
        patch.object(
            TPUTorchDeviceManager, "create_stream", return_value=memcpy_stream
        ),
        patch.object(
            TPUTorchDeviceManager, "get_stream_context", side_effect=fake_stream_ctx
        ),
        patch.object(
            TPUTorchDeviceManager, "get_current_stream", return_value=curr_stream
        ),
        patch(
            "ray.train.torch.train_loop_utils.get_torch_device_manager_by_device_type",
            return_value=dm,
        ),
    ):
        wrapped = _WrappedDataLoader(batches, device=tpu_device, auto_transfer=True)
        assert wrapped._auto_transfer is True
        assert wrapped._memcpy_stream is memcpy_stream

        results = list(wrapped)

    assert len(results) == 2
    assert results[0]["x"] is tpu_tensor_1
    assert results[0]["nested"][0] is tpu_tensor_2
    assert results[1]["x"] is tpu_tensor_3
    assert wrapped._supports_record_stream is False
    assert memcpy_stream in entered_streams
    assert curr_stream.wait_stream.call_count == 2
    tpu_tensor_1.record_stream.assert_called_once_with(curr_stream)
    tpu_tensor_2.record_stream.assert_not_called()
    tpu_tensor_3.record_stream.assert_not_called()


@pytest.mark.skipif(
    not TPUTorchDeviceManager().is_available(),
    reason="Requires live TPU runtime with torch_tpu installed.",
)
def test_wrapped_dataloader_live_tpu_auto_transfer():
    """Verify _WrappedDataLoader(auto_transfer=True) on live TPU hardware."""
    TPUTorchDeviceManager.register_custom_torch_dist_backend()
    if not torch.accelerator.is_available():
        pytest.skip("No TPU accelerator hardware detected.")
    device = torch.device("tpu")
    raw_batches = [
        (
            torch.arange(16, dtype=torch.float32),
            {"y": torch.ones(16, dtype=torch.float32)},
        )
        for _ in range(4)
    ]
    wrapped = _WrappedDataLoader(raw_batches, device=device, auto_transfer=True)
    assert wrapped._auto_transfer is True
    assert wrapped._memcpy_stream is not None

    seen = 0
    for x, meta in wrapped:
        assert x.device.type == "tpu"
        assert meta["y"].device.type == "tpu"
        out = (x + meta["y"]).sum()
        assert float(out.cpu().item()) == float((16 * 15 / 2) + 16)
        seen += 1

    assert seen == 4


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", "-x", __file__]))
