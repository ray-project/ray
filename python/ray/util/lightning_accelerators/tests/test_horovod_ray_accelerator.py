import os
import platform
import sys
from unittest import mock

import pytest
import ray

import pytorch_lightning.tests.base.develop_pipelines as tpipes
from pytorch_lightning.tests.base import EvalModelTemplate

path_here = os.path.abspath(os.path.dirname(__file__))
path_root = os.path.abspath(os.path.join(path_here, '..', '..'))


try:
    import horovod
    from horovod.common.util import nccl_built
except ImportError:
    HOROVOD_AVAILABLE = False
else:
    HOROVOD_AVAILABLE = True


def _nccl_available():
    if not HOROVOD_AVAILABLE:
        return False

    try:
        return nccl_built()
    except AttributeError:
        return False


@pytest.fixture
def ray_start_2_cpus():
    address_info = ray.init(num_cpus=2)
    try:
        yield address_info
    finally:
        ray.shutdown()


@pytest.fixture
def ray_start_2_gpus():
    address_info = ray.init(num_cpus=2, num_gpus=2)
    try:
        yield address_info
    finally:
        ray.shutdown()


def create_mock_executable():
    # Some modules in site-packages conflict with the tests module
    # used by PyTorch Lightning. As a result, we need to make sure all
    # the Ray workers push the correct tests path to the front of the sys path.
    class MockExecutable:
        def __init__(self):
            sys.path.insert(0, os.path.abspath(path_root))
    return MockExecutable


@mock.patch('pytorch_lightning.accelerators.horovod_ray_accelerator.get_executable_cls')
@pytest.mark.skipif(platform.system() == "Windows", reason="Horovod is not supported on Windows")
def test_horovod_cpu(mock_executable_cls, tmpdir, ray_start_2_cpus):
    """Test Horovod running multi-process on CPU."""
    mock_executable_cls.return_value = create_mock_executable()
    trainer_options = dict(
        default_root_dir=tmpdir,
        max_epochs=1,
        limit_train_batches=10,
        limit_val_batches=10,
        num_processes=2,
        distributed_backend='horovod_ray',
        progress_bar_refresh_rate=0
    )

    model = EvalModelTemplate()
    tpipes.run_model_test(trainer_options, model)


# @mock.patch('pytorch_lightning.accelerators.horovod_ray_accelerator.get_executable_cls')
# @pytest.mark.skipif(platform.system() == "Windows", reason="Horovod is not supported on Windows")
# @pytest.mark.skipif(not _nccl_available(), reason="test requires Horovod with NCCL support")
# @pytest.mark.skipif(torch.cuda.device_count() < 2, reason="test requires multi-GPU machine")
# def test_horovod_multi_gpu(mock_executable_cls, tmpdir, ray_start_2_gpus):
#     """Test Horovod with multi-GPU support."""
#     mock_executable_cls.return_value = create_mock_executable()
#     trainer_options = dict(
#         default_root_dir=tmpdir,
#         max_epochs=1,
#         limit_train_batches=10,
#         limit_val_batches=10,
#         gpus=2,
#         distributed_backend='horovod_ray',
#         progress_bar_refresh_rate=0
#     )
#
#     model = EvalModelTemplate()
#     tpipes.run_model_test(trainer_options, model)