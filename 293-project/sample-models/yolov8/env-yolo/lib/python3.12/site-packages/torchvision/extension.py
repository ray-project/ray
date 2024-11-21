import os
import sys

import torch

from ._internally_replaced_utils import _get_extension_path


_HAS_OPS = False


def _has_ops():
    return False


try:
    # On Windows Python-3.8.x has `os.add_dll_directory` call,
    # which is called to configure dll search path.
    # To find cuda related dlls we need to make sure the
    # conda environment/bin path is configured Please take a look:
    # https://stackoverflow.com/questions/59330863/cant-import-dll-module-in-python
    # Please note: if some path can't be added using add_dll_directory we simply ignore this path
    if os.name == "nt" and sys.version_info < (3, 9):
        env_path = os.environ["PATH"]
        path_arr = env_path.split(";")
        for path in path_arr:
            if os.path.exists(path):
                try:
                    os.add_dll_directory(path)  # type: ignore[attr-defined]
                except Exception:
                    pass

    lib_path = _get_extension_path("_C")
    torch.ops.load_library(lib_path)
    _HAS_OPS = True

    def _has_ops():  # noqa: F811
        return True

except (ImportError, OSError):
    pass


def _assert_has_ops():
    if not _has_ops():
        raise RuntimeError(
            "Couldn't load custom C++ ops. This can happen if your PyTorch and "
            "torchvision versions are incompatible, or if you had errors while compiling "
            "torchvision from source. For further information on the compatible versions, check "
            "https://github.com/pytorch/vision#installation for the compatibility matrix. "
            "Please check your PyTorch version with torch.__version__ and your torchvision "
            "version with torchvision.__version__ and verify if they are compatible, and if not "
            "please reinstall torchvision so that it matches your PyTorch install."
        )


def _check_cuda_version():
    """
    Make sure that CUDA versions match between the pytorch install and torchvision install
    """
    if not _HAS_OPS:
        return -1
    from torch.version import cuda as torch_version_cuda

    _version = torch.ops.torchvision._cuda_version()
    if _version != -1 and torch_version_cuda is not None:
        tv_version = str(_version)
        if int(tv_version) < 10000:
            tv_major = int(tv_version[0])
            tv_minor = int(tv_version[2])
        else:
            tv_major = int(tv_version[0:2])
            tv_minor = int(tv_version[3])
        t_version = torch_version_cuda.split(".")
        t_major = int(t_version[0])
        t_minor = int(t_version[1])
        if t_major != tv_major:
            raise RuntimeError(
                "Detected that PyTorch and torchvision were compiled with different CUDA major versions. "
                f"PyTorch has CUDA Version={t_major}.{t_minor} and torchvision has "
                f"CUDA Version={tv_major}.{tv_minor}. "
                "Please reinstall the torchvision that matches your PyTorch install."
            )
    return _version


def _load_library(lib_name):
    lib_path = _get_extension_path(lib_name)
    torch.ops.load_library(lib_path)


_check_cuda_version()
