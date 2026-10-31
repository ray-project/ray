"""Backport: device-neutral torch.compile cache for vLLM's Ray workers.

TODO(kourosh): Once Ray Serve LLM requires a vLLM with
https://github.com/vllm-project/vllm/pull/63247 (and torch >= 2.14), replace
this module with setting VLLM_COMPILE_ON_ONE_RANK=1.

vLLM's Ray executor runs a replica's rank 0 on its physical GPU (cuda:1 for a
replica placed on GPU 1), while the compile cache is keyed by rank. Inductor
bakes the compile-time device index into the cached code and kernels, so a
replica reusing a cache built by a replica on another GPU index crashes with
"CUDA driver error: invalid argument". The vLLM fix uses torch's
compile_on_one_rank mode behind the opt-in VLLM_COMPILE_ON_ONE_RANK, which this
module turns on; torch 2.13 does not have the mode, so on torch 2.13 this
module patches the two places Inductor bakes the device instead.

`apply` runs in each vLLM worker process from Serve LLM's
worker_process_setup_hook.
"""

import logging
import os
import re

logger = logging.getLogger(__name__)

# Generated wrapper code pins the compile-time device; resolve it at run time.
_DEVICE_LITERALS = re.compile(
    r"(torch\.cuda\._DeviceGuard|torch\.cuda\.set_device|(?<![\w.])get_raw_stream)\(\d+\)"
)
_CURRENT_DEVICE = "__import__('torch').cuda.current_device()"


def apply() -> None:
    import torch
    import vllm.envs as envs
    from torch.compiler import config as compiler_config

    if hasattr(compiler_config, "compile_on_one_rank"):
        if hasattr(envs, "VLLM_COMPILE_ON_ONE_RANK"):
            # vLLM applies the mode during its compile and keys its cache on
            # it, but leaves it off by default.
            os.environ.setdefault("VLLM_COMPILE_ON_ONE_RANK", "1")
            return
        compiler_config.compile_on_one_rank = True
    elif torch.__version__.startswith("2.13."):
        _patch_codegen()
        _patch_static_launcher()
    else:
        logger.warning(
            "No device-neutral compile cache backport for torch %s; "
            "replicas on different GPU indices must not share a compile cache.",
            torch.__version__,
        )
        return
    _add_compile_cache_factor(envs)
    logger.info(
        "Device-neutral compile cache backport applied (torch %s).", torch.__version__
    )


def _rewrite(source_code: str) -> str:
    return _DEVICE_LITERALS.sub(rf"\1({_CURRENT_DEVICE})", source_code)


def _patch_codegen() -> None:
    # The wrapper and kernel modules are written and loaded through PyCodeCache,
    # and the compile cache stores the source as written, so rewriting here also
    # makes the cached copy device-neutral.
    from torch._inductor.codecache import PyCodeCache

    write, load = PyCodeCache.write.__func__, PyCodeCache.load.__func__

    def patched_write(cls, source_code, *args, **kwargs):
        return write(cls, _rewrite(source_code), *args, **kwargs)

    def patched_load(cls, source_code, *args, **kwargs):
        return load(cls, _rewrite(source_code), *args, **kwargs)

    PyCodeCache.write = classmethod(patched_write)
    PyCodeCache.load = classmethod(patched_load)


def _patch_static_launcher() -> None:
    # Cached kernels are loaded into one device's CUDA context when the cache is
    # restored; load them into the device they are launched on instead.
    import torch
    from torch._inductor.runtime.static_triton_launcher import (
        StaticallyLaunchedCudaKernel as Kernel,
    )

    load_kernel, run = Kernel.load_kernel, Kernel.run

    def patched_load_kernel(self, device):
        if self.function is not None:
            return
        cubin_path = self.cubin_path
        load_kernel(self, device)
        self._cubin_path_for_devices = cubin_path
        self._handles_by_device = {device: (self.module, self.function)}

    def patched_run(self, *args, **kwargs):
        handles = getattr(self, "_handles_by_device", None)
        if handles is not None and self._cubin_path_for_devices is not None:
            device = torch.cuda.current_device()
            if device not in handles:
                module, function, _, _ = self.C_impl._load_kernel(
                    self._cubin_path_for_devices, self.name, self.shared, device
                )
                handles[device] = (module, function)
            self.module, self.function = handles[device]
        return run(self, *args, **kwargs)

    Kernel.load_kernel = patched_load_kernel
    Kernel.run = patched_run


def _add_compile_cache_factor(envs) -> None:
    # Caches compiled before the backport bake in device indices; never reuse them.
    compile_factors = envs.compile_factors

    def patched_compile_factors():
        return {**compile_factors(), "RAY_LLM_DEVICE_NEUTRAL_COMPILE": True}

    envs.compile_factors = patched_compile_factors
