# Intel GPU on Ray

## Intel XPU Overview

The “X” in “XPU” stands for any compute architecture that best fits the need of your application. Every application’s performance, latency, and power needs are matched to the optimal hardware architecture (CPU, GPU, FPGA, ASIC) which creates complexity.

[oneAPI](https://en.wikipedia.org/wiki/OneAPI_(compute_acceleration)) is an open standard adopted by Intel for a unified programming model and APIs to deliver a common developer experience across accelerator architectures.

Intel XPU is programmed with Data Parallel C++ (DPC++) based on open [SYCL](https://en.wikipedia.org/wiki/SYCL) standard. Check [here](https://dgpu-docs.intel.com/_images/one_api_sw_stack.png) for overall Intel oneAPI software stack.

Intel XPU support has been added for some most popular machine learning projects (E.g. [Intel® Extension for PyTorch](https://www.intel.cn/content/www/cn/zh/developer/articles/technical/introducing-intel-extension-for-pytorch-for-gpus.html) and [Intel® Extension for DeepSpeed](https://github.com/intel/intel-extension-for-deepspeed)).

Intel GPU is one of the XPU categories. Check official [Intel Graphics Processor Table](https://dgpu-docs.intel.com/devices/hardware-table.html) for supported devices.

## How it works with Intel GPUs (Experimental)

Users first define an environment variable `RAY_EXPERIMENTAL_ACCELERATOR` to `XPU` before `ray.init`
to specify using Intel XPU runtime to detect Intel GPUs for Ray.

Users can also switch back to use CUDA (enabled by default) `RAY_EXPERIMENTAL_ACCELERATOR=CUDA`. The support of XPU and CUDA are exclusive for now.

Then users specify GPUs via the \`num_gpus\` argument, which is accepted by a variety of workloads.

```
@ray.remote(num_gpus=1)
def gpu_task():
   pass
```

Ray assigns a specific Intel GPU id to each workload (soft assignment but not strongly enforce). It also configures hints using [ONEAPI_DEVICE_SELECTOR](https://intel.github.io/llvm-docs/EnvironmentVariables.html#oneapi-device-selector) for frameworks to use the assigned Intel GPU by default.

## Advanced
### DPC++ Runtime Backends

Similar to NVIDIA CUDA runtime, Intel XPU needs a DPC++ runtime to talk to driver and execute kernels. DPC++ can be used with [different backends]( https://intel.github.io/llvm-docs/design/PluginInterface.html) (E.g., OpenCL and Level Zero). level_zero is device native backend talking with device directly which is recommended and enabled by default.

## References
* [Match Every Application to Its Optimal Architecture with XPU](https://www.intel.com/content/www/us/en/architecture-and-technology/xpu.html)
* [Intel® oneAPI Toolkits](https://www.intel.com/content/www/us/en/developer/tools/oneapi/toolkits.html)
* [Data Parallel C++: the oneAPI Implementation of SYCL](https://www.intel.com/content/www/us/en/developer/tools/oneapi/data-parallel-c-plus-plus.html)
* [Level Zero](https://www.intel.com/content/www/us/en/developer/articles/technical/zero-in-on-level-zero-oneapi-open-backend-approach.html)