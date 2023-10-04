# Hardware Accelerators (GPUs) on Ray


# Overview

This document aims to describe Ray’s resource model for GPUs and how Ray aims to interact with hardware accelerators as it exists today. It does _not_ attempt to propose fixes or changes for deeper integration with different computational models (e.g. TPUs).

Using this guide an ML Accelerator should be able to be added to Ray in less than a 100 lines of code.


# Ray Core/application level


## Resource Model

Ray models hardware accelerators as GPUs. The GPU resource should be used to model physical accelerators which have the properties:

- **Managed by a node**: A single GPU is managed by a single GPU. (as opposed to a “pod” which many machines can control).
- **Homogenous within a node**: While a ray cluster can support multiple GPU types, and each node may have multiple GPUs, each node should only have a single type of GPU. (e.g. it is ok to have 2 nodes, one with 8x A100 GPUs, one with 8x H100 GPUs, but not a single node with 1x H100 and 1x A100).
- **Multiplexing**: Multiple processes can concurrently run workloads on a single device, assuming they can operate within the resource constraints of the device.
- **Discrete**: If a node has multiple GPUs, each is independently identifiable. A 1-GPU task cannot be split across multiple GPUs. (But communication between GPUs can be implemented out-of-band to achieve multi-gpu workloads).


## How it works with Nvidia GPUs today

Users specify GPUs via the \`num_gpus\` argument, which is accepted by a variety of workloads.

```
@ray.remote(num_gpus=1) 
def gpu_task():     
   pass 
```


### Sharing GPUs (multiplexing)

Multiple workloads can run concurrently on a GPU if it has enough resources.

``` 
@ray.remote(num_gpus=0.5) class HalfGpuActor():     
   pass 

# Shares a GPU 

a1, a2 = HalfGpuActor.remote(), HalfGpuActor.remote()
```


### Specifying a specific type of GPU

GPU types can be specified via the \`accelerator_type\` field.

``` 
from  ray.util.accelerators import  NVIDIA_TESLA_V100 

@ray.remote(num_gpus=1, accelerator_type=NVIDIA_TESLA_V100)

def gpu_task():     
   pass
```


### Multi-gpu nodes

Ray assigns (but does not strongly enforce) a specific GPU id to each workload. It also configures hints for frameworks to use the assigned GPU by default (e.g. the CUDA_VISIBLE_DEVICES environment variable for NVIDIA gpus).

```
@ray.remote(num_gpus=1)
def gpu_task():    
   print(ray.get_gpu_ids())    
   print(os.environ\[“CUDA_VISIBLE_DEVICES”])
   pass
```


## Deploying Ray with GPUs

The number of GPUs and GPU type are expected to be automatically detected and configured. So that the above use cases work out of the box.


### GPU dependencies

Many GPU types have dependencies such as nvidia/cuda drivers and libraries in the case of nvidia GPUs. With docker, there may also be changes required to the container runtime.


# How to add non-Nvidia hardware accelerators


## Adding Basic Support


### Teaching Ray how detect your accelerator type

See [resource_spec.py](https://github.com/ray-project/ray/blob/master/python/ray/_private/resource_spec.py) and add your accelerator to the [autodetect_num_gpus](https://github.com/ray-project/ray/blob/master/python/ray/_private/resource_spec.py#L268)() routine (which only works for Nvidia accelerators right now). This routine is run when Ray starts on the node, and must return the number of devices that can be advertised to the Ray scheduler for this node.


### Supporting worker assignment to specific devices on a node

Most hardware accelerators come with a software framework to help interact with the accelerator (e.g. CUDA for Nvidia). The GPU id should ideally be passed to the framework by default. 

Once your accelerator is present on the current node, and gpu_ids is assigned for the current worker you need to configure your device assignment. For Nvidia GPUs, the following code sets CUDA_VISIBLE_DEVICES to the set of GPU ids assigned to the worker for Ray.

For a custom accelerator, you'll need to add to the logic in [\_raylet.pyx](https://github.com/ray-project/ray/blob/master/python/ray/_raylet.pyx#L1667)  and follow the set_cuda_visible_devices approach for your own device type. Your code should run when there are accelerators of your type on the current node.

Once a job finishes on a node, on worker death Ray kills the process assigned to the ‘GPU resource’.


## Advanced


### Supporting specific accelerator type as resources

See <https://github.com/ray-project/ray/blob/master/python/ray/_private/resource_spec.py#L182>


### Accelerator-type aware autoscaling support

Ensuring that accelerators are properly autodetected can be done fairly simply by filling out/modifying the \`fillout_available_node_types_resources\` in the node provider, or equivalent in KubeRay.
