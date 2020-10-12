## About
Ray is an open source framework that provides a simple, universal API for building distributed applications. Ray is packaged with RLlib, a scalable reinforcement learning library, and Tune, a scalable hyperparameter tuning library. 
These docker images can be used for both local development and *are ideal* for use with the [Ray Cluster Launcher](https://docs.ray.io/en/latest/cluster/launcher.html).




## Tags
* [`:latest`](https://hub.docker.com/repository/docker/rayproject/ray/tags?page=1&name=latest) - The most recent Ray release.
* `:1.x.x` - A specific release build. 
* [`:nightly`](https://hub.docker.com/repository/docker/rayproject/ray/tags?page=1&name=nightly) - The most recent nightly build.
* `:SHA` - A specific nightly build.

### Suffixes
* `-gpu` - These are based off of an `NVIDIA CUDA` image. They require the [Nvidia Docker Runtime](https://github.com/NVIDIA/nvidia-docker) to be installed on the host for the container to access GPUs.  
* `-cpu`- These are based off of an `Ubuntu` iamge.
* Tags without a suffix refer to `-cpu` images

## Other Images
* [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) - This image with common ML libraries to make development & deployment more smooth!
<br></br><br></br>
* [`rayproject/base-deps`](https://hub.docker.com/repository/docker/rayproject/base-deps) - Internal image with system-level dependencies.
* [`rayproject/ray-deps`](https://hub.docker.com/repository/docker/rayproject/ray-deps) - Internal image with python-level dependencies. 
