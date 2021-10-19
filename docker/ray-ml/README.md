## About
This image is an extension of the [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) image. It includes all extended requirements of `RLlib`, `Serve` and `Tune`. It is a well-provisioned starting point for trying out the Ray ecosystem. [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray-ml/Dockerfile)




## Tags
* [`:latest`](https://hub.docker.com/repository/docker/rayproject/ray-ml/tags?page=1&name=latest) - The most recent Ray release.
* `:1.x.x` - A specific release build. 
* [`:nightly`](https://hub.docker.com/repository/docker/rayproject/ray-ml/tags?page=1&name=nightly) - The most recent nightly build.
* `:SHA` - A specific nightly build.

### Suffixes
* `-gpu` - These are based off of an `NVIDIA CUDA` image. They require the [Nvidia Docker Runtime](https://github.com/NVIDIA/nvidia-docker) to be installed on the host for the container to access GPUs.  
* `-cpu`- These are based off of an `Ubuntu` image.
* Tags without a suffix refer to `-cpu` images

## Other Images
* [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) - Ray and all of its dependencies.
