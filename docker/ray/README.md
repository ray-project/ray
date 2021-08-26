## About
Default docker images for [Ray](https://github.com/ray-project/ray)! This includes
everything needed to get started with running Ray! They work for both local development and *are ideal* for use with the [Ray Cluster Launcher](https://docs.ray.io/en/master/cluster/launcher.html). [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray/Dockerfile)




## Tags
* [`:latest`](https://hub.docker.com/repository/docker/rayproject/ray/tags?page=1&name=latest) - The most recent Ray release.
* `:1.x.x` - A specific release build. 
* [`:nightly`](https://hub.docker.com/repository/docker/rayproject/ray/tags?page=1&name=nightly) - The most recent nightly build.
* `:SHA` - A specific nightly build.

### Suffixes
* `-gpu` - These are based off of an `NVIDIA CUDA` image. They require the [Nvidia Docker Runtime](https://github.com/NVIDIA/nvidia-docker) to be installed on the host for the container to access GPUs.  
* `-cpu`- These are based off of an `Ubuntu` image.
* Tags without a suffix refer to `-cpu` images

## Other Images
* [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) - This image with common ML libraries to make development & deployment more smooth!
