## About
This is an internal image, the [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) or [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) should be used!


This image  has the system-level dependencies for `Ray` and the `Ray Autoscaler`. The `ray-deps` image is built on top of this. This image is built periodically or when dependencies are added. [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/base-deps/Dockerfile)




## Tags
* [`:latest`](https://hub.docker.com/repository/docker/rayproject/base-deps/tags?page=1&name=latest) - The most recent Ray release.
* `:1.x.x` - A specific release build. 
* [`:nightly`](https://hub.docker.com/repository/docker/rayproject/base-deps/tags?page=1&name=nightly) - The most recent nightly build.
* `:DATE` - A specific build.

### Suffixes
* `-gpu` - These are based off of an `NVIDIA CUDA` image. They require the [Nvidia Docker Runtime](https://github.com/NVIDIA/nvidia-docker) to be installed on the host for the container to access GPUs.  
* `-cpu`- These are based off of an `Ubuntu` image.
* Tags without a suffix refer to `-cpu` images

## Other Images
* [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) - Ray and all of its dependencies.
* [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) - This image with common ML libraries to make development & deployment more smooth!
<br></br><br></br>

* [`rayproject/ray-deps`](https://hub.docker.com/repository/docker/rayproject/ray-deps) - Internal image with python-level dependencies. 
