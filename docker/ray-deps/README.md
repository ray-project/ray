## About
This is an internal image, the [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) or [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) should be used!

This has the python-level dependencies for `Ray` and the `Ray Autoscaler`. The `ray` image is built on top of this. This image is built periodically or when dependencies are added. [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray-deps/Dockerfile)

## Tags

Images are `tagged` with the format `{Ray version}[-{Python version}][-{Platform}]`. `Ray version` tag can be one of the following:

| Ray version tag | Description |
| --------------- | ----------- |
| `latest`                     | The most recent Ray release. |
| `x.y.z`                      | A specific Ray release, e.g. 1.12.1 |
| `nightly`                    | The most recent Ray development build (a recent commit from Github `master`) |
| `6 character Git SHA prefix` | A specific development build (uses a SHA from the Github `master`, e.g. `8960af`). |

The optional `Python version` tag specifies the Python version in the image. All Python versions supported by Ray are available, e.g. `py37`, `py38`, `py39` and `py310`. If unspecified, the tag points to an image using `Python 3.7`.

The optional `Platform` tag specifies the platform where the image is intended for:

| Platform tag | Description |
| --------------- | ----------- |
| `-cpu`  | These are based off of an Ubuntu image. |
| `-cuXX` | These are based off of an NVIDIA CUDA image with the specified CUDA version `xx`. They require the Nvidia Docker Runtime. |
| `-gpu`  | Aliases to a specific `-cuXX` tagged image. |
| no tag  | Aliases to `-cpu` tagged images for `ray`, and aliases to ``-gpu`` tagged images for `ray-ml`. |

## Other Images
* [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) - Ray and all of its dependencies.
* [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) - This image with common ML libraries to make development & deployment more smooth!
<br></br><br></br>
* [`rayproject/base-deps`](https://hub.docker.com/repository/docker/rayproject/base-deps) - Internal image with system-level dependencies.
