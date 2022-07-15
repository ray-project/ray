## About
This image is an extension of the [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) image. It includes all extended requirements of `RLlib`, `Serve` and `Tune`. It is a well-provisioned starting point for trying out the Ray ecosystem. [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray-ml/Dockerfile)

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

Examples tags:
- none: equivalent to `latest`
- `latest`: equivalent to `latest-py37-gpu`, i.e. image for the most recent Ray release
- `nightly-py38-cpu`
- `806c18-py38-cu112`

## Other Images
* [`rayproject/ray`](https://hub.docker.com/repository/docker/rayproject/ray) - Ray and all of its dependencies.
