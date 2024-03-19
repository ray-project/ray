## About
Default docker images for [Ray](https://github.com/ray-project/ray)! This includes
everything needed to get started with running Ray! They work for both local development and *are ideal* for use with the [Ray Cluster Launcher](https://docs.ray.io/en/master/cluster/cloud.html). [Find the Dockerfile here.](https://github.com/ray-project/ray/blob/master/docker/ray/Dockerfile)

## Tags

Images are `tagged` with the format `{Ray version}[-{Python version}][-{Platform}][-{Architecture}]`. `Ray version` tag can be one of the following:

| Ray version tag | Description |
| --------------- | ----------- |
| `latest`                     | The most recent Ray release. |
| `x.y.z`                      | A specific Ray release, e.g. 2.9.3 |
| `nightly`                    | The most recent Ray development build (a recent commit from GitHub `master`) |

The optional `Python version` tag specifies the Python version in the image. All Python versions supported by Ray are available, e.g. `py38`, `py39` and `py310`. If unspecified, the tag points to an image using `Python 3.8`.

The optional `Platform` tag specifies the platform where the image is intended for:

| Platform tag | Description |
| --------------- | ----------- |
| `-cpu`  | These are based off of an Ubuntu image. |
| `-cuXX` | These are based off of an NVIDIA CUDA image with the specified CUDA version `xx`. They require the Nvidia Docker Runtime. |
| `-gpu`  | Aliases to a specific `-cuXX` tagged image. |
| no tag  | Aliases to `-cpu` tagged images for `ray`, and aliases to ``-gpu`` tagged images for `ray-ml`. |

The optional `Architecture` tag can be used to specify images for different CPU architectures.
Currently, we support the `x86_64` (`amd64`) and `aarch64` (`arm64`) architectures.

Please note that suffixes are only used to specify `aarch64` images. No suffix means
`x86_64`/`amd64`-compatible images.

| Platform tag | Description             |
|--------------|-------------------------|
| `-aarch64`   | arm64-compatible images |
| no tag       | Defaults to `amd64`     |

Examples tags:
- none: equivalent to `latest`
- `latest`: equivalent to `latest-py38-cpu`, i.e. image for the most recent Ray release
- `nightly-py38-cpu`
- `806c18-py38-cu112`
- `806c18-py38-cu116-aarch64`

## Roadmap

Ray 2.3 will be the first release for which arm64 images are released. These images will have the `-aarch64` suffix.

There won't be a `:latest-aarch64` image, instead `:2.3.0-aarch64` should be used explicitly. This is because
we may remove suffixes in the next release.

For Ray 2.4, we aim to have support for multiplatform images. This means that specifying the suffix
will not be needed anymore - docker will automatically choose a compatible image.

We may stop publishing architecture suffixes completely when we have support for multiplatform images.

There is an open RFC issue on GitHub to discuss this roadmap: [Link to issue](https://github.com/ray-project/ray/issues/31966)

## Other Images
* [`rayproject/ray-ml`](https://hub.docker.com/repository/docker/rayproject/ray-ml) - This image with common ML libraries to make development & deployment more smooth!
