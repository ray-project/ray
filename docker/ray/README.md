Official container images for [Ray](https://github.com/ray-project/ray).
These images contains working Python virtual environments and required
dependencies to run launche Ray nodes and form Ray clusters.
everything needed to get started with running Ray. One can use the images
for local development, or launch clusters with [Ray VM launcher][vm-launcher],
[KubeRay][kuberay], or running them on [Anyscale][anyscale].

Source that builds the images are [here][source].

[vm-launcher]: https://docs.ray.io/en/latest/cluster/vms/index.html
[kuberay]: https://ray-project.github.io/kuberay/
[anyscale]: https://www.anyscale.com/
[source]: https://github.com/ray-project/ray/blob/master/docker/ray/Dockerfile

## Tags

Images are `tagged` with the format
`{Ray version}[-{Python version}][-{Platform}][-{Architecture}]`.

`Ray version` tag can be one of the following:

| Ray version tag | Description |
| --------------- | ----------- |
| `latest`        | The most recent Ray release. |
| `x.y.z`         | A specific Ray release version, e.g. 2.53.0 |
| `nightly`       | The most recent Ray development build (a recent commit from GitHub `master`) |

The optional `Python version` tag specifies the Python version in the image.
All Python versions supported by Ray are available, e.g. `py310`, `py311`, and
`py312`. If unspecified, the tag points to an image using `Python 3.10`.

The optional `Platform` tag specifies the platform where the image is intended
for:

| Platform tag    | Description |
| --------------- | ----------- |
| `-cpu`          | Based off of an Ubuntu image. |
| `-cuXX`         | Based off of an NVIDIA CUDA image with the specified CUDA version `xx`. They require the NVIDIA Docker Runtime. |
| `-gpu`          | Aliases to a specific `-cuXX` tagged image. |
| no tag          | Aliases to `-cpu` tagged images. |

The optional `Architecture` tag can be used to specify images for different CPU
architectures.  Currently, we support the `x86_64` (`amd64`) and `aarch64`
(`arm64`) architectures.

Please note that suffixes are only used to specify `aarch64` images.
No suffix means that it is a multi-platform image index.

| Platform tag | Description                 |
|--------------|-----------------------------|
| no tag       | Multi-platform image index. |
| `-aarch64`   | arm64-compatible images     |
