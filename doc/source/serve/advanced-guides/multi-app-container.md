---
myst:
  html_meta:
    description: "Run Serve applications in separate containers with Podman-backed runtime environments, including compatibility notes and troubleshooting."
---

(serve-container-runtime-env-guide)=
# Run multiple applications in different containers

This page describes how to run multiple Serve applications on the same cluster in separate containers with different images.

:::{note}
This feature is experimental, and the API might change. If you have feature requests or run into issues, submit them on [GitHub](https://github.com/ray-project/ray/issues).
:::

## Install Podman

The `image_uri` runtime environment feature uses [Podman](https://podman.io/) to start and run containers. Follow the [Podman installation instructions](https://podman.io/docs/installation) to install Podman in the environment of every head and worker node.

:::{note}
On Ubuntu 20.10 and later, the Podman package is available in the official Ubuntu repositories.

```bash
sudo apt-get update
sudo apt-get install podman -y
```
:::

## Run a Serve application in a container

This example deploys a Whisper model and a ResNet50 image classification model as two applications, each in its own container.

First, install the required dependencies in the images.

:::{warning}
The Ray and Python versions in the container must exactly match those of the host environment. Python versions must match down to the patch number.
:::

Save the following to files named `whisper.Dockerfile` and `resnet.Dockerfile`.

::::{tab-set}
:::{tab-item} whisper.Dockerfile
```dockerfile
# Use a Ray GPU image, `rayproject/ray:2.56.1-py311-gpu`, so the Whisper model can run on GPUs.
FROM rayproject/ray:2.56.1-py311-gpu

# Install the package `faster_whisper`, which is a dependency for the Whisper model.
RUN pip install faster_whisper==0.10.0
RUN sudo apt-get update && sudo apt-get install curl -y

# Download the source code for the Whisper application into `whisper_example.py`.
RUN curl -O https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/doc_code/whisper_example.py

# Add /home/ray path to PYTHONPATH avoid import module error
ENV PYTHONPATH "${PYTHONPATH}:/home/ray"
```
:::
:::{tab-item} resnet.Dockerfile
```dockerfile
# Use a Ray CPU image, `rayproject/ray:2.56.1-py311-cpu`.
FROM rayproject/ray:2.56.1-py311-cpu

# Install the packages `torch` and `torchvision`, which are dependencies for the ResNet model.
RUN pip install torch==2.0.1 torchvision==0.15.2
RUN sudo apt-get update && sudo apt-get install curl -y

# Download the source code for the ResNet application into `resnet50_example.py`.
RUN curl -O https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/doc_code/resnet50_example.py

# Add /home/ray path to PYTHONPATH avoid import module error
ENV PYTHONPATH "${PYTHONPATH}:/home/ray"
```
:::
::::

Each Dockerfile downloads the source code for its application. The Whisper application transcribes an audio file at a given URL, and the ResNet application classifies an image at a given URL:

::::{tab-set}
:::{tab-item} whisper_example.py
```{literalinclude} ../doc_code/whisper_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```
:::
:::{tab-item} resnet50_example.py
```{literalinclude} ../doc_code/resnet50_example.py
:language: python
:start-after: __serve_example_begin__
:end-before: __serve_example_end__
```
:::
::::

Then build the images and push them to a container registry of your choice. This tutorial uses `alice/whisper_image:latest` and `alice/resnet_image:latest` as placeholder image names. Replace `alice` with a repository name of your choice.

::::{tab-set}
:::{tab-item} Whisper
```bash
# Build the image from the Dockerfile using Podman
export IMG1=alice/whisper_image:latest
podman build -t $IMG1 -f whisper.Dockerfile .
# Push to a registry. This step is unnecessary if you are deploying Serve locally.
podman push $IMG1
```
:::
:::{tab-item} ResNet
```bash
# Build the image from the Dockerfile using Podman
export IMG2=alice/resnet_image:latest
podman build -t $IMG2 -f resnet.Dockerfile .
# Push to a registry. This step is unnecessary if you are deploying Serve locally.
podman push $IMG2
```
:::
::::

Finally, specify the container image that each application runs in with the `image_uri` field of that application's runtime environment.

:::{note}
Earlier versions exposed this feature through the `container` field of the runtime environment. That field is deprecated in favor of `image_uri`.
:::

The following Serve config file runs the `whisper` application with the image `IMG1` and the `resnet` application with the image `IMG2`. Every replica in each application starts and runs in a container with that application's image. To list the names of your images, run `podman images`.

```yaml
applications:
  - name: whisper
    import_path: whisper_example:entrypoint
    route_prefix: /whisper
    runtime_env:
      image_uri: {IMG1}
  - name: resnet
    import_path: resnet50_example:app
    route_prefix: /resnet
    runtime_env:
      image_uri: {IMG2}
```

### Send queries

Send a request to each application at its route prefix:

```python
import json
import requests

audio_file = "https://storage.googleapis.com/public-lyrebird-test/test_audio_22s.wav"
resp = requests.post("http://localhost:8000/whisper", json={"filepath": audio_file})
print(json.dumps(resp.json(), indent=4))
```

```text
{
    "language": "en",
    "language_probability": 1,
    "duration": 21.775,
    "transcript_text": " Well, think about the time of our ancestors. A ping, a ding, a rustling in the bushes is like, whoo, that means an immediate response. Oh my gosh, what's that thing? Oh my gosh, I have to do it right now. And dude, it's not a tiger, right? Like, but our, our body treats stress as if it's life-threatening because to quote Robert Sapolsky or butcher his quote, he's a Robert Sapolsky is like one of the most incredible stress physiologists of",
    "whisper_alignments": [
        [
            0.0,
            0.36,
            " Well,",
            0.3125
        ],
        ...
    ]
}
```

```python
link_to_image = "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"
resp = requests.post("http://localhost:8000/resnet", json={"uri": link_to_image})
print(resp.text)
```

```text
ox
```

## Advanced

The following sections cover how `image_uri` works with other runtime environment fields, which environment variables the container receives, how to run the Ray cluster itself in a container, and how to troubleshoot common errors.

### Compatibility with other runtime environment fields

Currently, Ray supports the `image_uri` field only together with the `config` and `env_vars` fields. If you have a use case for pairing `image_uri` with another runtime environment feature, submit a feature request on [GitHub](https://github.com/ray-project/ray/issues).

### Environment variables

The process in your container receives the following environment variables, in order from highest to lowest priority:

1. Environment variables specified in `runtime_env["env_vars"]`.
1. All environment variables that start with the prefix `RAY_`, which the container inherits at runtime. These include the two special variables `RAY_RAYLET_PID` and `RAY_JOB_ID`.
1. Any environment variables set in the Docker image.

(running-the-ray-cluster-in-a-docker-container)=
### Run the Ray cluster in a Docker container

If the raylet runs inside a container, that container needs permission to start new containers. To grant it, start the container that runs the raylet with the `--privileged` flag.

### Troubleshooting
* `Permission denied: '/tmp/ray/session_2023-11-28_15-27-22_167972_6026/ports_by_node.json.lock'`
  * This error likely occurs because the user inside the Podman container differs from the host user that started the Ray cluster. The host user that started Ray owns the `/tmp/ray` folder, which is volume-mounted into the Podman container. The container starts with the `--userns=keep-id` flag, which maps the host user into the container as itself. So permission issues should only occur if the user inside the container differs from the host user. For example, if the host user is `root` and the container's base image is a standard Ray image, the container starts as the user `ray(1000)` by default, and that user can't access the mounted `/tmp/ray` volume.
* `ERRO[0000] 'overlay' is not supported over overlayfs: backing file system is unsupported for this graph driver`
  * This error should only occur when you run the Ray cluster inside a container. If you see it when the replica actor starts, try volume-mounting `/var/lib/containers` in the container that runs the raylet. To do so, add `-v /var/lib/containers:/var/lib/containers` to the command that starts that Docker container.
* `cannot clone: Operation not permitted; Error: cannot re-exec process`
  * This error should only occur when you run the Ray cluster inside a container. It means you don't have permission to start a container with Podman. Start the container that runs the raylet with the `--privileged` flag.
* Slow or hanging container startup
  * The typical cause is the default Podman storage driver, `vfs`, combined with large container images. Podman runs in rootless mode, so its startup sequence modifies the permissions of files in the container, and the `vfs` driver does this slowly. Try configuring Podman to use the `overlay` storage driver instead. You might also need to set `mount_program` to `/usr/bin/fuse-overlayfs` or the equivalent path on your system.
