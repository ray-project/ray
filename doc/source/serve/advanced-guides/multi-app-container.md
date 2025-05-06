(serve-container-runtime-env-guide)=
# Run Multiple Applications in Different Containers

This section explains how to run multiple Serve applications on the same cluster in separate containers with different images.

This feature is experimental and the API is subject to change. If you have additional feature requests or run into issues, please submit them on [Github](https://github.com/ray-project/ray/issues).

## Install Podman

The `image_uri` runtime environment feature uses [Podman](https://podman.io/) to start and run containers. Follow the [Podman Installation Instructions](https://podman.io/docs/installation) to install Podman in the environment for all head and worker nodes.

:::{note}
For Ubuntu, the Podman package is only available in the official repositories for Ubuntu 20.10 and newer. To install Podman in Ubuntu 20.04 or older, you need to first add the software repository as a debian source. Follow these instructions to install Podman on Ubuntu 20.04 or older:

```bash
sudo sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 4D64390375060AA4
sudo apt-get update
sudo apt-get install podman -y
```
:::

## Run a Serve application in a container

This example deploys two applications in separate containers: a Whisper model and a Resnet50 image classification model.

First, install the required dependencies in the images.

:::{warning}
The Ray version and Python version in the container *must* match those of the host environment exactly. Note that for Python, the versions must match down to the patch number.
:::

Save the following to files named `whisper.Dockerfile` and `resnet.Dockerfile`.

::::{tab-set}
:::{tab-item} whisper.Dockerfile
```dockerfile
# Use the latest Ray GPU image, `rayproject/ray:latest-py38-gpu`, so the Whisper model can run on GPUs.
FROM rayproject/ray:latest-py38-gpu

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
# Use the latest Ray CPU image, `rayproject/ray:latest-py38-cpu`.
FROM rayproject/ray:latest-py38-cpu

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

Then, build the corresponding images and push it to your choice of container registry. This tutorial uses `alice/whisper_image:latest` and `alice/resnet_image:latest` as placeholder names for the images, but make sure to swap out `alice` for a repo name of your choice.

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
:::{tab-item} Resnet
```bash
# Build the image from the Dockerfile using Podman
export IMG2=alice/resnet_image:latest
podman build -t $IMG2 -f resnet.Dockerfile .
# Push to a registry. This step is unnecessary if you are deploying Serve locally.
podman push $IMG2
```
:::
::::

Finally, you can specify the container image within which you want to run each application in the `image_uri` field of an application's runtime environment specification.

:::{note}
Previously you could access the feature through the `container` field of the runtime environment. That API is now deprecated in favor of `image_uri`.
:::

The following Serve config runs the `whisper` app with the image `IMG1`, and the `resnet` app with the image `IMG2`. `podman images` command can be used to list the names of the images. Concretely, all deployment replicas in the applications start and run in containers with the respective images.

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



```python
>>> import requests
>>> audio_file = "https://storage.googleapis.com/public-lyrebird-test/test_audio_22s.wav"
>>> resp = requests.post("http://localhost:8000/whisper", json={"filepath": audio_file}) # doctest: +SKIP
>>> resp.json() # doctest: +SKIP
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

>>> link_to_image = "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"
>>> resp = requests.post("http://localhost:8000/resnet", json={"uri": link_to_image}) # doctest: +SKIP
>>> resp.text # doctest: +SKIP
ox
```

## Advanced

### Compatibility with other runtime environment fields

Currently, use of the `image_uri` field is only supported with `config` and `env_vars`. If you have a use case for pairing `image_uri` with another runtime environment feature, submit a feature request on [Github](https://github.com/ray-project/ray/issues).

### Environment variables

The following environment variables will be set for the process in your container, in order of highest to lowest priority:
1. Environment variables specified in `runtime_env["env_vars"]`.
2. All environment variables that start with the prefix `RAY_` (including the two special variables `RAY_RAYLET_PID` and `RAY_JOB_ID`) are inherited by the container at runtime.
3. Any environment variables set in the docker image.

### Running the Ray cluster in a Docker container

If raylet is running inside a container, then that container needs the necessary permissions to start a new container. To setup correct permissions, you need to start the container that runs the raylet with the flag `--privileged`.

### Troubleshooting
* **Permission denied: '/tmp/ray/session_2023-11-28_15-27-22_167972_6026/ports_by_node.json.lock'**
  * This error likely occurs because the user running inside the Podman container is different from the host user that started the Ray cluster. The folder `/tmp/ray`, which is volume mounted into the podman container, is owned by the host user that started Ray. The container, on the other hand, is started with the flag `--userns=keep-id`, meaning the host user is mapped into the container as itself. Therefore, permissions issues should only occur if the user inside the container is different from the host user. For instance, if the user on host is `root`, and you're using a container whose base image is a standard Ray image, then by default the container starts with user `ray(1000)`, who won't be able to access the mounted `/tmp/ray` volume.
* **ERRO[0000] 'overlay' is not supported over overlayfs: backing file system is unsupported for this graph driver**
  * This error should only occur when you're running the Ray cluster inside a container. If you see this error when starting the replica actor, try volume mounting `/var/lib/containers` in the container that runs raylet. That is, add `-v /var/lib/containers:/var/lib/containers` to the command that starts the Docker container.
* **cannot clone: Operation not permitted; Error: cannot re-exec process**
  * This error should only occur when you're running the Ray cluster inside a container. This error implies that you don't have the permissions to use Podman to start a container. You need to start the container that runs raylet, with privileged permissions by adding `--privileged`.
