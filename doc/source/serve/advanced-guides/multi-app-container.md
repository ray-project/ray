(serve-container-runtime-env-guide)=
# Run Multiple Applications in Different Containers

This section goes over how you can manage dependencies per application in a more isolated manner by running your Serve applications in separate containers with different images.

:::note
This is an experimental feature and the API is subject to change. If you have additional feature requests or run into issues, please submit them on [Github](https://github.com/ray-project/ray/issues).
:::

## Install podman

The `container` runtime environment feature uses [Podman](https://podman.io/) to start and run containers. Please follow these steps to install Podman in your environment. For instance, if you are using Kuberay, you need to use an image that has podman installed for the head and worker group specs.

::::{tab-set}

:::{tab-item} Ubuntu 20.10 and newer
```bash
sudo apt-get update
sudo apt-get install podman -y
```
:::

:::{tab-item} Ubuntu 20.04 and older
The podman package is only available in the official repositories for Ubuntu 20.10 and newer. To install podman in Ubuntu 20.04, you need to first add the software repository as a debian source.

```bash
sudo sh -c "echo 'deb http://download.opensuse.org/repositories/devel:/kubic:/libcontainers:/stable/xUbuntu_20.04/ /' > /etc/apt/sources.list.d/devel:kubic:libcontainers:stable.list"
sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 4D64390375060AA4
sudo apt-get update
sudo apt-get install podman -y
```
:::

:::{tab-item} Other
See [Podman Installation Instructions](https://podman.io/docs/installation).
:::

::::

## Run Serve application in a container

In this example, we are going to walk through deploying two applications in separate containers: a whisper model and a Resnet50 image classification model. 

First, let's set up the docker images with the required dependencies. 

:::note
The Ray version and Python version in the container should match those of the host environment exactly. Note that for Python, the versions must match down to the patch number.
:::

In `whisper.Dockerfile`:
* We are running the Whisper model on GPU, so the base image for the Whisper Dockerfile is the latest Ray GPU image, `rayproject/ray:latest-py38-gpu`.
* We install the package `faster_whisper`, which is a dependency for the whisper model.
* We then download the source code for the whisper application into `whisper_example.py`.

Similarly, `resnet.Dockerfile` uses the appropriate base image, installs necessary dependencies, and downloads the resnet model source code.
Save the following to files named `whisper.Dockerfile` and `resnet.Dockerfile`.

::::{tab-set}
:::{tab-item} whisper.Dockerfile
```dockerfile
FROM rayproject/ray:latest-py38-gpu

RUN pip install faster_whisper==0.10.0
RUN sudo apt-get update && sudo apt-get install curl -y

RUN curl -O https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/doc_code/whisper_example.py
```
:::
:::{tab-item} resnet.Dockerfile
```dockerfile
FROM rayproject/ray:latest-py38-cpu

RUN torch==2.0.1 torchvision==0.15.2
RUN sudo apt-get update && sudo apt-get install curl -y

RUN curl -O https://raw.githubusercontent.com/ray-project/ray/master/doc/source/serve/doc_code/resnet50_example.py
```
:::
::::

Then, build the corresponding docker images and push it to your choice of docker registry.

::::{tab-set}
:::{tab-item} Whisper
```bash
export IMG1={your-image-repo-and-name-1}
docker build -t $IMG1 -f whisper.Dockerfile .
docker push $IMG1
```
:::
:::{tab-item} Resnet
```bash
export IMG2={your-image-repo-and-name-2}
docker build -t $IMG2 -f resnet.Dockerfile .
docker push $IMG2
```
:::
::::

Finally, you can specify the docker image within which you want to run each application in the `container` field of an application's runtime environment specification. The `container` field has three fields:
- `image`: (Required) The image to run your application in.
- `worker_path`: The absolute path to `default_worker.py` inside the container. This is necessary if the Ray installation directory is different than that of the host (where raylet is running). For instance, if you are running on bare metal, and using a standard Ray docker image as the base image for your application image, then the Ray installation directory on host will likely differ from that of the container. Note that if you are using a standard Ray docker image as the base image, then `default_worker.py` will always be found at `/home/ray/anaconda3/lib/{python-version}/site-packages/ray/_private/workers/default_worker.py`
- `run_options`: Additional options to pass to the `podman run` command used to start a Serve deployment replica in a container. See [podman run documentation](https://docs.podman.io/en/latest/markdown/podman-run.1.html) for a list of all options. If you are familiar with `docker run`, most options will work the same way.

The following Serve config will run the `whisper` app with the image `IMG1`, and the `resnet` app with the image `IMG2`. Concretely, this means that all deployment replicas in the applications will start and run in containers with the respective images.

```yaml
applications:
  - name: whisper
    import_path: whisper_example:entrypoint
    route_prefix: /whisper
    runtime_env:
      container:
        image: {IMG1}
        worker_path: /home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py
  - name: resnet
    import_path: resnet50_example:app
    route_prefix: /resnet
    runtime_env:
      container:
        image: {IMG2}
        worker_path: /home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py
```

### Send queries



```python
>>> import requests
>>> audio_file = "https://storage.googleapis.com/public-lyrebird-test/test_audio_22s.wav"
>>> resp = requests.post("http://localhost:8000/whisper", json={"filepath": audio_file})
>>> resp.json()
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

>>> image_uri = "https://serve-resnet-benchmark-data.s3.us-west-1.amazonaws.com/000000000019.jpeg"
>>> resp = requests.post("http://localhost:8000/resnet", json={"uri": image_uri})
>>> resp.text
ox
```

## Details

### Compatibility with other Runtime Environment Fields

Currently, use of the `container` field is not supported with any other field in `runtime_env`. If you have a use case for pairing `container` with another runtime environment feature, please submit a feature request on [Github](https://github.com/ray-project/ray/issues).

### Environment Variables

All environment variables that start with the prefix `RAY_` (including the two special variables `RAY_RAYLET_PID` and `RAY_JOB_ID`) will be propagated into the container's environment at runtime.

### Running the Ray cluster in a Docker container

If raylet is running inside a container, then that container will need the necessary permissions to start a new container. In order to do that, you need to start the container that runs raylet with the flag `--privileged`.

### Troubleshooting
* **Permission denied: '/tmp/ray/session_2023-11-28_15-27-22_167972_6026/ports_by_node.json.lock'**
  * If you're seeing this error, that likely means the user running inside the podman container is different from the host user that started the Ray cluster. The folder `/tmp/ray`, which is volume mounted into the podman container, is owned by the host user that started Ray, and the container is started with the `--userns=keep-id`, so there should only be permimssions issues if the user running inside the podman container is different. For instance, if the user on host is `root`, and you're using a container whose base image is a standard Ray image, then by default the container will be started with user `ray(1000)`, who won't be able to access the mounted `/tmp/ray` volume.
* **ERRO[0000] 'overlay' is not supported over overlayfs: backing file system is unsupported for this graph driver**
  * [This should only occur when you are running the Ray cluster inside a container] If you see this error when starting the replica actor, try volume mounting `/var/lib/containers` in the container that runs raylet. (i.e, add `-v /var/lib/containers:/var/lib/containers` to the command that starts the docker container)
* **cannot clone: Operation not permitted; Error: cannot re-exec process**
  * [This should only occur when you are running the Ray cluster inside a container] This implies that you don't have the permissions to use podman to start a container. You need to start the container (that runs raylet) with privileged permissions by adding `--privileged`.