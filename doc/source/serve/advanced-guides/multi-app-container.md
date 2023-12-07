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

Note that the Ray version and Python version in the container should match those of the host environment exactly.
In this example, we are going to walk through deploying two applications in separate containers: a whisper model and a Resnet50 image classification model. 

First, let's set up the docker images with the required dependencies. Save the following to files named `whisper.Dockerfile` and `resnet.Dockerfile`.

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
export IMG1={your-image-repo-and-name}
docker build -t $IMG1 -f whisper.Dockerfile .
docker push $IMG1
```
:::
:::{tab-item} Resnet
```bash
export IMG2={your-image-repo-and-name}
docker build -t $IMG2 -f resnet.Dockerfile .
docker push $IMG2
```
:::
::::

Finally, you can specify the docker image within which you want to run each application in the `container` field of an application's runtime environment specification. The `container` field has three fields:
- `image`: (Required)
- `worker_path`: 
- `run_options`

The following Serve config will start the `whisper` app in a container with the image `your-image-1`, and the `resnet` app in a container with the image `your-image-2`. Concretely, this means that all deployment replicas 

```yaml
applications:
  - name: whisper
    import_path: whisper_example:entrypoint
    route_prefix: /whisper
    runtime_env:
      container:
        image: {your-image-1}
        worker_path: /home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py
  - name: resnet
    import_path: resnet50_example:app
    route_prefix: /resnet
    runtime_env:
      container:
        image: {your-image-2}
        worker_path: /home/ray/anaconda3/lib/python3.8/site-packages/ray/_private/workers/default_worker.py
```

