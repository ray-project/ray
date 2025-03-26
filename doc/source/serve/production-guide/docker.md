(serve-custom-docker-images)=

# Custom Docker Images

This section helps you:

* Extend the official Ray Docker images with your own dependencies
* Package your Serve application in a custom Docker image instead of a `runtime_env`
* Use custom Docker images with KubeRay

To follow this tutorial, make sure to install [Docker Desktop](https://docs.docker.com/engine/install/) and create a [Dockerhub](https://hub.docker.com/) account where you can host custom Docker images.

## Working example

Create a Python file called `fake.py` and save the following Serve application to it:

```{literalinclude} ../doc_code/fake_email_creator.py
:start-after: __fake_start__
:end-before: __fake_end__
:language: python
```

This app creates and returns a fake email address. It relies on the [Faker package](https://github.com/joke2k/faker) to create the fake email address. Install the `Faker` package locally to run it:

```console
% pip install Faker==18.13.0

...

% serve run fake:app

...

# In another terminal window:
% curl localhost:8000
john24@example.org
```

This tutorial explains how to package and serve this code inside a custom Docker image.

## Extending the Ray Docker image

The [rayproject](https://hub.docker.com/u/rayproject) organization maintains Docker images with dependencies needed to run Ray. In fact, the [rayproject/ray](https://hub.docker.com/r/rayproject/ray) repo hosts Docker images for this doc. For instance, [this RayService config](https://github.com/ray-project/kuberay/blob/release-1.1.0/ray-operator/config/samples/ray-service.sample.yaml) uses the [rayproject/ray:2.9.0](https://hub.docker.com/layers/rayproject/ray/2.9.0/images/sha256-e64546fb5c3233bb0f33608e186e285c52cdd7440cae1af18f7fcde1c04e49f2?context=explore) image hosted by `rayproject/ray`.

You can extend these images and add your own dependencies to them by using them as a base layer in a Dockerfile. For instance, the working example application uses Ray 2.9.0 and Faker 18.13.0. You can create a Dockerfile that extends the `rayproject/ray:2.9.0` by adding the Faker package:

```dockerfile
# File name: Dockerfile
FROM rayproject/ray:2.9.0

RUN pip install Faker==18.13.0
```

In general, the `rayproject/ray` images contain only the dependencies needed to import Ray and the Ray libraries. You can extend images from either of these repos to build your custom images.

Then, you can build this image and push it to your Dockerhub account, so it can be pulled in the future:

```console
% docker build . -t your_dockerhub_username/custom_image_name:latest

...

% docker image push your_dockerhub_username/custom_image_name:latest

...
```

Make sure to replace `your_dockerhub_username` with your DockerHub user name and the `custom_image_name` with the name you want for your image. `latest` is this image's version. If you don't specify a version when you pull the image, then Docker automatically pulls the `latest` version of the package. You can also replace `latest` with a specific version if you prefer.

## Adding your Serve application to the Docker image

During development, it's useful to package your Serve application into a zip file and pull it into your Ray cluster using `runtime_envs`. During production, it's more stable to put the Serve application in the Docker image instead of the `runtime_env` since new nodes won't need to dynamically pull and install the Serve application code before running it.

Use the [WORKDIR](https://docs.docker.com/engine/reference/builder/#workdir) and [COPY](https://docs.docker.com/engine/reference/builder/#copy) commands inside the Dockerfile to install the example Serve application code in your image:

```dockerfile
# File name: Dockerfile
FROM rayproject/ray:2.9.0

RUN pip install Faker==18.13.0

# Set the working dir for the container to /serve_app
WORKDIR /serve_app

# Copies the local `fake.py` file into the WORKDIR
COPY fake.py /serve_app/fake.py
```

KubeRay starts Ray with the `ray start` command inside the `WORKDIR` directory. All the Ray Serve actors are then able to import any dependencies in the directory. By `COPY`ing the Serve file into the `WORKDIR`, the Serve deployments have access to the Serve code without needing a `runtime_env.`

For your applications, you can also add any other dependencies needed for your Serve app to the `WORKDIR` directory.

Build and push this image to Dockerhub. Use the same version as before to overwrite the image stored at that version.

## Using custom Docker images in KubeRay

Run these custom Docker images in KubeRay by adding them to the RayService config. Make the following changes:

1. Set the `rayVersion` in the `rayClusterConfig` to the Ray version used in your custom Docker image.
2. Set the `ray-head` container's `image` to the custom image's name on Dockerhub.
3. Set the `ray-worker` container's `image` to the custom image's name on Dockerhub.
4. Update the  `serveConfigV2` field to remove any `runtime_env` dependencies that are in the container.

A pre-built version of this image is available at [shrekrisanyscale/serve-fake-email-example](https://hub.docker.com/r/shrekrisanyscale/serve-fake-email-example). Try it out by running this RayService config:

```{literalinclude} ../doc_code/fake_email_creator.yaml
:start-after: __fake_config_start__
:end-before: __fake_config_end__
:language: yaml
```
