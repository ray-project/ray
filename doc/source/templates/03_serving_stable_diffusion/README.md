# Serving a Stable Diffusion Model with Ray Serve

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template loads a pretrained stable diffusion model from HuggingFace and serves it to a local endpoint as a [Ray Serve](https://docs.ray.io/en/latest/serve/index.html) deployment. |
| Time to Run | Around 2 minutes to setup the models and generate your first image(s). Less than 10 seconds for every subsequent round of image generation (depending on the image size). |
| Minimum Compute Requirements | At least 1 GPU node. The default is 4 nodes, each with 1 NVIDIA T4 GPU. |
| Cluster Environment | This template uses a docker image built on top of the latest Anyscale-provided Ray image using Python 3.9: [`anyscale/ray:2.4.0-py39-cu118`](https://docs.anyscale.com/reference/base-images/ray-240/py39). See the appendix below for more details. |

## Get Started

**When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `start.ipynb` file and follow the instructions there.**

By the end, we'll have an application that generates images using stable diffusion for a given prompt!

The application will look something like this:

```text
Enter a prompt (or 'q' to quit):   twin peaks sf in basquiat painting style

Generating image(s)...

Generated 4 image(s) in 8.75 seconds to the directory: 58b298d9
```

![Example output](https://github-production-user-asset-6210df.s3.amazonaws.com/3887863/239090189-dc1f1b7b-2fa0-4886-ae12-ca5d35b8ebc9.png)

## Appendix

### Advanced: Build off of this template's cluster environment

#### Option 1: Build a new cluster environment on Anyscale

You'll find a `cluster_env.yaml` file in the working directory of the template. Feel free to modify this to include more requirements, then follow [this guide](https://docs.anyscale.com/configure/dependency-management/cluster-environments#creating-a-cluster-environment) use the `anyscale` CLI to create a new cluster environment.

Finally, update your workspace's cluster environment to this new one after it's done building.

#### Option 2: Build a new docker image with your own infrastructure

Use the following `docker pull` command if you want to manually build a new Docker image based off of this one.

```bash
docker pull us-docker.pkg.dev/anyscale-workspace-templates/workspace-templates/serve-stable-diffusion-model-ray-serve:2.4.0
```
