# Serving a Chatbot with Ray Serve

| Template Specification | Description |
| ---------------------- | ----------- |
| Summary | This template demonstrates how to serve a chatbot with [Ray Serve](https://docs.ray.io/en/latest/serve/index.html).|
| Time to Run | |
| Minimum Compute Requirements | At least 1 GPU node. The default is 1 node with 4 CPUs, and 1 node with 1 NVIDIA T4 GPU. |
| Cluster Environment | This template uses a docker image built on top of the latest Anyscale-provided Ray image using Python 3.9: [`anyscale/ray:latest-py39-cu118`](https://docs.anyscale.com/reference/base-images/overview). See the appendix below for more details. |

## Get Started

**When the workspace is up and running, start coding by clicking on the Jupyter or VSCode icon above. Open the `start.ipynb` file and follow the instructions there.**

By the end, we'll have a chatbot that can have a conversation using GPT-J.

The application will look something like this:

```text
>>> Hi there!

...

>>> What's your name?

...
```

![Example output]()

## Appendix

### Advanced: Build off of this template's cluster environment

#### Option 1: Build a new cluster environment on Anyscale

You'll find a `cluster_env.yaml` file in the working directory of the template. Feel free to modify this to include more requirements, then follow [this guide](https://docs.anyscale.com/configure/dependency-management/cluster-environments#creating-a-cluster-environment) use the `anyscale` CLI to create a new cluster environment.

Finally, update your workspace's cluster environment to this new one after it's done building.

#### Option 2: Build a new docker image with your own infrastructure

Use the following `docker pull` command if you want to manually build a new Docker image based off of this one.

```bash
...
```
