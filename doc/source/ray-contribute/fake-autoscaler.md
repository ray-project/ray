---
myst:
  html_meta:
    description: "How to run and test Ray's autoscaler locally without a real cluster, using RAY_FAKE_CLUSTER and the fake multi-node provider. Useful for autoscaler development or for debugging applications that depend on autoscaling behavior."
---

(fake-multinode)=

# Testing autoscaling locally

Testing autoscaling behavior is important for autoscaler development and for debugging applications that depend on autoscaler behavior. You can run the autoscaler locally, without launching a real cluster, using one of the following methods:

## Using `RAY_FAKE_CLUSTER=1 ray start`

Complete the following steps:

1. Navigate to the root directory of the Ray repo you have cloned locally.

2. Locate the [fake_multi_node/example.yaml](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/fake_multi_node/example.yaml) example file and fill in the number of CPUs and GPUs the local machine has for the head node type config. The YAML follows the same format as cluster autoscaler configurations, but some fields are not supported.

3. Configure worker types and other autoscaling configs as desired in the YAML file.

4. Start the fake cluster locally:

```shell
$ ray stop --force
$ RAY_FAKE_CLUSTER=1 ray start \
    --autoscaling-config=./python/ray/autoscaler/_private/fake_multi_node/example.yaml \
    --head --block
```

5. Connect your application to the fake local cluster with `ray.init("auto")`.

6. Run `ray status` to view the status of your cluster, or `cat /tmp/ray/session_latest/logs/monitor.*` to view the autoscaler monitor log:

```shell
$ ray status
======== Autoscaler status: 2021-10-12 13:10:21.035674 ========
Node status
---------------------------------------------------------------
Healthy:
 1 ray.head.default
 2 ray.worker.cpu
Pending:
 (no pending nodes)
Recent failures:
 (no failures)

Resources
---------------------------------------------------------------
Usage:
 0.0/10.0 CPU
 0.00/70.437 GiB memory
 0.00/10.306 GiB object_store_memory

Demands:
 (no resource demands)
```

## Using `ray.cluster_utils.AutoscalingCluster`

To programmatically create a fake multi-node autoscaling cluster and connect to it, you can use [cluster_utils.AutoscalingCluster](https://github.com/ray-project/ray/blob/master/python/ray/cluster_utils.py). Here's an example of a basic autoscaling test that launches tasks triggering autoscaling:

```{literalinclude} /../../python/ray/tests/test_autoscaler_fake_multinode.py
:language: python
:dedent: 4
:start-after: __example_begin__
:end-before: __example_end__
```

Python documentation:

```{eval-rst}
.. autoclass:: ray.cluster_utils.AutoscalingCluster
    :members:
```

## Features and limitations of `fake_multinode`

Most of the features of the autoscaler are supported in fake multi-node mode. For example, if you update the contents of the YAML file, the autoscaler picks up the new configuration and applies changes, as it does in a real cluster. Node selection, launch, and termination are governed by the same bin-packing and idle timeout algorithms as in a real cluster.

However, there are a few limitations:

1. All node raylets run uncontainerized on the local machine, so they share the same IP address. See the {ref}`fake_multinode_docker <fake-multinode-docker>` section for an alternative local multi-node setup.

2. Configurations for auth, setup, initialization, Ray start, file sync, and anything cloud-specific aren't supported.

3. You must limit the number of nodes, node CPU, and object store memory to avoid overloading your local machine.

(fake-multinode-docker)=

# Testing containerized multi-node clusters locally with Docker Compose

To go one step further and test a multi-node setup locally where each node uses its own container, with a separate filesystem, IP address, and Ray processes, you can use the `fake_multinode_docker` node provider.

The setup is similar to the {ref}`fake_multinode <fake-multinode>` provider. However, you need to start a monitoring process (`docker_monitor.py`) that runs the `docker compose` command.

Prerequisites:

1. Make sure you have [Docker](https://docs.docker.com/get-docker/) installed.

2. Make sure you have the [Docker Compose V2 plugin](https://docs.docker.com/compose/cli-command/#installing-compose-v2) installed.

## Using `RAY_FAKE_CLUSTER=1 ray up`

Complete the following steps:

1. Navigate to the root directory of the Ray repo you have cloned locally.

2. Locate the [fake_multi_node/example_docker.yaml](https://github.com/ray-project/ray/blob/master/python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml) example file and fill in the number of CPUs and GPUs the local machine has for the head node type config. The YAML follows the same format as cluster autoscaler configurations, but some fields are not supported.

3. Configure worker types and other autoscaling configs as desired in the YAML file.

4. Make sure the `shared_volume_dir` is empty on the host system.

5. Start the monitoring process:

```shell
$ python ./python/ray/autoscaler/_private/fake_multi_node/docker_monitor.py \
    ./python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml
```

6. Start the Ray cluster using `ray up`:

```shell
$ RAY_FAKE_CLUSTER=1 ray up -y ./python/ray/autoscaler/_private/fake_multi_node/example_docker.yaml
```

7. Connect your application to the fake local cluster with `ray.init("ray://localhost:10002")`.

8. Alternatively, get a shell on the head node:

```shell
$ docker exec -it fake_docker_fffffffffffffffffffffffffffffffffffffffffffffffffff00000_1 bash
```

## Using `ray.autoscaler._private.fake_multi_node.test_utils.DockerCluster`

You use this utility to write tests that use multi-node behavior. Use the `DockerCluster` class to set up a Docker Compose cluster in a temporary directory, start the monitoring process, wait for the cluster to come up, connect to it, and update the configuration.

See the API documentation and example test cases for how to use this utility.

```{eval-rst}
.. autoclass:: ray.autoscaler._private.fake_multi_node.test_utils.DockerCluster
    :members:
```

## Features and limitations of `fake_multinode_docker`

The fake multinode docker node provider creates fully fledged nodes in their own containers. However, some limitations remain:

1. Configurations for auth, setup, initialization, Ray start, file sync, and anything cloud-specific aren't supported, but might be in the future.

2. You must limit the number of nodes, node CPU, and object store memory to avoid overloading your local machine.

3. In docker-in-docker setups, you must follow a careful setup to make the fake multinode docker provider work. See the following sections.

## Shared directories within the Docker environment

The containers mount two locations to host storage:

- `/cluster/node`: This location (in the container) points to `cluster_dir/nodes/<node_id>` (on the host). This location is individual per node, so the host can examine contents stored in this directory.
- `/cluster/shared`: This location (in the container) points to `cluster_dir/shared` (on the host). This location is shared across nodes and effectively acts as a shared filesystem (comparable to NFS).

## Setting up in a Docker-in-Docker (dind) environment

When setting up in a Docker-in-Docker (dind) environment, such as the Ray OSS Buildkite environment, keep some things in mind. To make this clear, consider these concepts:

* The **host** is the non-containerized machine that executes the code, for example a Buildkite runner.
* The **outer container** is the container running directly on the **host**. In the Ray OSS Buildkite environment, two containers are started: a *dind* network host and a container with the Ray source code and wheel.
* The **inner container** is a container started by the fake multinode docker node provider.

The control plane for the multinode docker node provider lives in the outer container. However, `docker compose` commands run from the connected docker-in-docker network. In the Ray OSS Buildkite environment, this is the `dind-daemon` container running on the host docker. For example, if you mounted `/var/run/docker.sock` from the host instead, it would be the host docker daemon. We refer to both as the **host daemon** from now on.

The outer container modifies files that must be mounted in the inner containers and modified from there as well. This means that the host daemon must also have access to these files.

Similarly, the inner containers expose ports, but because the host daemon starts the containers, the ports are only accessible on the host or the dind container.

For the Ray OSS Buildkite environment, we set the following environment variables:

* `RAY_TEMPDIR="/ray-mount"`. This environment variable defines where the temporary directory for the cluster files should be created. This directory must be accessible to the host, the outer container, and the inner container. In the inner container, we can control the directory name.

* `RAY_HOSTDIR="/ray"`. When the shared directory has a different name on the host, we can rewrite the mount points dynamically. In this example, the outer container is started with `-v /ray:/ray-mount` or similar, so the directory on the host is `/ray` and in the outer container `/ray-mount` (see `RAY_TEMPDIR`).

* `RAY_TESTHOST="dind-daemon"`. Because the host daemon starts the containers, we can't connect to `localhost`. The ports aren't exposed to the outer container. Thus, we can set the Ray host with this environment variable.

Lastly, Docker Compose requires a Docker image. The default Docker image is `rayproject/ray:nightly`. The Docker image requires `openssh-server` to be installed and enabled. In Buildkite, we build a new image from `rayproject/ray:nightly-py38-cpu` to avoid installing this on the fly for every node, which is the default. This base image is built in one of the previous build steps.

Thus, we set

* `RAY_DOCKER_IMAGE="rayproject/ray:multinode-py38"`

* `RAY_HAS_SSH=1`

to use this Docker image and inform our multinode infrastructure that SSH is already installed.

## Local development

If you're doing local development on the fake multi-node docker module, you can set

* `FAKE_CLUSTER_DEV="auto"`

This mounts the `ray/python/ray/autoscaler` directory to the started nodes. Note that this probably won't work in your docker-in-docker setup.

If you want to specify which top-level Ray directories to mount, you can use:

* `FAKE_CLUSTER_DEV_MODULES="autoscaler,tune"`

This mounts both `ray/python/ray/autoscaler` and `ray/python/ray/tune` within the node containers. The list of modules should be comma-separated and without spaces.
