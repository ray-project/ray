# Installation on Docker

You can install Ray on any platform that runs Docker. We do not presently publish Docker images for Ray, but you can build them yourself using the Ray distribution.

Using Docker can streamline the build process reliable way to get up and running quickly.

## Install Docker

### Mac, Linux, Windows platforms

The Docker Platform release is available for Mac, Windows, and Linux platforms. Please download the appropriate version from the [Docker website](https://www.docker.com/products/overview#/install_the_platform) and follow the corresponding installation instructions.

### Docker installation on EC2 with Ubuntu

The instructions below show in detail how to prepare an Amazon EC2 instance running Ubuntu 16.04 for use with Docker.

Apply patches to the system:

```
sudo apt-get update
sudo apt-get -y dist-upgrade
```

Install Docker and start the service:
```
sudo apt-get install -y docker.io
sudo service docker start
```

Add the `ubuntu` user to the `docker` group to allow running Docker commands:
```
sudo usermod -a -G docker ubuntu
```

Initiate a new login to gain group permissions (alternatively, log out and log back in again):

```
exec sudo su -l ubuntu
```

Confirm that docker is running:

```
docker images
```
Should produce an empty table similar to the following:
```
REPOSITORY          TAG                 IMAGE ID            CREATED             SIZE
```


## Clone the Ray repository

```
git clone https://github.com/ray-project/ray.git
```

## Build Docker images

Run the script to create Docker images.

```
cd ray
./build-docker.sh
```

This script creates several Docker images:

 * The `ray-project/deploy` image is a self-contained copy of code and binaries suitable for end users.
 * The `ray-project/examples` adds additional libraries for running examples.
 * The `ray-project/base-deps` image builds from Ubuntu Xenial and includes Anaconda and other basic dependencies and can serve as a starting point for developers.

Review images by listing them:
```
$ docker images
```

Output should look something like the following:
```
REPOSITORY                          TAG                 IMAGE ID            CREATED             SIZE
ray-project/examples                latest              7584bde65894        4 days ago          3.257 GB
ray-project/deploy                  latest              970966166c71        4 days ago          2.899 GB
ray-project/base-deps               latest              f45d66963151        4 days ago          2.649 GB
ubuntu                              xenial              f49eec89601e        3 weeks ago         129.5 MB
```


## Launch Ray in Docker

Start out by launching the deployment container.

```
docker run --shm-size=<shm-size> -t -i ray-project/ray:deploy
```

Replace `<shm-size>` with a limit appropriate for your system, for example `512M` or `2G`.
The `-t` and `-i` options here are required to support interactive use of the container.
You should now see a prompt that looks something like:

```
root@ebc78f68d100:/ray#
```


## Test if the installation succeeded

To test if the installation was successful, try running some tests. Within the container shell enter the following commands:

```
python test/runtest.py # This tests basic functionality.
python test/array_test.py # This tests some array libraries.
```

You are now ready to continue with the [Tutorial](tutorial.md).

## Running examples in Docker

Ray includes a Docker image that includes dependencies necessary for running some of the examples. This can be an easy way to see Ray in action on a variety of workloads.

Launch the examples container.
```
docker run --shm-size=1024m -t -i ray-project/ray:examples
```

### Hyperparameter optimization


```
cd /ray/examples/hyperopt/
python driver.py
```

See the [Hyperparameter optimization documentation](../examples/hyperopt/README.md).

### Batch L-BFGS

```
cd /ray/examples/lbfgs/
python driver.py
```

See the [Batch L-BFGS documentation](../examples/lbfgs/README.md).

### Learning to play Pong

```
cd /ray/examples/rl_pong/
python driver.py
```

See the [Learning to play Pong documentation](../examples/rl_pong/README.md).

## Running a cluster with Docker

We have provided [instructions for running a Ray cluster using Docker](using-ray-and-docker-on-a-cluster.md).

