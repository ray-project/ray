# Installation on Docker

You can install Ray on any platform that runs Docker. We do not presently publish Docker images for Ray, but you can build them yourself using the Ray distribution. Using Docker can provide a reliable way to get up and running quickly.

## Install Docker

The Docker Platform release is available for Mac, Windows, and Linux platforms. Please download the appropriate version from the [Docker website](https://www.docker.com/products/overview#/install_the_platform).

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

 * The `ray-project/ray:deploy` image is a self-contained copy of code and binaries suitable for end users.
 * The `ray-project/ray:examples` adds additional libraries for running examples.
 * Ray developers who want to edit locally on the host filesystem should use the `ray-project/ray:devel` image, which allows local changes to be reflected immediately within the container.

## Launch Ray in Docker

Start out by launching the deployment container.

```
docker run --shm-size=1024m -t -i ray-project/ray:deploy
```

## Test if the installation succeeded

To test if the installation was successful, try running some tests.

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
cd ~/ray/examples/hyperopt/
python driver.py
```

See the [Hyperparameter optimization documentation](../examples/hyperopt/README.md).

### Batch L-BFGS

```
cd ~/ray/examples/lbfgs/
python driver.py
```

See the [Batch L-BFGS documentation](../examples/lbfgs/README.md).

### Learning to play Pong

```
cd ~/ray/examples/rl_pong/
python driver.py
```

See the [Learning to play Pong documentation](../examples/rl_pong/README.md).


## Developing with Docker (Experimental)

These steps apply only to Ray developers who prefer to use editing tools on the host machine while building and running Ray within Docker. If you have previously been building locally we suggest that you start with a clean checkout before building with Ray's developer Docker container.

You may see errors while running `setup.sh` on Mac OS X. If you have this problem please try re-running the script. Increasing the memory of Docker's VM (say to 8GB from the default 2GB) seems to help.


Launch the developer container.

```
docker run -v $(pwd):/home/ray-user/ray --shm-size=1024m -t -i ray-project/ray:devel
```

Build Ray inside of the container.

```
cd ray
./setup.sh
./build.sh
```
