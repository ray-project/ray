# Using Ray and Docker on a Cluster (Experimental)

Packaging and deploying an application using Docker can provide certain advantages. It can make managing dependencies easier, help ensure that each cluster node receives a uniform configuration, and facilitate swapping hardware resources between applications.


## Create your Docker image

First build a Ray Docker image by following the instructions for [Installation on Docker](install-on-docker.md).
This will allow you to create the `ray-project/deploy` image that serves as a basis for using Ray on a cluster with Docker.

Docker images encapsulate the system state that will be used to run nodes in the cluster.
We recommend building on top of the Ray-provided Docker images to add your application code and dependencies.

You can do this in one of two ways: by building from a customized Dockerfile or by saving an image after entering commands manually into a running container.
We describe both approaches below.

### Creating a customized Dockerfile

We recommend that you read the official Docker documentation for [Building your own image](https://docs.docker.com/engine/getstarted/step_four/) ahead of starting this section.
Your customized Dockerfile is a script of commands needed to set up your application,
possibly packaged in a folder with related resources.

A simple template Dockerfile for a Ray application looks like this:

```
# Application Dockerfile template
FROM ray-project/deploy
RUN git clone <my-project-url>
RUN <my-project-installation-script>
```

This file instructs Docker to load the image tagged `ray-project/deploy`, check out the git
repository at `<my-project-url>`, and then run the script `<my-project-installation-script>`.

Build the image by running something like:
```
docker build -t <my-app> .
```
Replace `<app-tag>` with a tag of your choice.


### Creating a Docker image manually

Launch the `ray-project/deploy` image interactively

```
docker run -t -i ray-project/deploy
```

Next, run whatever commands are needed to install your application.
When you are finished type `exit` to stop the container.

Run
```
docker ps -a
```
to identify the id of the container you just exited.

Next, commit the container
```
docker commit -t <app-tag> <container-id>
```

Replace `<app-tag>` with a name for your container and replace `<container-id>` id with the hash id of the container used in configuration.

## Publishing your Docker image to a repository

When using Amazon EC2 it can be practical to publish images using the Repositories feature of Elastic Container Service.
Follow the steps below and see [documentation for creating a repository](http://docs.aws.amazon.com/AmazonECR/latest/userguide/repository-create.html) for additional context.

First ensure that the AWS command-line interface is installed.

```
sudo apt-get install -y awscli
```

Next create a repository in Amazon's Elastic Container Registry.
This results in a shared resource for storing Docker images that will be accessible from all nodes.


```
aws ecr create-repository --repository-name <repository-name> --region=<region>
```

Replace `<repository-name>` with a string describing the application.
Replace `<region>` with the AWS region string, e.g., `us-west-2`.
This should produce output like the following:

```
{
    "repository": {
        "repositoryUri": "123456789012.dkr.ecr.us-west-2.amazonaws.com/my-app",
        "createdAt": 1487227244.0,
        "repositoryArn": "arn:aws:ecr:us-west-2:123456789012:repository/my-app",
        "registryId": "123456789012",
        "repositoryName": "my-app"
    }
}
```

Take note of the `repositoryUri` string, in this example `123456789012.dkr.ecr.us-west-2.amazonaws.com/my-app`.


Tag the Docker image with the repository URI.

```
docker tag <app-tag> <repository-uri>
```

Replace the `<app-tag>` with the container name used previously and replace `<repository-uri>` with URI returned by the command used to create the repository.

Log into the repository:

```
eval $(aws ecr get-login --region <region>)
```

Replace `<region>` with your selected AWS region.

Push the image to the repository:
```
docker push <repository-uri>
```
Replace `<repository-uri>` with the URI of your repository. Now other hosts will be able to access your application Docker image.


## Starting a cluster

We assume a cluster configuration like that described in instructions for [using Ray on a large cluster](using-ray-on-a-large-cluster.md).
In particular, we assume that there is a head node that has ssh access to all of the worker nodes, and that there is a file `workers.txt` listing the IP addresses of all worker nodes.

### Install the Docker image on all nodes

Create a script called `setup-docker.sh` on the head node.
```
# setup-docker.sh
sudo apt-get install -y docker.io
sudo service docker start
sudo usermod -a -G docker ubuntu
exec sudo su -l ubuntu
eval $(aws ecr get-login --region <region>)
docker pull <repository-uri>
```

Replace `<repository-uri>` with the URI of the repository created in the previous section.
Replace `<region>` with the AWS region in which you created that repository.
This script will install Docker, authenticate the session with the container registry, and download the container image from that registry.

Run `setup-docker.sh` on the head node (if you used the head node to build the Docker image then you can skip this step):
```
bash setup-docker.sh
```

Run `setup-docker.sh` on the worker nodes:
```
parallel-ssh -h workers.txt -P -t 0 -I < setup-docker.sh
```

### Launch Ray cluster using Docker

To start Ray on the head node run the following command:

```
eval $(aws ecr get-login --region <region>)
docker run \
    -d --shm-size=<shm-size> --net=host \
    <repository-uri> \
    ray start --head \
        --object-manager-port=8076 \
        --redis-port=6379 \
        --num-workers=<num-workers>
```

Replace `<repository-uri>` with the URI of the repository.
Replace `<region>` with the region of the repository.
Replace `<num-workers>` with the number of workers, e.g., typically a number similar to the number of cores in the system.
Replace `<shm-size>` with the the amount of shared memory to make available within the Docker container, e.g., `8G`.


To start Ray on the worker nodes create a script `start-worker-docker.sh` with content like the following:
```
eval $(aws ecr get-login --region <region>)
docker run -d --shm-size=<shm-size> --net=host \
    <repository-uri> \
    ray start \
        --object-manager-port=8076 \
        --redis-address=<redis-address> \
        --num-workers=<num-workers>

```

Replace `<redis-address>` with the string `<head-node-private-ip>:6379` where `<head-node-private-ip>` is the private network IP address of the head node.

Execute the script on the worker nodes:
```
parallel-ssh -h workers.txt -P -t 0 -I < setup-worker-docker.sh
```


## Running jobs on a cluster

On the head node, identify the id of the container that you launched as the Ray head.

```
docker ps
```

the container id appears in the first column of the output.

Now launch an interactive shell within the container:

```
docker exec -t -i <container-id> bash
```

Replace `<container-id>` with the container id found in the previous step.

Next, launch your application program.
The Python program should contain an initialization command that takes the Redis address as a parameter:

```
ray.init(redis_address="<redis-address>")
```


## Shutting down a cluster

Kill all running Docker images on the worker nodes:
```
parallel-ssh -h workers.txt -P 'docker kill $(docker ps -q)'
```

Kill all running Docker images on the head node:
```
docker kill $(docker ps -q)
```
