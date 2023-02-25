# How to run the KubeRay autoscaling test

This page provides suggestions on running the test `test_autoscaling_e2e` locally.
You might want to do this if your PR is breaking this test in CI and you want to debug why.

## Build a docker image with your code changes.
First, push your code changes to your git fork.
The Dockerfile below will work if you've only made Python changes.
```dockerfile
# Use the latest Ray master as base.
FROM rayproject/ray:nightly-py37
# Invalidate the cache so that fresh code is pulled in the next step.
ARG BUILD_DATE
# Retrieve your development code.
RUN git clone -b <my-dev-branch> https://github.com/<my-git-handle>/ray
# Install symlinks to your modified Python code.
RUN python ray/python/ray/setup-dev.py -y
```

Build the image and push it to your docker account or registry. Assuming your Dockerfile is named "Dockerfile":
```shell
docker build --build-arg BUILD_DATE=$(date +%Y-%m-%d:%H:%M:%S) -t <registry>/<repo>:<tag> - < Dockerfile
docker push <registry>/<repo>:<tag>
```

## Setup Access to a Kubernetes cluster.
Gain access to a Kubernetes cluster.
The easiest thing to do is to use KinD.
```shell
brew install kind
kind create cluster
```

## Install master Ray
The test uses Ray client, so you should either
- install nightly Ray in your environment
- install Ray from source in your environment (`pip install -e`)

Match your environment's Python version with the Ray image you are using.

## Run the test.

```shell
# Set up the operator.
python setup/setup_kuberay.py
# Run the test.
RAY_IMAGE=<your-image> python test_autoscaling_e2e.py
# Tear RayClusters and operator down.
python setup/teardown_kuberay.py
```

The test itself does not tear down resources on failure; you can
- examine a Ray cluster from a failed test (`kubectl get pod`, `kubectl get raycluster`)
- delete the Ray cluster (`kubectl delete raycluster -A`)
- rerun the test without tearing the operator down (`RAY_IMAGE=<your-image> python test_autoscaling_e2e.py`)
- tear down the operator when you're done `python setup/teardown_kuberay.py`
