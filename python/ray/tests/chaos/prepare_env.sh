#!/usr/bin/env bash
#
# Sets up environment for the Kubernetes chaos testing.
# The environment consists of:
# - a KubeRay cluster, port-forwarded to localhost:8265.
# - a chaos-mesh operator ready to inject faults.

set -xe

./ci/env/install-minimal.sh 3.8
PYTHON=3.8 ./ci/env/install-dependencies.sh
# Specifying above somehow messes up the Ray install.
# Uninstall and re-install Ray so that we can use Ray Client.
# (Remove thirdparty_files to sidestep an issue with psutil.)
pip uninstall -y ray && rm -rf /ray/python/ray/thirdparty_files
pip install -e /ray/python
echo "--Setting up local kind cluster."

echo "--Preparing k8s environment."
./ci/k8s/prep-k8s-environment.sh
./ci/k8s/prep-helm.sh

echo "--Building py38-cpu Ray image for the test."
LINUX_WHEELS=1 BUILD_ONE_PYTHON_ONLY=py38 ./ci/ci.sh build
pip install -q docker
python ci/build/build-docker-images.py --py-versions py38 --device-types cpu --build-type LOCAL --build-base
# Tag the image built in the last step. We want to be sure to distinguish the image from the real Ray nightly.
docker tag rayproject/ray:nightly-py38-cpu ray-ci:kuberay-test
# Load the image into the kind node
kind load docker-image ray-ci:kuberay-test

# Helm install KubeRay
echo "--Installing KubeRay operator from official Helm repo."
helm repo add kuberay https://ray-project.github.io/kuberay-helm/
helm install kuberay-operator kuberay/kuberay-operator
kubectl wait pod  -l app.kubernetes.io/name=kuberay-operator --for=condition=Ready=True  --timeout=5m

echo "--Installing KubeRay cluster and port forward."
# We are in m4i.xlarge and have 4 cpus. Can't have too many nodes.
helm install raycluster kuberay/ray-cluster --set image.repository=ray-ci --set image.tag=kuberay-test --set worker.replicas=2 --set worker.resources.limits.cpu=500m --set worker.resources.requests.cpu=500m --set head.resources.limits.cpu=500m --set head.resources.requests.cpu=500m
kubectl wait pod -l ray.io/cluster=raycluster-kuberay --for=condition=Ready=True --timeout=5m
kubectl port-forward --address 0.0.0.0 service/raycluster-kuberay-head-svc 8265:8265 &

# Helm install chaos-mesh
echo "--Installing chaos-mesh operator and CR."
helm repo add chaos-mesh https://charts.chaos-mesh.org
kubectl create ns chaos-mesh
helm install chaos-mesh chaos-mesh/chaos-mesh -n=chaos-mesh --set chaosDaemon.runtime=containerd --set chaosDaemon.socketPath=/run/containerd/containerd.sock --version 2.6.1
kubectl wait pod  --namespace chaos-mesh  -l app.kubernetes.io/instance=chaos-mesh --for=condition=Ready=True
