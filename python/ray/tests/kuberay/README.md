# How to run the KubeRay autoscaling test

This page provides suggestions on running the test `test_autoscaling_e2e` locally.
You might want to do this if your PR is breaking this test in CI and you want to debug why.

Running the test must happen in stages:

1. Tear down any running `kind` cluster
2. Remove the existing ray docker image that will be deployed to the cluster
3. Build a new docker image containing the local ray repository
4. Create a new `kind` cluster
5. Load the docker image into the cluster
6. Set up kuberay
7. Run the test

To help with this, there is a `Dockerfile` and a `rune2e.sh` bash script which
together run these things for you.

## Test requirements

1. Ensure `kind` and `kustomize` are both installed
2. Run `ray/autoscaler/kuberay/init-config.sh` to clone `ray-project/kuberay`,
   which contains config files needed to set up kuberay.
3. Finally, make sure that the `Dockerfile` is using the same python version as
   what you're using to run the test. By default, this dockerfile is built using
   the `rayproject/ray:nightly-py310` build.
4. Modify `EXAMPLE_CLUSTER_PATH` in `test_autoscaling_e2e.py`.

Now you're ready to run the test.

## Running the test

Run `./rune2e.sh` to run the test.

The test itself does not tear down resources on failure; you can
- examine a Ray cluster from a failed test (`kubectl get pods`, `kubectl get pod`, `kubectl get raycluster`)
- view all logs (`kubectl logs <head pod name>`) or just logs associated with the autoscaler (`kubectl logs <head pod name> -c autoscaler`)
- delete the Ray cluster (`kubectl delete raycluster -A`)
- rerun the test without tearing the operator down (`RAY_IMAGE=<registry>/<repo>:<tag> python test_autoscaling_e2e.py`)
- tear down the operator when you're done `python setup/teardown_kuberay.py`
- copy files from a pod to your filesystem (`kubectl cp <pod>:/path/to/file /target/path/in/local/filesystem`)
- access a bash prompt inside the pod (`kubectl exec -it <pod> bash`)

## Joblib elastic Pool autoscaling test

`test_joblib_autoscaling_e2e.py` verifies that both the multiprocessing Pool and
the Joblib backend drive KubeRay's Autoscaler v2 from zero workers to two and
back to zero while each backend remains open. The Ray head advertises zero CPUs,
so successful work placement proves that worker scale-up occurred. The workload
records Ray node IDs and asserts that work ran on two distinct worker nodes. It
then waits for Ray's CPU resources and the worker Pods to return to zero, proving
that scale-down came from idle actor retirement rather than backend teardown.

The test reuses `ray-cluster.autoscaler-v2-template.yaml`. CI derives a thin
image from the current test-branch Ray image that adds the repository's pinned
Joblib dependency; it does not copy source files into site-packages or pin a Ray
release. `ci/k8s/run-operator-tests.sh` builds and loads that image, installs the
KubeRay operator, and runs all `team:kuberay` tests, including this one.

For a targeted run against an existing kind cluster and KubeRay operator, build
the same thin image from a local Ray image, load it, and expose it to the test:

```bash
docker build \
  --build-arg RAY_BASE_IMAGE=ray-ci:kuberay-test \
  --file python/ray/tests/kuberay/joblib_autoscaling.Dockerfile \
  --tag ray-ci:kuberay-test-with-joblib \
  .
kind load docker-image ray-ci:kuberay-test-with-joblib
AUTOSCALER_V2=True \
  RAY_IMAGE=docker.io/library/ray-ci:kuberay-test-with-joblib \
  PULL_POLICY=Never \
  python python/ray/tests/kuberay/test_joblib_autoscaling_e2e.py
```

The test creates a process-specific namespace, runs the Pool and Joblib paths as
two consecutive scale-up/scale-down cycles, and removes only that namespace when
it finishes.
