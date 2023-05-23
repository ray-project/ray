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
