# ray-k8s-tests

These tests are not automated and thus **must be run manually** for each release.
If you have issues running them, bug the code owner(s) for OSS Kubernetes support.

## How to run
1. Configure kubectl and Helm 3 to access a K8s cluster.
2. `git checkout releases/<release version>`
3. You might have to locally pip install the Ray wheel for the relevant commit (or pip install -e) in a conda env, see Ray client note below.
4. You might have to temporarily delete the file `ray/python/ray/tests/conftest.py`.
5. cd to this directory
6. `IMAGE=rayproject/ray:<release version> bash k8s_release_tests.sh`
7. Test outcomes will be reported at the end of the output.

This runs three tests and does the necessary resource creation/teardown. The tests typically take about 15 minutes to finish.

## Notes
0. Anyscale employees: You should have access to create a K8s cluster using either GKE or EKS, ask OSS Kubernetes code owner if in doubt.
1. Your Ray cluster should be able to accomodate 30 1-CPU pods to run all of the tests.
2. These tests use basic Ray client functionality -- your locally installed Ray version may need to be updated to match the one in the release image.
3. The tests do a poor job of Ray client port-forwarding process clean-up -- if a test fails, it's possible there might be a port-forwarding process stuck running in the background. To identify the rogue process run `ps aux | grep "port-forward"`. Then `kill` it.
4. There are some errors that will appear on the screen during the run -- that's normal, error recovery is being tested.

## Running individual tests
To run any of the three individual tests, substitute in step 5 of **How to Run** `k8s-test.sh` or `helm-test.sh` or `k8s-test-scale.sh`.
It's the last of these that needs 30 1-cpu pods. 10 is enough for either of the other two. The scale test is currently somewhat flaky. Rerun it if it fails.
