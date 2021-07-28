# ray-k8s-tests

How to run
1. Configure kubectl and Helm 3 to access a K8s cluster.
2. `git checkout releases/<release version>`
3. You might have to locally pip install the Ray wheel for the relevant commit (or pip install -e) in a conda env, see Ray client note below.
4. cd to this directory
3. `IMAGE=rayproject/ray:<release version> bash k8s_ci.sh`

This runs three tests and does the necessary resource creation/teardown. The tests typically take about 15 minutes to finish.
Notes:
1. Your Ray cluster should be able to accomodate 30 1-CPU pods to run all of the tests.
2. These tests use basic Ray client functionality -- your locally installed Ray version may need to be updated to match the one in the release image.
3. The tests do a poor job of Ray client port-forwarding process clean-up -- if a test fails, it's possible there might be a port-forwarding process stuck running in the background. To identify the rogue process run `ps aux | grep "port-forward"`. Then `kill` it.
4. There are some errors that will appear on the screen during the run -- that's normal, error recovery is being tested.

To run any of the three individual tests, substitute in step 4 above `k8s-test.sh` or `helm-test.sh` or `k8s-test-scale.sh`.
It's the last of these that needs 30 1-cpu pods. 10 is enough for either of the other two.
