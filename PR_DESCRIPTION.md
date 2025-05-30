<!-- Thank you for your contribution! Please review https://github.com/ray-project/ray/blob/master/CONTRIBUTING.rst before opening a pull request. -->

<!-- Please add a reviewer to the assignee section when you create a PR. If you don't have the access to it, we will shortly find a reviewer and assign them to your PR. -->

## Why are these changes needed?

This PR introduces a new QPS-based autoscaling policy for Ray Serve that allows scaling decisions to be based on Queries Per Second (QPS) metrics rather than just request queue length. This provides a more direct and intuitive way to scale deployments based on actual traffic load.

**Key Benefits:**
- More accurate scaling decisions based on actual request throughput
- Better handling of applications where QPS is a more relevant metric than queue depth
- Configurable target QPS per replica for fine-tuned scaling control
- Improved autoscaling for high-throughput, low-latency applications

**Key Changes:**
1. **AutoscalingStateManager Enhancement**: Added QPS metric storage and retrieval with windowed averaging
2. **QPS Metric Collection**: ProxyActor now calculates and reports QPS metrics per application
3. **New QPS-Based Policy**: Configurable `qps_based_autoscaling_policy` with target QPS per replica
4. **Configuration Updates**: Extended `AutoscalingConfig` with policy selection and QPS-specific parameters
5. **Comprehensive Testing**: Unit tests for the new policy and QPS handling
6. **Documentation**: Updated autoscaling guide with QPS policy usage examples

## Related issue number

<!-- For example: "Closes #1234" -->
<!-- Please add the related issue number if applicable -->

## Checks

- [ ] I've signed off every commit(by using the -s flag, i.e., `git commit -s`) in this PR.
- [ ] I've run `scripts/format.sh` to lint the changes in this PR.
- [ ] I've included any doc changes needed for https://docs.ray.io/en/master/.
    - [x] I've added any new APIs to the API Reference. For example, if I added a
           method in Tune, I've added it in `doc/source/tune/api/` under the
           corresponding `.rst` file.
- [ ] I've made sure the tests are passing. Note that there might be a few flaky tests, see the recent failures at https://flakey-tests.ray.io/
- Testing Strategy
   - [x] Unit tests
   - [ ] Release tests
   - [ ] This PR is not tested :( 