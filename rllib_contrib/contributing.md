# Contributing Guidelines

Any issues that are filed in `rllib_contrib` will be solved best-effort by the community and there is no expectation of maintenance by the RLlib team. 

**The api surface between algorithms in rllib_contrib and current versions of ray / rllib is not guaranteed. This means that any apis that are used in rllib_contrib could potentially become modified/removed in newer version of ray/rllib. You should check the version of ray that an algorithm is using before making any modifications, and refer to that documentation / release on github.** 

We will generally accept contributions to this repo that meet any of the following criteria:

1. Updating dependencies.
2. Submitting community contributed algorithms that have been tested and are ready for use.
3. Enabling algorithms to be run in different environments (ex. adding support for a new type of gym environment).
4. Updating algorithms for use with the newer RLlib APIs.
5. General bug fixes.

We will not accept contributions that generally add a significant maintenance burden. In this case users should instead make their own repo with their contribution, **using the same guidelines as this repo**, and the RLlib team can help to market/promote it in the ray docs.

## Contributing new algorithms

If users would like to contribute a new algorithm tor rllib_contrib, they should follow these steps:
1. Create a new directory with the same structure as the other algorithms.
2. Add a `README.md` file that describes the algorithm and its usecases.
3. Create unit tests/shorter learning tests and long learning tests for the algorithm.
4. Submit a PR, add the tag `rllib_contrib`, and then a RLlib maintainer will review it and help you set up your testing to integrate with the CI of this repo.

Regarding unit tests and long running tests:

- Unit tests are any tests that tests a sub component of an algorithm. For example tests that check the value of a loss function given some inputs.
- Short learning tests should run an algorithm on an easy to learn environment for a short amount of time (e.g. ~3 minutes) and check that the algorithm is achieving some learning threshold (e.g. reward mean or loss).
- Long learning tests should run an algorithm on a hard to learn environment (e.g.) for a long amount of time (e.g. ~1 hour) and check that the algorithm is achieving some learning threshold (e.g. reward mean or loss).
