# RLlib-Contrib

RLlib-Contrib is a directory for more experimental community contributions to RLlib including contributed algorithms. **This directory has a more relaxed bar for contributions than Ray or RLlib.** If you are interested in contributing to RLlib-Contrib, please see the [contributing guide](contributing.md).

## Getting Started and Installation
Navigate to the algorithm sub-directory you are interested in and see the README.md for installation instructions and example scripts to help you get started!

## List of Algorithms and Examples
Go to [List of examples and algorithms](TOC.md) to checkout the examples that our open source contributors have created with RLlib. 


## Maintenance

**Any issues that are filed in `rllib_contrib` will be solved best-effort by the community and there is no expectation of maintenance by the RLlib team.**

**The API surface between algorithms in `rllib_contrib` and current versions of Ray / RLlib is not guaranteed. This means that any APIs that are used in rllib_contrib could potentially become modified/removed in newer version of Ray/RLlib.**

We will generally accept contributions to this directory that meet any of the following criteria:

1. Updating dependencies.
2. Submitting community contributed algorithms that have been tested and are ready for use.
3. Enabling algorithms to be run in different environments (ex. adding support for a new type of gymnasium environment).
4. Updating algorithms for use with the newer RLlib APIs.
5. General bug fixes.

We will not accept contributions that generally add a significant maintenance burden. In this case users should instead make their own repo with their contribution, using the same guidelines as this directory, and the RLlib team can help to market/promote it in the Ray docs.

## Getting Involved

| Platform | Purpose | Support Level |
| --- | --- | --- |
| [Discuss Forum](https://discuss.ray.io) | For discussions about development and questions about usage. | Community |
| [GitHub Issues](https://github.com/ray-project/ray/issues) | For reporting bugs and filing feature requests. | Community |
| [Slack](https://forms.gle/9TSdDYUgxYs8SA9e8) | For collaborating with other Ray users. | Community |
