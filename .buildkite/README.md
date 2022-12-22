# Buildkite pipelines

This directory contains buildkite pipelines used to start CI tests.

Each step contains a buildkite step that is parsed and executed according to the 
[Buildkite pipeline specification](https://buildkite.com/docs/pipelines).

## Conditions

An extra optional field `conditions` is defined, which includes conditions under which tests are run.
The script `ci/pipeline/determine_tests_to_run.py` determines changed files in a PR and only kicks off
tests that include at least one of the conditions. If no condition is specified, the test is always run.

A special case is the `NO_WHEELS_REQUIRED` condition. If this is present, it indicates that the test can
be run with the latest available binaries - in this case the test can be started early, as it will re-use
the latest branch image and only check out the current code revision in the PR. This early kick off will
only trigger on PR builds, not on branch builds.

## Pipelines

This directory should be considered with respect to the docker images located in `ci/docker`.

- `pipeline.build.yml` contains jobs that require build dependencies. This includes all tests that re-build
  Ray (e.g. when switching Python versions). The tests are run on the `build.Dockerfile` image.
- `pipeline.test.yml` contains jobs that only require an installed Ray and a small subset of dependencies,
  notably exlcuding ML libraries such as Tensorflow or Torch. The tests are run on the `test.Dockerfile` image.
- `pipeline.ml.yml` contains jobs that require ML libraries Tensorflow and Torch to be available. The tests
  are run on the `ml.Dockerfile` image.
- `pipeline.gpu.yml` contains jobs that require one GPU. The tests are run on the `gpu.Dockerfile` image.
- `pipeline.gpu.large.yml` contains jobs that require multi-GPUs (currently 4). The tests are run on the `gpu.Dockerfile` image.
