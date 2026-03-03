
## How to pull upstream changes into the vendored cloudpickle

Right now, we will need to update the vendored cloudpickle manually. Here are the steps to do so:
1. Overwrite the `cloudpickle.py` and `cloudpickle_fast.py` from the [upstream](https://github.com/cloudpipe/cloudpickle)
2. Update the version in python/ray/cloudpickle/__init__.py
3. Run performance benchmarks to validate no regression. (e.g. microbenchmark, scalability tests, etc.)
