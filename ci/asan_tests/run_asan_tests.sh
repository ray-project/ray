set -e
set -x

export LD_PRELOAD=/usr/lib/gcc/x86_64-linux-gnu/7/libasan.so
export ASAN_OPTIONS=detect_leaks=0

cd $HOME/ray
# async plasma test
python -m pytest -v --durations=5 --timeout=300 python/ray/experimental/test/async_test.py

# Ray tests
bazel test --test_tag_filters=-jenkins_only python/ray/serve/...
bazel test --test_tag_filters=-jenkins_only python/ray/dashboard/...
bazel test --test_tag_filters=-jenkins_only python/ray/tests/...
bazel test --test_tag_filters=-jenkins_only python/ray/tune/...
