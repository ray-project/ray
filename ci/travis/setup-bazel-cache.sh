set -x
set -e

mkdir -p $HOME/ray-bazel-cache
echo "build --disk_cache=$HOME/ray-bazel-cache" >> .bazelrc
