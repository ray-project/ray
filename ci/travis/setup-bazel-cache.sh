set -x
set -e

mkdir $HOME/ray-bazel-cache
echo "build --dist_cache=$HOME/ray-bazel-cache" >> .bazelrc
