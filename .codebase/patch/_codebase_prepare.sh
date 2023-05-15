mkdir -p /root/ray-bazel-cache
export GIT_SSL_NO_VERIFY=1
export BYTED_RAY_BUILD_TAG=1
echo "build --config=ci" >> ~/.bazelrc
export https_proxy=http://sys-proxy-rd-relay.byted.org:8118 http_proxy=http://sys-proxy-rd-relay.byted.org:8118 no_proxy="*.byted.org"