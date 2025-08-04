# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG POST_BUILD_SCRIPT

ENV RAY_worker_num_grpc_internal_threads=1

COPY "$POST_BUILD_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh
