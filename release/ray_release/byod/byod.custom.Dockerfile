# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG POST_BUILD_SCRIPT

ARG PYTHON_DEPSET=dummy.lock

COPY "$PYTHON_DEPSET" python_depset.lock

COPY "$POST_BUILD_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh
