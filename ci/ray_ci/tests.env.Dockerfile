# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG TEST_ENVIRONMENT_SCRIPT

COPY "$TEST_ENVIRONMENT_SCRIPT" /tmp/post_build_script.sh
RUN bash --login -i /tmp/post_build_script.sh
