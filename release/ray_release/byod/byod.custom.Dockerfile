# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG LOCK_FILE
COPY "$LOCK_FILE" /tmp/lock_file.txt

ARG POST_BUILD_SCRIPT

COPY "$POST_BUILD_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh
