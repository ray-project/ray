# syntax=docker/dockerfile:1.3-labs
# shellcheck disable=SC2148

ARG BASE_IMAGE
FROM "$BASE_IMAGE"

ARG LOCK_FILE
SHELL ["/bin/bash", "-ice"]

RUN <<EOF
#!/bin/bash
set -euo pipefail

if [ -n "$LOCK_FILE" ]; then
 cp "$LOCK_FILE" /home/ray/lock_file.lock
fi
EOF

ARG POST_BUILD_SCRIPT

COPY "$POST_BUILD_SCRIPT" /tmp/post_build_script.sh
RUN /tmp/post_build_script.sh
