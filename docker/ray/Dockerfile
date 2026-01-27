# syntax=docker/dockerfile:1.3-labs

ARG BASE_IMAGE
ARG FULL_BASE_IMAGE=rayproject/ray-deps:nightly"$BASE_IMAGE"
FROM $FULL_BASE_IMAGE

ARG WHEEL_PATH
ARG FIND_LINKS_PATH=".whl"
ARG CONSTRAINTS_FILE="requirements_compiled.txt"

# POWER prebuild (only runs on ppc64le)
USER root
COPY docker/ray/prebuild_power.sh /tmp/prebuild_power.sh

RUN <<EOF
#!/bin/bash
set -euo pipefail

if [[ "${HOSTTYPE}" =~ ^(ppc64le|powerpc64le)$ ]]; then
    echo "POWER detected: ${HOSTTYPE}"
    echo "POWER detected: setting /home/ray permissions"
    chgrp -R 0 /home/ray
    chmod -R g+rwX /home/ray
    ls -ld /home/ray
    chmod +x /tmp/prebuild_power.sh
    /tmp/prebuild_power.sh
else
    echo "Non-POWER detected: ${HOSTTYPE}, skipping POWER prebuild"
fi
EOF

USER ray

COPY $WHEEL_PATH .
COPY $FIND_LINKS_PATH $FIND_LINKS_PATH

RUN <<EOF
#!/bin/bash
set -euo pipefail

if [[ "${HOSTTYPE}" =~ ^(ppc64le|powerpc64le)$ ]]; then
    echo "Installing Ray for POWER"
    export GRPC_PYTHON_BUILD_SYSTEM_OPENSSL=1
    $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
        --extra-index-url https://wheels.developerfirst.ibm.com/ppc64le/linux \
        `basename $WHEEL_PATH`[default,data,train,tune,serve] \
        --find-links $FIND_LINKS_PATH
else
    echo "Installing Ray for non-POWER"
    $HOME/anaconda3/bin/pip --no-cache-dir install -c $CONSTRAINTS_FILE \
        `basename $WHEEL_PATH`[all] \
        --find-links $FIND_LINKS_PATH
fi

sudo rm -f `basename $WHEEL_PATH`
EOF

RUN $HOME/anaconda3/bin/pip freeze > /home/ray/pip-freeze.txt
