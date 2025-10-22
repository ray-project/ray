ARG DOCKER_IMAGE_RAY_CORE=cr.ray.io/rayproject/ray-core-py3.9
ARG DOCKER_IMAGE_RAY_DASHBOARD=cr.ray.io/rayproject/ray-dashboard
ARG DOCKER_IMAGE_BASE_BUILD=cr.ray.io/rayproject/oss-ci-base_build

FROM $DOCKER_IMAGE_RAY_CORE AS ray_core
FROM $DOCKER_IMAGE_RAY_DASHBOARD AS ray_dashboard

FROM $DOCKER_IMAGE_BASE_BUILD

COPY . .

SHELL ["/bin/bash", "-ice"]

RUN --mount=type=bind,from=ray_core,target=/mnt/ray-core \
    --mount=type=bind,from=ray_dashboard,target=/mnt/ray-dashboard \
    <<EOF
#!/bin/bash

set -euo pipefail

mkdir -p /opt/ray-build

cp /mnt/ray-core/ray_pkg.zip /opt/ray-build/ray_pkg.zip
cp /mnt/ray-core/ray_py_proto.zip /opt/ray-build/ray_py_proto.zip
cp /mnt/ray-dashboard/dashboard.tar.gz /opt/ray-build/dashboard.tar.gz

pip install -r python/deplocks/docs/docbuild_depset_py${PYTHON}.lock

EOF
