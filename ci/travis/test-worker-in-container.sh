#!/usr/bin/env bash

set -euxo pipefail

export STORAGE_DRIVER=vfs
podman load --input /var/lib/containers/images.tar

cat > /ray/docker/ray-nest-container/test-Dockerfile << EOF
FROM rayproject/ray-nest-container:nightly-py36-cpu
RUN $HOME/anaconda3/bin/pip --no-cache-dir install pandas
EOF

podman build -f /ray/docker/ray-nest-container/test-Dockerfile -t rayproject/ray-nest-container:nightly-py36-cpu-pandas .

export RAY_BACKEND_LOG_LEVEL=debug
"$HOME/anaconda3/bin/pip" install --no-cache-dir pytest
pytest /ray/python/ray/tests/test_actor_in_container.py -s

