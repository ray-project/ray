ARG HOSTTYPE=x86_64
ARG MANYLINUX_VERSION
FROM rayproject/manylinux2014:${MANYLINUX_VERSION}-jdk-${HOSTTYPE} AS builder

WORKDIR /home/forge/ray

COPY --chown=forge:users . .

RUN <<EOF
#!/bin/bash

set -euo pipefail

export PATH="/usr/local/node/bin:$PATH"

(
    cd python/ray/dashboard/client

    echo "--- npm ci"
    npm ci

    echo "--- npm run build"
    npm run build

    tar -czf /home/forge/dashboard.tar.gz -C build .
)

EOF

FROM scratch

COPY --from=builder /home/forge/dashboard.tar.gz /dashboard.tar.gz
