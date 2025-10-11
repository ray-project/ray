FROM cr.ray.io/rayproject/manylinux AS builder

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
