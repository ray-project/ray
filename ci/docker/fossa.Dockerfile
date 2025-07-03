# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

RUN <<EOF
#!/bin/bash

set -euo pipefail

mkdir -p "$HOME/fossa"

curl -sSfL https://github.com/fossas/fossa-cli/releases/download/v3.10.6/fossa_3.10.6_linux_amd64.tar.gz \
    | tar -xvzf - -C "$HOME/fossa"

EOF

CMD ["echo", "ray fossa"]
