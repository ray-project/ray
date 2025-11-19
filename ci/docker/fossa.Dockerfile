# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

RUN <<EOF
#!/bin/bash

set -euo pipefail

mkdir -p "$HOME/fossa"

curl -sSfL https://github.com/fossas/fossa-cli/releases/download/v3.10.6/fossa_3.10.6_linux_amd64.tar.gz \
    | tar -xvzf - -C "$HOME/fossa"

uv venv --no-project $HOME/venv && source $HOME/venv/bin/activate && uv pip install pandas==2.3.3 openpyxl==3.1.5 pyyaml==6.0.3

wget -O /tmp/askalono.zip https://github.com/jpeddicord/askalono/releases/download/0.5.0/askalono-Linux.zip
unzip -d /tmp/ /tmp/askalono.zip
mkdir -p $HOME/.local/bin/
mv /tmp/askalono $HOME/.local/bin/

EOF

CMD ["echo", "ray fossa"]
