# syntax=docker/dockerfile:1.3-labs

FROM cr.ray.io/rayproject/forge

# Add forge user to sudo group
USER root
RUN  <<EOF
#!/bin/bash

set -euo pipefail

usermod -aG sudo forge && \
    echo "forge ALL=(ALL) NOPASSWD:ALL" >> /etc/sudoers.d/forge && \
    chmod 0440 /etc/sudoers.d/forge

EOF

USER forge

# Install fossa and askalono
RUN <<EOF
#!/bin/bash

set -euo pipefail

curl -sSfL https://github.com/fossas/fossa-cli/releases/download/v3.10.6/fossa_3.10.6_linux_amd64.tar.gz \
    | sudo tar -xvzf - -C /usr/local/bin/

wget -O /tmp/askalono.zip https://github.com/jpeddicord/askalono/releases/download/0.5.0/askalono-Linux.zip
unzip -d /tmp/ /tmp/askalono.zip
sudo mv /tmp/askalono /usr/local/bin/
rm /tmp/askalono.zip

EOF

CMD ["echo", "ray fossa"]
