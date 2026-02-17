# syntax=docker/dockerfile:1
# Conda-Forge Ray Reproducer Image
# =====================================
# Reproduces the conda-forge ray + Anyscale Connect 502 issue.
# RHEL 9 + micromamba + conda-forge ray setup.
#

FROM redhat/ubi9

ARG PYTHON_VERSION=3.11

# Install system dependencies required by Anyscale BYOD spec.
# --allowerasing needed because UBI 9 ships curl-minimal which conflicts with curl.
RUN dnf install -y --allowerasing \
    sudo bash openssh-server openssh-clients rsync zip unzip \
    git gdb curl wget bzip2 which \
    && dnf clean all

# Create the ray user per Anyscale BYOD requirements
# UID 1000, GID 100 (users group), passwordless sudo
RUN useradd -ms /bin/bash -d /home/ray ray --uid 1000 --gid 100 && \
    usermod -aG wheel ray && \
    echo 'ray ALL=NOPASSWD: ALL' >> /etc/sudoers

# Install micromamba
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | \
    tar -xvj -C /usr/local bin/micromamba && \
    mkdir -p /home/ray/micromamba && \
    chown -R ray:users /home/ray

USER ray
WORKDIR /home/ray
ENV HOME=/home/ray
ENV MAMBA_ROOT_PREFIX=/home/ray/micromamba
ENV PATH="/home/ray/micromamba/envs/ray-env/bin:${HOME}/.local/bin:${PATH}"

RUN /usr/local/bin/micromamba shell init --shell bash --root-prefix="${MAMBA_ROOT_PREFIX}" && \
    echo 'eval "$(/usr/local/bin/micromamba shell hook --shell bash)"' >> ~/.bashrc && \
    echo 'micromamba activate ray-env' >> ~/.bashrc

# Create conda environment with ray packages from conda-forge.
# This pulls protobuf 6 and links grpcio against conda-forge's libgrpc (dynamic),
# which differs from pip grpcio (statically bundled). This mismatch is likely the
# root cause of the 502.
RUN /usr/local/bin/micromamba create -n ray-env -c conda-forge \
    python=${PYTHON_VERSION} \
    "ray-core>=2.53.0,<2.54" \
    "ray-default>=2.53.0,<2.54" \
    "ray-serve>=2.53.0,<2.54" \
    -y && \
    /usr/local/bin/micromamba clean -a -f -y

# Install anyscale (--no-deps to avoid clobbering conda-forge grpcio/protobuf)
# and its remaining dependencies (deliberately omitting grpcio and protobuf).
RUN /home/ray/micromamba/envs/ray-env/bin/pip install --no-cache-dir --no-deps anyscale && \
    /home/ray/micromamba/envs/ray-env/bin/pip install --no-cache-dir \
    packaging boto3 google-cloud-storage jupyterlab terminado \
    GitPython humanize jsonpatch kubernetes oauth2client \
    "pathspec>=0.8.1" rich tabulate "termcolor>=1.1.0" tqdm tzlocal websockets

COPY --chown=ray:users reproduce_502.py /home/ray/reproduce_502.py

# Assert conda-forge grpcio/protobuf survived pip installs.
# If pip clobbered them, the bug won't reproduce.
RUN <<'EOF'
/home/ray/micromamba/envs/ray-env/bin/python -c "
import ray, grpc
from google.protobuf import __version__ as pb

print(f'Ray: {ray.__version__}, grpcio: {grpc.__version__}, protobuf: {pb}')

assert grpc.__version__.startswith('1.73'), \
    f'Expected conda-forge grpcio 1.73.x, got {grpc.__version__}'
assert pb.startswith('6.'), \
    f'Expected conda-forge protobuf 6.x, got {pb}'
"
EOF

CMD ["/bin/bash"]
