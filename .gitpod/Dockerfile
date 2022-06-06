FROM ubuntu:20.10
ENV SHELL=/bin/bash
ENV TERM=xterm-256color
ENV LANG=C.UTF-8
ENV LC_TERMINAL=iTerm2
ENV LC_TERMINAL_VERSION=3.4.4
ENV SKIP_THIRDPARTY_INSTALL=1
ARG DEBIAN_FRONTEND=noninteractive
RUN set -x; apt update \
    && ln -fs /usr/share/zoneinfo/America/Los_Angeles /etc/localtime \
    && apt install emacs gdb wget npm git build-essential curl unzip zip psmisc curl gnupg python3 pip iptables ycmd -y \
    && dpkg-reconfigure --frontend noninteractive tzdata \
    && apt install default-jre default-jdk clang rtags tmux clang-format shellcheck cmake autogen python-dev automake autoconf libtool jq -y \
    && curl -fsSL https://bazel.build/bazel-release.pub.gpg | gpg --dearmor > bazel.gpg \
    && mv bazel.gpg /etc/apt/trusted.gpg.d/ \
    && echo "deb [arch=amd64] https://storage.googleapis.com/bazel-apt stable jdk1.8" | tee /etc/apt/sources.list.d/bazel.list \
    && apt update && apt install bazel-3.7.2 -y \
    && pip3 install cython==0.29.26 pytest pandas tree tabulate pexpect sklearn joblib black==21.12b0 flake8==3.9.1 mypy==0.782 flake8-quotes flake8-bugbear==21.9.2 setproctitle==1.1.10 psutil yq \
    && python3 -c  'print("startup --output_base=/workspace/ray/.bazel-cache\nstartup --host_jvm_args=-Xmx1800m\nbuild --jobs=6")' > /etc/bazel.bazelrc

RUN update-alternatives --install /usr/local/bin/python python /usr/bin/python3 30 \
    && update-alternatives --install /usr/bin/bazel bazel /usr/bin/bazel-3.7.2 30 \
    && echo "kernel.yama.ptrace_scope = 0" > /etc/sysctl.d/10-ptrace.conf
