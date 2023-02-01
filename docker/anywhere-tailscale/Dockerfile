ARG IMAGETYPE=cpu
FROM rayproject/ray:2.2.0-py38-$IMAGETYPE
ENV __MODIN_AUTOIMPORT_PANDAS__=1 \
        MODIN_ENGINE=ray \
        MODIN_RAY_CLUSTER=True \
        RAY_IGNORE_UNHANDLED_ERRORS=1 \
        RAY_ADDRESS="nexus.chimp-beta.ts.net:6379" \
        RAY_CLIENT_RECONNECT_GRACE_PERIOD=60 \
        TZ=America/Los_Angeles \
        NODETYPE=edge \
        TSKEY="" \
        TSAPIKEY="" \
        AWS_ACCESS_KEY_ID="" \
        AWS_SECRET_ACCESS_KEY="" \
        AWS_DEFAULT_REGION="us-west-2" \
        AWS_S3_BUCKET="cluster-anywhere" \
        CRATE_GC_LOG_DIR="/data/crate/log" \
        CRATE_HEAP_DUMP_PATH="/data/crate/data" \
        CRATE_JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle -Xtune:virtualized -Des.cgroups.hierarchy.override=/ $CRATE_JAVA_OPTS" \
        PATH=/crate/bin/:$PATH \
        CRATE_HEAP_SIZE=5G \
        CRATE_HOME=/crate/bin/ \
        CRATE_USE_IPV4=true \
        DISABLE_IPV6=true \
        IPv6=false \
        SQLALCHEMY_SILENCE_UBER_WARNING=1
#        TS_STATEDIR="/data/tailscale/" \
#        TS_STATE="tailscale.state"
SHELL ["/bin/bash", "-o", "pipefail", "-c"]
RUN sudo rm -f /etc/apt/apt.conf.d/docker-clean; echo 'Binary::apt::APT::Keep-Downloaded-Packages "true";' | sudo tee /etc/apt/apt.conf.d/keep-cache
RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
        sudo apt update \
        && sudo apt install -y --no-install-recommends ca-certificates curl bc wget jq lsb-release

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    'dask==2022.2.0' \
    modin[dask] \
    modin[ray] \
    'ray>=2.2.0' \
    fredapi \
    sympy \
    numba \
    numpy \
    datetime \
    icecream \
    crate \
    geojson \
    sqlalchemy \
    crate[sqlalchemy] \
    alpaca_py

    # dask version compatible with ray and modin


#installing tailscale

RUN --mount=type=cache,target=/var/cache/apt,sharing=locked \
    --mount=type=cache,target=/var/lib/apt,sharing=locked \
    curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.noarmor.gpg | sudo tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null \
        && curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.tailscale-keyring.list | sudo tee /etc/apt/sources.list.d/tailscale.list \
        && sudo apt update \
        && sudo apt install -y --no-install-recommends tailscale \
        && sudo mkdir -p /var/run/tailscale /var/cache/tailscale /var/lib/tailscale
#        && sudo apt clean \
#        && sudo rm -rf /var/lib/apt/lists/* \

### begin installing crate
RUN sudo groupadd crate \
        && export PLATFORM="$( case $(uname --m) in x86_64) echo x64_linux ;; aarch64) echo aarch64_linux ;; esac)" \
        && sudo mkdir -p /crate \
        && sudo adduser ray crate \
        && sudo chown -R ray /crate \
        && sudo chgrp -R crate /crate \
        && export CRATE_URL=https://cdn.crate.io/downloads/releases/cratedb/${PLATFORM}/crate-5.1.2.tar.gz \
        && curl -fSL -O ${CRATE_URL} \
        && curl -fSL -O ${CRATE_URL}.asc \
        && curl -fSL -O "https://raw.githubusercontent.com/jcoffi/docker-crate/fb90674b15c6d7fac738a112458a18d72c4b5a77/config/crate.yml" \
        && curl -fSL -O "https://raw.githubusercontent.com/jcoffi/docker-crate/master/config/log4j2.properties" \
        && sudo tar -xf crate-5.1.2.tar.gz -C /crate --strip-components=1 \
        && sudo chown 1000 crate.yml \
        && sudo mv crate.yml /crate/config/crate.yml \
        && sudo chown 1000 log4j2.properties \
        && sudo mv log4j2.properties /crate/config/log4j2.properties \
        && rm crate-5.1.2.tar.gz \
        && curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0 \
        && curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0.asc \
        && sudo mv crash_standalone_0.28.0 /usr/local/bin/crash \
        && sudo chmod +x /usr/local/bin/crash

#for direct connections from tailscale
EXPOSE 41641/udp
EXPOSE 4300

VOLUME /data
RUN sudo mkdir -p $CRATE_HEAP_DUMP_PATH $CRATE_GC_LOG_DIR $TS_STATEDIR

##building juicefs
#RUN JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v') && \
#  wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
#  tar -zxf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
#  sudo install juicefs /usr/bin && \
#  rm juicefs "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"

##COPY startup.sh /home/ray/startup.sh
##COPY run_tests.sh /home/ray/run_tests.sh

RUN curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/startup.sh" \
&& sudo chmod +x /home/ray/startup.sh \
&& curl -fSL -O "https://raw.githubusercontent.com/jcoffi/cluster-anywhere/master/docker/anywhere-tailscale/run_tests.sh" \
&& sudo chmod +x /home/ray/run_tests.sh \
&& echo "net.ipv6.conf.all.disable_ipv6=1" | sudo tee -a /etc/sysctl.conf \
&& echo "net.ipv6.conf.default.disable_ipv6=1" | sudo tee -a /etc/sysctl.conf \
&& echo "net.ipv6.conf.lo.disable_ipv6=1" | sudo tee -a /etc/sysctl.conf


ENTRYPOINT ["/bin/bash", "/home/ray/startup.sh"]
HEALTHCHECK --interval=300s --timeout=60s --start-period=300s \
CMD curl -s localhost:4200 | jq -r '.ok' | grep -q true && ray list nodes -f NODE_NAME=$HOSTNAME.chimp-beta.ts.net -f STATE=ALIVE | grep -q ALIVE
#CMD sh /home/ray/run_tests.sh