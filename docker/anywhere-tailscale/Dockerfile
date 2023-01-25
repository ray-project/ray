ARG IMAGETYPE=cpu
FROM rayproject/ray:2.2.0-py38-$IMAGETYPE
ENV __MODIN_AUTOIMPORT_PANDAS__=1 \
        MODIN_ENGINE=ray \
        MODIN_RAY_CLUSTER=True \
        TZ=America/Los_Angeles \
        NODETYPE=edge \
        TSKEY="" \
        TSAPIKEY="" \
        CFLAGS="-O3" \
        AWS_ACCESS_KEY_ID="" \
        AWS_SECRET_ACCESS_KEY="" \
        AWS_DEFAULT_REGION="us-west-2" \
        AWS_S3_BUCKET="" \
        CRATE_GC_LOG_DIR="/data/crate/log" \
        CRATE_HEAP_DUMP_PATH="/data/crate/data" \
        CRATE_JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle -Xtune:virtualized -Des.cgroups.hierarchy.override=/ $CRATE_JAVA_OPTS" \
        PATH=/crate/bin/:$PATH \
        CRATE_HEAP_SIZE=4G \
        CRATE_HOME=/crate/bin/ \
        CRATE_USE_IPV4=true \
        DISABLE_IPV6=true \
        IPv6=false
#        TS_STATEDIR="/data/tailscale/" \
#        TS_STATE="tailscale.state"

RUN sudo apt update \
        && sudo apt install -y --no-install-recommends ca-certificates curl bc wget jq lsb-release \
        && sudo apt clean \
        && sudo rm -rf /var/lib/apt/lists/*

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin[dask] \
    modin[ray] \
    'ray>=2.2.0'

#installing tailscale

RUN curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.noarmor.gpg | sudo tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null \
        && curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.tailscale-keyring.list | sudo tee /etc/apt/sources.list.d/tailscale.list \
        && sudo apt update \
        && sudo apt install -y --no-install-recommends tailscale \
        && sudo apt clean \
        && sudo rm -rf /var/lib/apt/lists/* \
        && sudo mkdir -p /var/run/tailscale /var/cache/tailscale /var/lib/tailscale

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
        && curl -fSL -O "https://raw.githubusercontent.com/jcoffi/docker-crate/master/config/crate.yml" \
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

VOLUME /data
RUN sudo mkdir -p $CRATE_HEAP_DUMP_PATH $CRATE_GC_LOG_DIR $TS_STATEDIR

##building juicefs
RUN JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v') && \
  wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  tar -zxf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  sudo install juicefs /usr/bin && \
  rm juicefs "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"



COPY startup.sh /home/ray/startup.sh
RUN sudo chmod +x /home/ray/startup.sh \
&& sudo sysctl -w vm.max_map_count=262144 \
&& sudo sysctl -w net.ipv6.conf.all.disable_ipv6=1 \
&& sudo sysctl -w net.ipv6.conf.default.disable_ipv6=1 \
&& sudo sysctl -w net.ipv6.conf.lo.disable_ipv6=1

ENTRYPOINT ["/bin/bash", "/home/ray/startup.sh"]
HEALTHCHECK --interval=300s --timeout=5s --start-period=300s \
CMD tailscale status -json | jq -r .BackendState | grep -q "Running" || exit 1