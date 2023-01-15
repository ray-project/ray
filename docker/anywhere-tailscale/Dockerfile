ARG IMAGETYPE=cpu
FROM rayproject/ray:latest-py38-$IMAGETYPE
ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True
ENV TZ=America/Los_Angeles
ENV NODETYPE=head
ENV TSKEY=""
ENV TSAPIKEY=""
ENV CFLAGS="-O3"
ENV AWS_ACCESS_KEY_ID=""
ENV AWS_SECRET_ACCESS_KEY=""
ENV AWS_DEFAULT_REGION="us-west-2"
ENV AWS_S3_BUCKET=""
ENV CRATE_GC_LOG_DIR="/data/log"
ENV CRATE_HEAP_DUMP_PATH="/data/data"
ENV CRATE_JAVA_OPTS="-XX:+IgnoreUnrecognizedVMOptions -XX:+UseContainerSupport -XX:+IdleTuningCompactOnIdle -XX:+IdleTuningGcOnIdle -Xtune:virtualized -Des.cgroups.hierarchy.override=/ $CRATE_JAVA_OPTS"
ENV N2N_INTERFACE=tailscale0


#the $RANDOMSTRING variable is defined in the startup script
#ENV N2N_KEY=$RANDOMSTRING

RUN curl -s https://packages.cloud.google.com/apt/doc/apt-key.gpg | sudo apt-key add \
        && sudo apt-get update \
        && sudo apt-get install -y --no-install-recommends ca-certificates curl wget lsb-release \ 
        && sudo apt-get clean \
        && sudo rm -rf /var/lib/apt/lists/*

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin[dask] \
    modin[ray]


#installing tailscale
RUN curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.noarmor.gpg | sudo tee /usr/share/keyrings/tailscale-archive-keyring.gpg >/dev/null \
        && curl -fsSL https://pkgs.tailscale.com/stable/ubuntu/focal.tailscale-keyring.list | sudo tee /etc/apt/sources.list.d/tailscale.list \
        && sudo apt-get update \
        && sudo apt-get install -y --no-install-recommends tailscale iptables dnsutils jq \
        && sudo apt-get clean \
        && sudo rm -rf /var/lib/apt/lists/* \
        && sudo mkdir -p /var/run/tailscale /var/cache/tailscale /var/lib/tailscale

### begin installing crate
RUN sudo groupadd crate \
        && export PLATFORM="$( case $(uname --m) in x86_64) echo x64_linux ;; aarch64) echo aarch64_linux ;; esac)" \
        && sudo mkdir -pv /crate \
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
        && rm crate-5.1.2.tar.gz

RUN curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0 \
        && curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0.asc \
        && sudo mv crash_standalone_0.28.0 /usr/local/bin/crash \
        && sudo chmod +x /usr/local/bin/crash

ENV PATH /crate/bin/:$PATH
ENV CRATE_HEAP_SIZE=512M
ENV CRATE_HOME=/crate/bin/
ENV CRATE_USE_IPV4=true

VOLUME /data
RUN sudo mkdir -p /data/data /data/log

##building juicefs
RUN JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v') && \
  wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  tar -zxf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  sudo install juicefs /usr/bin && \
  rm juicefs "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"


#crate
#EXPOSE 4200/tcp
#EXPOSE 4300/tcp
#EXPOSE 5432/tcp

COPY startup.sh /home/ray/startup.sh
RUN sudo chmod +x /home/ray/startup.sh
ENTRYPOINT ["/bin/bash", "/home/ray/startup.sh"]