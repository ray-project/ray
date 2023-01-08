FROM rayproject/ray:2.1.0-py38-gpu
ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True
ENV TZ=America/Los_Angeles
ENV NODETYPE=head

ENV CFLAGS="-O3"

ENV DDNS_LOGIN=username
ENV DDNS_PASSWORD=password
ENV DDNS_HOST=nexus.usethetools.com

ENV N2N_COMMUNITY=community
ENV N2N_FEDERATION=clusteranywhere
ENV N2N_SUPERNODE_PORT=7654
ENV N2N_INTERFACE=edge0
ENV N2N_KEY=$DDNS_HOST

#the $RANDOMSTRING variable is defined in the startup script
#ENV N2N_KEY=$RANDOMSTRING

RUN sudo apt-get update  \
        && sudo apt-get install -y --no-install-recommends ca-certificates curl wget build-essential libssl-dev libzstd-dev git cmake linux-headers-virtual net-tools openssl software-properties-common lsb-release libzstd1 \ 
        && sudo apt-get clean  \
        && sudo rm -rf /var/lib/apt/lists/*

RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin \
    dask[dataframe]==2022.2.0

# building n2n
RUN cd $HOME  \
        && git clone --depth 1 --branch 3.1.1 https://github.com/ntop/n2n.git  \
        && cd n2n  \
        && mkdir build  \
        && cd build  \
        && cmake .. "-DN2N_OPTION_USE_OPENSSL=ON" "-DN2N_OPTION_USE_ZSTD=ON"  \
        && cmake --build .  \
        && sudo cmake --install .  \
        && cd $HOME  \
        && rm -rf n2n
 
### begin installing crate
RUN sudo groupadd crate  \
        && export PLATFORM="$( case $(uname --m) in x86_64) echo x64_linux ;; aarch64) echo aarch64_linux ;; esac)"  \
        && sudo mkdir -pv /crate  \
        && sudo chgrp -R crate /crate  \
        && export CRATE_URL=https://cdn.crate.io/downloads/releases/cratedb/${PLATFORM}/crate-5.1.2.tar.gz  \
        && curl -fSL -O ${CRATE_URL}  \
        && curl -fSL -O ${CRATE_URL}.asc  \
        && curl -fSL -O "https://raw.githubusercontent.com/jcoffi/docker-crate/master/config/crate.yml"  \
        && curl -fSL -O "https://raw.githubusercontent.com/jcoffi/docker-crate/master/config/log4j2.properties"  \
        && export GNUPGHOME="$(mktemp -d)"  \
        && gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 90C23FC6585BC0717F8FBFC37FAAE51A06F6EAEB  \
        && gpg --batch --verify crate-5.1.2.tar.gz.asc crate-5.1.2.tar.gz  \
        && rm -rf "$GNUPGHOME" crate-5.1.2.tar.gz.asc  \
        && sudo tar -xf crate-5.1.2.tar.gz -C /crate --strip-components=1  \
        && sudo chown 1000 crate.yml  \
        && sudo mv crate.yml /crate/config/crate.yml  \
        && sudo chown 1000 log4j2.properties  \
        && sudo mv log4j2.properties /crate/config/log4j2.properties  \
        && rm crate-5.1.2.tar.gz  \
        && sudo sysctl -w vm.max_map_count=262144

RUN curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0  \
        && curl -fSL -O https://cdn.crate.io/downloads/releases/crash_standalone_0.28.0.asc  \
        && export GNUPGHOME="$(mktemp -d)"  \
        && gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 90C23FC6585BC0717F8FBFC37FAAE51A06F6EAEB  \
        && gpg --batch --verify crash_standalone_0.28.0.asc crash_standalone_0.28.0  \
        && rm -rf "$GNUPGHOME" crash_standalone_0.28.0.asc  \
        && sudo mv crash_standalone_0.28.0 /usr/local/bin/crash  \
        && sudo chmod +x /usr/local/bin/crash

ENV PATH /crate/bin/:$PATH
ENV CRATE_HEAP_SIZE 512M

VOLUME /data
RUN sudo mkdir -p /data/data /data/log

##building juicefs
RUN JFS_LATEST_TAG=$(curl -s https://api.github.com/repos/juicedata/juicefs/releases/latest | grep 'tag_name' | cut -d '"' -f 4 | tr -d 'v') && \
  wget "https://github.com/juicedata/juicefs/releases/download/v${JFS_LATEST_TAG}/juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  tar -zxf "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz" && \
  sudo install juicefs /usr/bin && \
  rm juicefs "juicefs-${JFS_LATEST_TAG}-linux-amd64.tar.gz"


#UDP management
EXPOSE 5645
#supernode port
EXPOSE 7654/udp
EXPOSE 7654/tcp
#crate
EXPOSE 4200/tcp 4300/tcp 5432/tcp

COPY startup.sh /home/ray/startup.sh
RUN sudo chmod +x /home/ray/startup.sh
ENTRYPOINT ["/bin/sh", "-c", "/home/ray/startup.sh"]