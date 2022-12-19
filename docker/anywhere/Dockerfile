FROM ubuntu:focal AS build-env
ARG DEBIAN_FRONTEND=noninteractive
ENV TZ=America/Los_Angeles
RUN apt-get update
RUN apt-get install --assume-yes --no-install-recommends ca-certificates curl wget build-essential libssl-dev libzstd-dev git cmake linux-headers-virtual libpthread*
WORKDIR /root
## building n2n
RUN cd /root && \
git clone --depth 1 --branch 3.1.1 https://github.com/ntop/n2n.git && \
cd n2n && mkdir build && cd build && \
cmake .. "-DN2N_OPTION_USE_OPENSSL=ON" "-DN2N_OPTION_USE_ZSTD=ON" "-DN2N_OPTION_USE_PTHREAD=ON" && \
cmake --build . && \
cmake --install .
## building crate
RUN cd /root && mkdir -p crate \
#    groupadd crate \
#    && useradd -u 1000 -g crate -d crate crate \
    && export PLATFORM="$( \
        case $(uname --m) in \
            x86_64)  echo x64_linux ;; \
            aarch64) echo aarch64_linux ;; \
        esac)" \
    && export CRATE_URL=https://cdn.crate.io/downloads/releases/cratedb/${PLATFORM}/crate-5.1.1.tar.gz \
    && curl -fSL -O ${CRATE_URL} \
#    && curl -fSL -O ${CRATE_URL}.asc \
#    && export GNUPGHOME="$(mktemp -d)" \
#    && gpg --keyserver hkp://keyserver.ubuntu.com:80 --recv-keys 90C23FC6585BC0717F8FBFC37FAAE51A06F6EAEB \
#    && gpg --batch --verify crate-5.1.1.tar.gz.asc crate-5.1.1.tar.gz \
#    && rm -rf "$GNUPGHOME" crate-5.1.1.tar.gz.asc \
    && tar -xf crate-5.1.1.tar.gz -C crate --strip-components=1 \
    && rm crate-5.1.1.tar.gz


#FROM rayproject/ray:latest
FROM rayproject/ray:2.1.0-py38-gpu

ARG DEBIAN_FRONTEND=noninteractive
ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True
ENV NODETYPE=head
ENV DDNS_LOGIN=username
ENV KEY=randomstring
ENV DDNS_PASSWORD=password
ENV DDNS_HOST=nexus.usethetools.com

ENV N2N_COMMUNITY=community
ENV N2N_FEDERATION=clusteranywhere
RUN RANDOMSTRING=$(openssl rand -base64 8)
ENV RANDOMSTRING=$RANDOMSTRING



RUN sudo apt-get update && sudo apt-get install -y curl wget net-tools openssl libzstd1 dyndns && sudo rm -rf /var/lib/apt/lists/* && sudo apt-get clean
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin \
    dask[dataframe]==2022.2.0

RUN $(if [ "$NODETYPE" = "head" ]; then echo 'dyndns --login ${DDNS_LOGIN} --password ${DDNS_PASSWORD} --host ${DDNS_HOST} --system custom' | sudo tee -a /root/.bashrc; fi)

COPY --from=build-env /usr/local/sbin/supernode /usr/bin/
COPY --from=build-env /usr/local/sbin/edge /usr/sbin/
COPY --from=build-env /root/n2n/packages/etc/n2n /etc/n2n

### begin crate
COPY --from=build-env /root/crate /crate

ENV PATH /crate/bin:$PATH
ENV CRATE_HEAP_SIZE 512M
RUN sudo mkdir -p /data/data /data/log && sudo groupadd crate && sudo chgrp -hR crate /crate && sudo chmod -R 777 /crate

VOLUME /data

COPY --from=build-env --chown=1000:0 /root/crate/config/crate.yml /crate/config/crate.yml
COPY --from=build-env --chown=1000:0 /root/crate/config/log4j2.properties /crate/config/log4j2.properties

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


#CMD ["/usr/bin/supernode" "-AS" "-l=${DDNS_HOST}" "-r" "-b" "-v" "-d n2n0"]
