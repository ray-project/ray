FROM rayproject/ray:2.1.0-py38-gpu
ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True
ENV TZ=America/Los_Angeles
ENV NODETYPE=head
ENV DDNS_LOGIN=username
ENV CFLAGS="-O3"

ENV DDNS_PASSWORD=password
ENV DDNS_HOST=nexus.usethetools.com

ENV N2N_COMMUNITY=community
ENV N2N_FEDERATION=clusteranywhere
ENV N2N_SUPERNODE_PORT=7654
#the $RANDOMSTRING variable is defined in the startup script
ENV N2N_KEY=$RANDOMSTRING

RUN sudo apt-get update && sudo apt-get install -y --no-install-recommends ca-certificates curl wget build-essential libssl-dev libzstd-dev git cmake linux-headers-virtual net-tools openssl software-properties-common lsb-release libzstd1 dyndns && sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/* 
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin \
    dask[dataframe]==2022.2.0

# building n2n
RUN cd $HOME && \
git clone --depth 1 --branch 3.1.1 https://github.com/ntop/n2n.git && \
cd n2n && mkdir build && cd build && \
cmake .. "-DN2N_OPTION_USE_OPENSSL=ON" "-DN2N_OPTION_USE_ZSTD=ON" && \
cmake --build . && \
sudo cmake --install . && cd $HOME && rm -rf n2n
 
### begin installing crate
RUN sudo apt-key adv --fetch-keys https://cdn.crate.io/downloads/deb/DEB-GPG-KEY-crate && sudo add-apt-repository "deb https://cdn.crate.io/downloads/deb/stable/ $(lsb_release -cs) main" && sudo apt-get update && sudo apt-get install crate && sudo apt-get clean && sudo rm -rf /var/lib/apt/lists/* 

ENV PATH /usr/share/crate/bin/:$PATH
ENV CRATE_HEAP_SIZE 512M

VOLUME /data

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

COPY startup.sh ~/startup.sh
ENTRYPOINT ["sh" "-c" "~/startup.sh"]
CMD ["/bin/sh" "-c"]