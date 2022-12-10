FROM ubuntu:focal AS build-env
ENV TZ=America/Los_Angeles
RUN apt-get update
RUN DEBIAN_FRONTEND=noninteractive apt-get install --assume-yes --no-install-recommends ca-certificates build-essential libssl-dev libzstd-dev git cmake linux-headers-virtual libpthread*

WORKDIR /root

RUN cd /root && \
git clone --depth 1 --branch 3.1.1 https://github.com/ntop/n2n.git && \
cd n2n && mkdir build && cd build && \
cmake .. "-DN2N_OPTION_USE_OPENSSL=ON" "-DN2N_OPTION_USE_ZSTD=ON" "-DN2N_OPTION_USE_PTHREAD=ON" && \
cmake --build . && \
cmake --install .






#FROM rayproject/ray:latest
FROM rayproject/ray:2.1.0-py38-gpu

ENV __MODIN_AUTOIMPORT_PANDAS__=1
ENV MODIN_ENGINE=ray
ENV MODIN_RAY_CLUSTER=True
ENV NODETYPE=head
ENV DDNS_LOGIN=username
ENV KEY=randomstring
ENV DDNS_PASSWORD=password
ENV DDNS_HOST=nexus.usethetools.com
ENV N2N_COMMUNITY=community
ENV RANDOMSTRING=ZXCVBA
# Needed to run Tune example with a 'plot' call - which does not actually render a plot, but throws an error.
RUN sudo apt-get update && sudo apt-get install -y net-tools openssl libzstd1 dyndns && sudo rm -rf /var/lib/apt/lists/* && sudo apt-get clean
RUN $HOME/anaconda3/bin/pip --no-cache-dir install -U pip \
    pandas \
    modin \
    dask[dataframe]==2022.2.0

RUN $(if [ "$NODETYPE" = "head" ]; then echo 'dyndns --login ${DDNS_LOGIN} --password ${DDNS_PASSWORD} --host ${DDNS_HOST} --system custom' | sudo tee -a /root/.bashrc; fi)

COPY --from=build-env /usr/local/sbin/supernode /usr/bin/
COPY --from=build-env /usr/local/sbin/edge /usr/sbin/
COPY --from=build-env /root/n2n/packages/etc/n2n /etc/n2n

#UDP management
EXPOSE 5645
#supernode port
EXPOSE 7654/udp

#CMD ["/bin/sh"]
CMD ["/usr/bin/supernode" "-c=${N2N_COMMUNITY}" "-f" "-AS" "-l=${DDNS_HOST}" "-r" "-b" "-v" "-d n2n0"]


