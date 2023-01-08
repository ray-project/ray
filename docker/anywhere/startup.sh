#!/bin/bash

# Generate a random string
RANDOMSTRING=$(openssl rand -hex 16)

# Pull external IP
IPADDRESS=$(curl -s http://ifconfig.me/ip)
export IPADDRESS=$IPADDRESS
echo $IPADDRESS > eth0-dyndns-custom-${DDNS_HOST}.log

# Export the random string as an environment variable
export RANDOMSTRING=$RANDOMSTRING


# Create named pipes
edgepipe=/tmp/f$RANDOMSTRING
dhcppipe=/tmp/d$RANDOMSTRING
mkfifo $edgepipe
mkfifo $dhcppipe

# If NODETYPE is "head", run the supernode command and append some text to .bashrc
if [ "$NODETYPE" = "head" ]; then
    wget http://${DDNS_LOGIN}:${DDNS_PASSWORD}@members.dyndns.org/nic/update?system=custom&hostname=${DDNS_HOST}&myip=${IPADDRESS}&wildcard=OFF&backmx=NO&offline=NO > /tmp/wget.log &
    #dyndns --login ${DDNS_LOGIN} --password ${DDNS_PASSWORD} --host ${DDNS_HOST} --system custom --urlping http://ifconfig.me/ip --urlping-regexp "([0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)"
    supernode -f > /tmp/n2n.log &
fi

#static not yet tested
if [ -n $STATIC_IP ]
then
    nohup sudo edge -c ${N2N_COMMUNITY} -k ${N2N_KEY} -l ${DDNS_HOST}:${N2N_SUPERNODE_PORT} -f -r > /tmp/n2n.log &
#    n2n_interface_ip=`/sbin/ifconfig ${N2N_INTERFACE}|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
#    while [ -z "${n2n_interface_ip}" ]
#    do
#        n2n_interface_ip=`/sbin/ifconfig ${N2N_INTERFACE}|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
#        dhclient ${N2N_INTERFACE}
#    done
else
    sudo edge -d ${N2N_INTERFACE} -a $STATIC_IP -c ${N2N_COMMUNITY} -k ${N2N_KEY} -l ${DDNS_HOST} -f -r > /tmp/n2n.log &
fi

set -ae

# GC logging set to default value of path.logs
CRATE_GC_LOG_DIR="/data/log"
CRATE_HEAP_DUMP_PATH="/data/data"
# Make sure directories exist as they are not automatically created
# This needs to happen at runtime, as the directory could be mounted.
mkdir -pv $CRATE_GC_LOG_DIR $CRATE_HEAP_DUMP_PATH

# Special VM options for Java in Docker
CRATE_JAVA_OPTS="-Des.cgroups.hierarchy.override=/ $CRATE_JAVA_OPTS"

/crate/bin/crate -Cnetwork.host=_${N2N_INTERFACE}_ \
            -Cnode.name=${DDNS_HOST} \
            -Ccluster.initial_master_nodes=${DDNS_HOST} \
            -Cgateway.expected_data_nodes=1 \
            -Cgateway.recover_after_data_nodes=1 \
            &

tail -f /tmp/n2n.log