#!/bin/bash

# Generate a random string
RANDOMSTRING=$(openssl rand -hex 16)

# Pull external IP
IPADDRESS=$(curl -s http://ifconfig.me/ip)
export IPADDRESS=$IPADDRESS

# Export the random string as an environment variable
export RANDOMSTRING=$RANDOMSTRING


# Create named pipes
edgepipe=/tmp/f$RANDOMSTRING
dhcppipe=/tmp/d$RANDOMSTRING
mkfifo $edgepipe
mkfifo $dhcppipe

# If NODETYPE is "head", run the supernode command and append some text to .bashrc
if [ "$NODETYPE" = "head" ]; then
sudo tailscaled &
sudo tailscale up --authkey=tskey-auth-kTSQbo3CNTRL-bWzNQtfVbgfmqTbd9zc5mffSAWJoMLLTB --accept-risk=all --hostname=nexus --accept-dns
else
sudo tailscaled &
sudo tailscale up --authkey=tskey-auth-kTSQbo3CNTRL-bWzNQtfVbgfmqTbd9zc5mffSAWJoMLLTB --accept-risk=all --accept-dns
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
            #-Cnode.name=${DDNS_HOST} \
            -Cdiscovery.type=zen
            -Ccluster.initial_master_nodes=nexus \
            -Cgateway.expected_data_nodes=2 \
            -Cgateway.recover_after_data_nodes=1 \
            &
#need to make it so that we discover and connect via n2n ips
