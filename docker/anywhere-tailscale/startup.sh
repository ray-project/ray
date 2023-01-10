#!/bin/bash

# Generate a random string
RANDOMSTRING=$(openssl rand -hex 16)

# Pull external IP
IPADDRESS=$(curl -s http://ifconfig.me/ip)
export IPADDRESS=$IPADDRESS

# Export the random string as an environment variable
export RANDOMSTRING=$RANDOMSTRING

set -ae

# GC logging set to default value of path.logs
CRATE_GC_LOG_DIR="/data/log"
CRATE_HEAP_DUMP_PATH="/data/data"
# Make sure directories exist as they are not automatically created
# This needs to happen at runtime, as the directory could be mounted.
mkdir -pv $CRATE_GC_LOG_DIR $CRATE_HEAP_DUMP_PATH

# Special VM options for Java in Docker
CRATE_JAVA_OPTS="-Des.cgroups.hierarchy.override=/ $CRATE_JAVA_OPTS"

sudo tailscaled &


# If NODETYPE is "head", run the supernode command and append some text to .bashrc
if [ "$NODETYPE" = "head" ]; then

    sudo tailscale up --authkey=tskey-auth-kTSQbo3CNTRL-bWzNQtfVbgfmqTbd9zc5mffSAWJoMLLTB --accept-risk=all --accept-routes --hostname=nexus --accept-dns

    while [ not $status = "Running" ]
    do 
        status="$(tailscale status -json | jq -r .BackendState)"
    done

nexus=$(tailscale ip -4 nexus)

/crate/bin/crate -Cnetwork.host=_${N2N_INTERFACE}_ \
            -Cnode.name=nexus \
            -Cnode.master=true \
            -Cnode.data=true \
            -Cdiscovery.seed_hosts=nexus.chimp-beta.ts.net,$nexus \
            -Ccluster.initial_master_nodes=nexus,$nexus \
            -Cstats.enabled=false
            

else

sudo tailscale up --authkey=tskey-auth-kTSQbo3CNTRL-bWzNQtfVbgfmqTbd9zc5mffSAWJoMLLTB --accept-risk=all --accept-routes --accept-dns

while [ not $status = "Running" ]
do 
    status="$(tailscale status -json | jq -r .BackendState)"
done

nexus=$(tailscale ip -4 nexus)

/crate/bin/crate -Cnetwork.host=_${N2N_INTERFACE}_ \
            #-Cnode.name=${DDNS_HOST} \
            -Cnode.data=true \
            -Cdiscovery.seed_hosts=nexus.chimp-beta.ts.net,$nexus \
            -Ccluster.initial_master_nodes=nexus,$nexus \
            -Cstats.enabled=false
            
fi






