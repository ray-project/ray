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

# Make sure directories exist as they are not automatically created
# This needs to happen at runtime, as the directory could be mounted.
mkdir -pv $CRATE_GC_LOG_DIR $CRATE_HEAP_DUMP_PATH
# Special VM options for Java in Docker

if [ -c /dev/net/tun ]; then
    sudo tailscaled &
    sudo tailscale up --authkey=${TSKEY} --accept-risk=all --accept-routes --accept-dns
else
    echo "tun doesn't exist"
    sudo tailscaled --tun=userspace-networking --state=mem: &
    sudo tailscale up --authkey=${TSKEY} --accept-risk=all --accept-routes --accept-dns
fi

while [ not $status = "Running" ]
    do 
        echo "Waiting for tailscale to start..."
        status="$(tailscale status -json | jq -r .BackendState)"
done



$deviceid = curl -s -u "${TSAPIKEY}:" https://api.tailscale.com/api/v2/tailnet/jcoffi.github/devices | jq '.devices[] | select(.hostname=="'"$HOSTNAME"'")' | jq .id

##begin shutdown script injection
#shutting down crate gracefully
echo -e "#! /bin/bash\n" | sudo tee /etc/rc6.d/K00shutdownscript >/dev/null && sudo chmod +x /etc/rc6.d/K00shutdownscript
echo -e "/usr/local/bin/crash -c \"SET GLOBAL TRANSIENT 'cluster.routing.allocation.enable' = 'new_primaries';\"\n" | sudo tee /etc/rc6.d/K00shutdownscript >/dev/null
echo -e "/usr/local/bin/crash -c \"ALTER CLUSTER DECOMMISSION '$HOSTNAME';\"\n" | sudo tee /etc/rc6.d/K00shutdownscript >/dev/null

#making sure we delete our machine from tailscale on shutdown
echo -e "curl -X DELETE https://api.tailscale.com/api/v2/device/${deviceid} -u ${TSAPIKEY}:\n" | sudo tee /etc/rc6.d/K00shutdownscript >/dev/null


# If NODETYPE is "head", run the supernode command and append some text to .bashrc
if [ "$NODETYPE" = "head" ]; then

ray start --head --num-cpus=0 --num-gpus=0 --disable-usage-stats --dashboard-host 0.0.0.0 --node-ip-address nexus.chimp-beta.ts.net

/crate/bin/crate -Cnetwork.host=_tailscale0_ \
            -Cnode.name=nexus \
            -Cnode.master=true \
            -Cnode.data=true \
            -Cnode.store.allow_mmap=false \
            -Cdiscovery.seed_hosts=nexus:4300 \
            -Ccluster.initial_master_nodes=nexus \
            -Ccluster.graceful_stop.min_availability=primaries \
            -Cstats.enabled=false
            

else

ray start --address='nexus.chimp-beta.ts.net:6379' --node-ip-address ${HOSTNAME}.chimp-beta.ts.net

/crate/bin/crate -Cnetwork.host=_tailscale0_ \
            -Cnode.data=true \
            -Cnode.store.allow_mmap=false \
            -Cdiscovery.seed_hosts=nexus:4300 \
            -Ccluster.initial_master_nodes=nexus \
            -Ccluster.graceful_stop.min_availability=primaries \
            -Cstats.enabled=false
            
fi


#CREATE REPOSITORY s3backup TYPE s3
#[ WITH (parameter_name [= value], [, ...]) ]
#[ WITH (access_key = ${AWS_ACCESS_KEY_ID}, secret_key = ${AWS_SECRET_ACCESS_KEY}), endpoint = s3.${AWS_DEFAULT_REGION}.amazonaws.com, bucket = ${AWS_S3_BUCKET}, base_path=crate/ ]
#




