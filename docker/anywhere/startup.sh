#!/bin/bash
set -x
edgepipe=/tmp/f$RANDOM
dhcppipe=/tmp/d$RANDOM
mkfifo $edgepipe
mkfifo $dhcppipe
if [ "$NODETYPE" = "head" ]
then
    RANDOMSTRING=$(openssl rand -base64 8)
    export RANDOMSTRING=$RANDOMSTRING
    supernode -l $N2N_SUPERNODE_PORT -f
    echo -e "dyndns --login ${DDNS_LOGIN} --password ${DDNS_PASSWORD} --host ${DDNS_HOST} --system custom --urlping-dyndns\n" >> ~/.bashrc

fi
if [ -n $STATIC_IP ]
then
    nohup edge -d $N2N_INTERFACE -a dhcp:0.0.0.0 -c $N2N_COMMUNITY -k $N2N_KEY -l ${DDNS_HOST} -f -r > /tmp/n2n.log &
    n2n_interface_ip=`/sbin/ifconfig $N2N_INTERFACE|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    while [ -z "${n2n_interface_ip}" ]
    do
        n2n_interface_ip=`/sbin/ifconfig $N2N_INTERFACE|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
        dhclient $N2N_INTERFACE
    done
else
    edge -d $N2N_INTERFACE -a $STATIC_IP -c $N2N_COMMUNITY -k $N2N_KEY -l ${DDNS_HOST} -f -r
fi
