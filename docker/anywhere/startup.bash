#!/bin/bash
set -x
edgepipe=/tmp/f$RANDOM
dhcppipe=/tmp/d$RANDOM
mkfifo $edgepipe
mkfifo $dhcppipe
if [ -n $SUPERNODE ]
then
    supernode -l $N2N_SUPERNODE_PORT -f
fi
if [ -n $STATIC_IP ]
then
    nohup edge -d $N2N_INTERFACE -a dhcp:0.0.0.0 -c $N2N_GROUP -k $N2N_PASS -l $N2N_SERVER -f -r > /workdir/n2n.log &
    n2n_interface_ip=`/sbin/ifconfig $N2N_INTERFACE|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
    while [ -z "${n2n_interface_ip}" ]
    do
        n2n_interface_ip=`/sbin/ifconfig $N2N_INTERFACE|grep inet|grep -v 127.0.0.1|grep -v inet6|awk '{print $2}'|tr -d "addr:"`
        dhclient $N2N_INTERFACE
    done
    tail -f /workdir/n2n.log
else
    edge -d $N2N_INTERFACE -a $STATIC_IP -c $N2N_GROUP -k $N2N_PASS -l $N2N_SERVER -f -r
fi