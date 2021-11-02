#!/bin/bash

CONFIG=config/raysort-cluster.yaml
HEAD_IP=$(ray get-head-ip $CONFIG | tail -1)

echo "Forwarding Ray Dashboard on 8265"
ray dashboard $CONFIG &

echo "Forwarding Prometheus UI on 9090"
ssh -f -N -L 9090:localhost:9090 $HEAD_IP -i ~/.ssh/ray-autoscaler_us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null

# echo "Forwarding Jaeger UI on 16686"
# ssh -f -N -L 16686:localhost:16686 $HEAD_IP -i ~/.ssh/ray-autoscaler_us-west-2.pem -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null
