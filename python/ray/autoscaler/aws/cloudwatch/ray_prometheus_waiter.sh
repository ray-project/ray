#!/bin/bash

MAX_ATTEMPTS=120
DELAY_SECONDS=10
RAY_PROM_METRICS_FILE_PATH="/tmp/ray/prom_metrics_service_discovery.json"
CLUSTER_NAME=$1
while [ $MAX_ATTEMPTS -gt 0 ]; do
  if [ -f $RAY_PROM_METRICS_FILE_PATH ]; then
    echo "Ray Prometheus metrics service discovery file found at: $RAY_PROM_METRICS_FILE_PATH."
    echo "Restarting cloudwatch agent.This may take a few minutes..."
    sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -m ec2 -a stop
    echo "Cloudwatch agent stopped, starting cloudwatch agent..."
    sudo /opt/aws/amazon-cloudwatch-agent/bin/amazon-cloudwatch-agent-ctl -a fetch-config -m ec2 -s -c "ssm:AmazonCloudWatch-ray_agent_config_$CLUSTER_NAME"
    echo "Cloudwatch agent successfully restarted!"
    exit 0
  else
    echo "Ray Prometheus metrics service discovery file not found at: $RAY_PROM_METRICS_FILE_PATH. Will check again in $DELAY_SECONDS seconds..."
    sleep $DELAY_SECONDS
    MAX_ATTEMPTS=$((MAX_ATTEMPTS-1))
  fi
done
echo "Ray Prometheus metrics service discovery file not found at: $RAY_PROM_METRICS_FILE_PATH. Ray system metrics will not be available in CloudWatch."
exit 1
