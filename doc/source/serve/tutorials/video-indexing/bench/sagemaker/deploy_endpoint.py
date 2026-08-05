"""Create a SageMaker Async Inference endpoint + backlog-based autoscaling.

Creates: Model -> EndpointConfig (AsyncInferenceConfig) -> Endpoint (ml.g4dn.xlarge)
-> Application Auto Scaling on the backlog metric. Two modes:
  default        -> one target-tracking policy on ApproximateBacklogSizePerInstance
  --step-scaling -> two step policies + two backlog alarms (the config used in the
                    benchmark): scale straight to max on backlog, back to min when empty.

Example:
  python deploy_endpoint.py \
    --image-uri <acct>.dkr.ecr.us-west-2.amazonaws.com/video-index-sm:latest \
    --role-arn arn:aws:iam::<acct>:role/<sagemaker-exec-role> \
    --s3-bucket anyscale-test-data-cld-... \
    --s3-output s3://<bucket>/sm-bench/output/ \
    --max-instances 16
"""

import argparse
import time

import boto3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--image-uri", required=True)
    ap.add_argument("--role-arn", required=True, help="SageMaker execution role ARN")
    ap.add_argument("--s3-bucket", required=True, help="bucket for embeddings output (container env S3_BUCKET)")
    ap.add_argument("--s3-output", required=True, help="s3:// prefix for async result objects")
    ap.add_argument("--s3-failure", default=None, help="s3:// prefix for async failures (default: <output>/failures)")
    ap.add_argument("--endpoint-name", default="video-index-sm")
    ap.add_argument("--instance-type", default="ml.g4dn.xlarge")
    ap.add_argument("--min-instances", type=int, default=1)
    ap.add_argument("--max-instances", type=int, default=16)
    ap.add_argument("--max-concurrency-per-instance", type=int, default=4,
                    help="MaxConcurrentInvocationsPerInstance; 4 matches the Ray monolith's "
                         "max_ongoing_requests for an apples-to-apples run")
    ap.add_argument("--target-backlog-per-instance", type=float, default=3.0)
    ap.add_argument("--step-scaling", action="store_true",
                    help="use two step policies + two backlog alarms (fast-out to max on "
                         "backlog>=5/instance, fast-in to min on empty backlog) instead of "
                         "target-tracking")
    ap.add_argument("--model-data-url", default=None,
                    help="s3:// model.tar.gz containing code/inference.py (script mode)")
    ap.add_argument("--delete-existing", action="store_true",
                    help="delete existing endpoint/config/model first (clean redeploy)")
    ap.add_argument("--region", default="us-west-2")
    a = ap.parse_args()

    name = a.endpoint_name
    variant = "AllTraffic"
    failure = a.s3_failure or (a.s3_output.rstrip("/") + "/failures/")
    sm = boto3.client("sagemaker", region_name=a.region)

    if a.delete_existing:
        # Delete the endpoint first and wait for it to be gone: SageMaker rejects
        # deleting the config/model while an endpoint still references them.
        try:
            sm.delete_endpoint(EndpointName=name)
            print("deleting endpoint", name)
        except Exception as e:
            print("skip delete endpoint:", type(e).__name__)
        for _ in range(60):
            try:
                sm.describe_endpoint(EndpointName=name)
                time.sleep(6)
            except Exception:
                break
        for fn, kw in [(sm.delete_endpoint_config, {"EndpointConfigName": name}),
                       (sm.delete_model, {"ModelName": name})]:
            try:
                fn(**kw)
                print("deleted", list(kw.values())[0])
            except Exception as e:
                print("skip delete:", type(e).__name__)

    container = {
        "Image": a.image_uri,
        "Environment": {
            "S3_BUCKET": a.s3_bucket,
            "TS_DEFAULT_RESPONSE_TIMEOUT": "600",
            "SAGEMAKER_MODEL_SERVER_WORKERS": "2",
        },
    }
    if a.model_data_url:
        # Script mode: the DLC extracts model.tar.gz to /opt/ml/model and imports
        # code/inference.py (overrides the image's baked SAGEMAKER_SUBMIT_DIRECTORY).
        container["ModelDataUrl"] = a.model_data_url
        container["Environment"]["SAGEMAKER_PROGRAM"] = "inference.py"
        container["Environment"]["SAGEMAKER_SUBMIT_DIRECTORY"] = "/opt/ml/model/code"

    sm.create_model(ModelName=name, ExecutionRoleArn=a.role_arn, PrimaryContainer=container)
    sm.create_endpoint_config(
        EndpointConfigName=name,
        ProductionVariants=[{
            "VariantName": variant,
            "ModelName": name,
            "InstanceType": a.instance_type,
            "InitialInstanceCount": max(1, a.min_instances),
        }],
        AsyncInferenceConfig={
            "OutputConfig": {"S3OutputPath": a.s3_output, "S3FailurePath": failure},
            "ClientConfig": {"MaxConcurrentInvocationsPerInstance": a.max_concurrency_per_instance},
        },
    )
    sm.create_endpoint(EndpointName=name, EndpointConfigName=name)
    print(f"creating endpoint {name} ... (poll DescribeEndpoint for InService)")
    waiter = sm.get_waiter("endpoint_in_service")
    waiter.wait(EndpointName=name, WaiterConfig={"Delay": 30, "MaxAttempts": 60})
    print("endpoint InService")

    # Application Auto Scaling on instance count, driven by backlog per instance.
    aas = boto3.client("application-autoscaling", region_name=a.region)
    resource_id = f"endpoint/{name}/variant/{variant}"
    aas.register_scalable_target(
        ServiceNamespace="sagemaker",
        ResourceId=resource_id,
        ScalableDimension="sagemaker:variant:DesiredInstanceCount",
        MinCapacity=a.min_instances,
        MaxCapacity=a.max_instances,
    )
    # Autoscaling modes (both on the default backlog CloudWatch metric, no custom metrics):
    #   default        -> one target-tracking policy on ApproximateBacklogSizePerInstance
    #   --step-scaling -> two step policies (fast-out / fast-in) + two backlog alarms,
    #                     which is the config used for the benchmark. The step-in policy
    #                     handles scale-in, so target-tracking is not attached in this mode.
    if not a.step_scaling:
        aas.put_scaling_policy(
            PolicyName=f"{name}-backlog-tt",
            ServiceNamespace="sagemaker",
            ResourceId=resource_id,
            ScalableDimension="sagemaker:variant:DesiredInstanceCount",
            PolicyType="TargetTrackingScaling",
            TargetTrackingScalingPolicyConfiguration={
                "TargetValue": a.target_backlog_per_instance,
                "CustomizedMetricSpecification": {
                    "MetricName": "ApproximateBacklogSizePerInstance",
                    "Namespace": "AWS/SageMaker",
                    "Dimensions": [{"Name": "EndpointName", "Value": name}],
                    "Statistic": "Average",
                },
                "ScaleInCooldown": 120,
                "ScaleOutCooldown": 60,
            },
        )
        print("target-tracking policy on ApproximateBacklogSizePerInstance registered (scale-out + scale-in)")

    if a.step_scaling:
        # Fast scale-OUT: step-scaling on a 1-datapoint/60s backlog alarm ->
        # jump straight to max on any backlog. Reacts in ~1 min vs the default
        # target-tracking mode's ~3-datapoint (~3 min) alarm. (Instance
        # provisioning is still the ~5-8 min floor.)
        resp = aas.put_scaling_policy(
            PolicyName=f"{name}-fast-out",
            ServiceNamespace="sagemaker",
            ResourceId=resource_id,
            ScalableDimension="sagemaker:variant:DesiredInstanceCount",
            PolicyType="StepScaling",
            StepScalingPolicyConfiguration={
                "AdjustmentType": "ExactCapacity",
                "Cooldown": 60,
                "MetricAggregationType": "Average",
                "StepAdjustments": [{"MetricIntervalLowerBound": 0, "ScalingAdjustment": a.max_instances}],
            },
        )
        cw = boto3.client("cloudwatch", region_name=a.region)
        cw.put_metric_alarm(
            AlarmName=f"{name}-backlog-fast",
            MetricName="ApproximateBacklogSizePerInstance",
            Namespace="AWS/SageMaker",
            Dimensions=[{"Name": "EndpointName", "Value": name}],
            Statistic="Average",
            Period=60, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=5,
            ComparisonOperator="GreaterThanOrEqualToThreshold",
            AlarmActions=[resp["PolicyARN"]], TreatMissingData="notBreaching",
        )
        print(f"STEP scaling: 1-datapoint/60s backlog alarm -> ExactCapacity {a.max_instances}")

        # Symmetric fast scale-IN: own alarm on empty backlog (1x60s datapoint,
        # missing data = idle = breaching) -> ExactCapacity min. Target-tracking's
        # managed AlarmLow needs 15 datapoints and is not configurable; this custom
        # alarm is. Trade-off: quiet-but-bursty traffic re-pays the cold start.
        resp_in = aas.put_scaling_policy(
            PolicyName=f"{name}-fast-in",
            ServiceNamespace="sagemaker",
            ResourceId=resource_id,
            ScalableDimension="sagemaker:variant:DesiredInstanceCount",
            PolicyType="StepScaling",
            StepScalingPolicyConfiguration={
                "AdjustmentType": "ExactCapacity",
                "Cooldown": 60,
                "MetricAggregationType": "Average",
                "StepAdjustments": [{"MetricIntervalUpperBound": 0, "ScalingAdjustment": a.min_instances}],
            },
        )
        cw.put_metric_alarm(
            AlarmName=f"{name}-backlog-empty",
            MetricName="ApproximateBacklogSize",
            Namespace="AWS/SageMaker",
            Dimensions=[{"Name": "EndpointName", "Value": name}],
            Statistic="Average",
            Period=60, EvaluationPeriods=1, DatapointsToAlarm=1, Threshold=1,
            ComparisonOperator="LessThanThreshold",
            AlarmActions=[resp_in["PolicyARN"]], TreatMissingData="breaching",
        )
        print(f"STEP scale-in: 1-datapoint/60s empty-backlog alarm -> ExactCapacity {a.min_instances}")

    print(f"DONE. endpoint={name} region={a.region} range={a.min_instances}-{a.max_instances}")


if __name__ == "__main__":
    main()
