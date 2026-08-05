"""Poll SageMaker async-endpoint metrics over time -> CSV.

Analogous to bench/autoscale_poll.py, but the signals come from CloudWatch +
DescribeEndpoint instead of our /queue + /replicas endpoints:
  - backlog / backlog_per_instance : AWS/SageMaker ApproximateBacklogSize[PerInstance]
  - instances                      : DescribeEndpoint CurrentInstanceCount (the autoscaled count)
  - completed                      : count of result objects landed in the async output S3 prefix

Note: CloudWatch async metrics publish at ~1-minute resolution, so this series
is coarser than the Ray Serve poller (a genuine observability difference).
"""

import argparse
import csv
import time
from datetime import datetime, timedelta, timezone

import boto3


def latest_metric(cw, endpoint, metric):
    end = datetime.now(timezone.utc)
    start = end - timedelta(minutes=10)
    r = cw.get_metric_data(
        MetricDataQueries=[{
            "Id": "m",
            "MetricStat": {
                "Metric": {
                    "Namespace": "AWS/SageMaker",
                    "MetricName": metric,
                    "Dimensions": [{"Name": "EndpointName", "Value": endpoint}],
                },
                "Period": 60,
                "Stat": "Average",
            },
        }],
        StartTime=start, EndTime=end, ScanBy="TimestampDescending",
    )
    vals = r["MetricDataResults"][0]["Values"]
    return vals[0] if vals else 0.0


def count_outputs(s3, bucket, prefix):
    n = 0
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
        n += page.get("KeyCount", 0)
    return n


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint-name", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--output-bucket", required=True)
    ap.add_argument("--output-prefix", required=True, help="async output S3 prefix (key part, no s3://)")
    ap.add_argument("--interval", type=float, default=15)
    ap.add_argument("--duration", type=float, default=1800)
    ap.add_argument("--out", default="sm_metrics.csv")
    a = ap.parse_args()

    sm = boto3.client("sagemaker", region_name=a.region)
    cw = boto3.client("cloudwatch", region_name=a.region)
    s3 = boto3.client("s3", region_name=a.region)

    t0 = time.time()
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["t", "backlog", "backlog_per_instance", "instances", "completed"])
        while time.time() - t0 < a.duration:
            t = round(time.time() - t0, 1)
            try:
                pv = sm.describe_endpoint(EndpointName=a.endpoint_name)["ProductionVariants"][0]
                instances = pv.get("CurrentInstanceCount", 0)
                backlog = latest_metric(cw, a.endpoint_name, "ApproximateBacklogSize")
                per = latest_metric(cw, a.endpoint_name, "ApproximateBacklogSizePerInstance")
                completed = count_outputs(s3, a.output_bucket, a.output_prefix)
                w.writerow([t, int(backlog), round(per, 2), instances, completed])
                fh.flush()
                print(f"[{t:.0f}s] backlog={int(backlog)} per_inst={per:.1f} instances={instances} completed={completed}")
            except Exception as e:
                print(f"[{t:.0f}s] err {type(e).__name__}: {e}")
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
