"""Load generator for a SageMaker Async Inference endpoint.

Mirrors bench/phased_load.py: same piecewise rate profile ("dur:r0:r1,..."),
same output CSV schema (video_id, enqueue_time, ack_latency_ms, ok, error). Because
invoke_endpoint_async needs an S3 InputLocation per request, it first stages N
small input JSONs in S3 (one per request, distinct video_id = real work), then
invokes them at the offered rate. Each invoke returns immediately (the async
ack), which is the equivalent of our enqueue ack.
"""

import argparse
import csv
import json
import time
from concurrent.futures import ThreadPoolExecutor

import boto3


def parse_profile(s):
    segs = []
    for part in s.split(","):
        d, r0, r1 = part.split(":")
        segs.append((float(d), float(r0), float(r1)))
    return segs


def rate_at(t, segs):
    acc = 0.0
    for d, r0, r1 in segs:
        if t < acc + d:
            frac = (t - acc) / d if d > 0 else 0.0
            return r0 + (r1 - r0) * frac
        acc += d
    return 0.0


def total_requests(segs):
    # Integrate the trapezoids -> approximate total request count.
    return int(sum(d * (r0 + r1) / 2.0 for d, r0, r1 in segs))


def stage_inputs(s3, bucket, prefix, n, video_uri, id_prefix, workers=32):
    prefix = prefix.strip("/")

    def put(i):
        body = json.dumps({"video_uri": video_uri, "video_id": f"{id_prefix}-{i}"}).encode()
        s3.put_object(Bucket=bucket, Key=f"{prefix}/{i}.json", Body=body)

    with ThreadPoolExecutor(max_workers=workers) as ex:
        list(ex.map(put, range(n)))
    return prefix


def run(args):
    segs = parse_profile(args.profile)
    total = sum(d for d, _, _ in segs)
    n = args.n or total_requests(segs)

    s3 = boto3.client("s3", region_name=args.region)
    smrt = boto3.client("sagemaker-runtime", region_name=args.region)

    print(f"staging {n} input objects to s3://{args.input_bucket}/{args.input_prefix}/ ...")
    stage_inputs(s3, args.input_bucket, args.input_prefix, n, args.video_uri, args.id_prefix)

    results = []

    def invoke(i):
        loc = f"s3://{args.input_bucket}/{args.input_prefix.strip('/')}/{i}.json"
        t0 = time.time()
        try:
            smrt.invoke_endpoint_async(
                EndpointName=args.endpoint_name,
                InputLocation=loc,
                ContentType="application/json",
                InvocationTimeoutSeconds=3600,
            )
            ok, err = True, ""
        except Exception as e:
            ok, err = False, type(e).__name__
        results.append((f"{args.id_prefix}-{i}", t0, round((time.time() - t0) * 1000, 1), ok, err))

    ex = ThreadPoolExecutor(max_workers=args.max_workers)
    counter, t0, tick, carry = 0, time.time(), 0.1, 0.0
    while time.time() - t0 < total and counter < n:
        rate = rate_at(time.time() - t0, segs)
        want = rate * tick + carry
        launch = int(want)
        carry = want - launch
        for _ in range(launch):
            if counter >= n:
                break
            ex.submit(invoke, counter)
            counter += 1
        time.sleep(tick)
    ex.shutdown(wait=True)

    with open(args.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["video_id", "enqueue_time", "ack_latency_ms", "ok", "error"])
        w.writerows(results)
    ok = sum(1 for r in results if r[3])
    print(f"invoked {counter} ({ok} ok, {counter - ok} failed) over {total:.0f}s -> {args.out}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--endpoint-name", required=True)
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--input-bucket", required=True)
    ap.add_argument("--input-prefix", default="sm-bench/inputs")
    ap.add_argument("--video-uri", required=True)
    ap.add_argument("--profile", required=True, help="dur:r0:r1,dur:r0:r1,...")
    ap.add_argument("--n", type=int, default=0, help="inputs to stage (default: derived from profile)")
    ap.add_argument("--id-prefix", default="smbench")
    ap.add_argument("--max-workers", type=int, default=64)
    ap.add_argument("--out", default="sm_load.csv")
    run(ap.parse_args())


if __name__ == "__main__":
    main()
