"""End-to-end latency (submit -> embeddings written to S3) for a load-test run.

Pairs each request's enqueue time (from the phased_load.py / sm_load.py output
CSV, columns video_id,enqueue_time) with the S3 done-marker's LastModified,
which is written right after the embeddings land (utils/idempotency.mark_done).
Both the Ray Serve and SageMaker runs write the same per-video done-marker, so
the same tool measures both. Reports the e2e latency distribution.

Run against a fresh done-marker prefix: markers left over from an earlier run
are skipped (their completion time predates the enqueue, so the latency is
non-positive and dropped).
"""

import argparse
import csv

import boto3


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--load-csv", required=True, help="phased_load.py / sm_load.py output CSV")
    ap.add_argument("--bucket", required=True)
    ap.add_argument("--done-prefix", default="video-index/done/",
                    help="S3 done-marker prefix (constants.S3_DONE_PREFIX)")
    ap.add_argument("--region", default="us-west-2")
    ap.add_argument("--out", default="e2e_latency.csv")
    a = ap.parse_args()

    # enqueue time per successfully-accepted request
    enqueued = {}
    with open(a.load_csv) as fh:
        for row in csv.DictReader(fh):
            if row.get("ok", "True") in ("True", "true", "1"):
                enqueued[row["video_id"]] = float(row["enqueue_time"])

    # completion time per video = done-marker LastModified
    s3 = boto3.client("s3", region_name=a.region)
    completed = {}
    for page in s3.get_paginator("list_objects_v2").paginate(Bucket=a.bucket, Prefix=a.done_prefix):
        for obj in page.get("Contents", []):
            vid = obj["Key"][len(a.done_prefix):].removesuffix(".done")
            completed[vid] = obj["LastModified"].timestamp()

    rows, lat = [], []
    for vid, t0 in enqueued.items():
        t1 = completed.get(vid)
        if t1 is None:
            continue
        e2e = t1 - t0
        if e2e <= 0:  # stale marker from an earlier run
            continue
        rows.append((vid, round(t0, 3), round(t1, 3), round(e2e, 2)))
        lat.append(e2e)

    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["video_id", "enqueue_time", "completed_time", "e2e_latency_s"])
        w.writerows(rows)

    lat.sort()
    n = len(lat)

    def pct(p):
        return round(lat[min(n - 1, int(p * n))], 2) if n else 0.0

    print(f"matched {n}/{len(enqueued)} requests -> {a.out}")
    if n:
        print(f"e2e latency s: p50={pct(0.5)} p90={pct(0.9)} p99={pct(0.99)} max={round(lat[-1], 2)}")


if __name__ == "__main__":
    main()
