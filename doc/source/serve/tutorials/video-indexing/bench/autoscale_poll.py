"""Sample /queue + /replicas over time -> CSV, for the queue-depth autoscaling demo.

No S3/creds needed: reads queue depth and per-deployment running replica counts
straight from the service endpoints.
"""

import argparse
import csv
import json
import time
import urllib.request


def get(url):
    with urllib.request.urlopen(url, timeout=8) as r:
        return json.load(r)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--interval", type=float, default=3)
    ap.add_argument("--duration", type=float, default=900)
    a = ap.parse_args()

    t0 = time.time()
    with open(a.out, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(["t", "queue_depth", "dlq_depth", "ingress", "consumer", "encoder"])
        while time.time() - t0 < a.duration:
            t = round(time.time() - t0, 1)
            try:
                q = get(a.url + "/queue")
                rep = get(a.url + "/replicas")
                qd = q.get("queue_depth", -1)
                dlq = q.get("dlq_depth", -1)
                ing = rep.get("IndexingIngress", {}).get("RUNNING", 0)
                cons = (rep.get("VideoIndexConsumer") or rep.get("MatchedIndexer") or {}).get("RUNNING", 0)
                enc = rep.get("VideoEncoder", {}).get("RUNNING", 0)
                w.writerow([t, qd, dlq, ing, cons, enc])
                fh.flush()
                print(f"[{t:.0f}s] q={qd} dlq={dlq} ing={ing} cons={cons} enc={enc}")
            except Exception as e:
                print(f"[{t:.0f}s] err {type(e).__name__}: {e}")
            time.sleep(a.interval)


if __name__ == "__main__":
    main()
