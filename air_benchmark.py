import argparse

import ray


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--benchmark", type=str, default="ingest", help="ingest or infer")
    parser.add_argument("--dataset-size-gb", type=int, default=200)
    parser.add_argument("--streaming", action="store_true", default=False)
    args = parser.parse_args()
    ds = make_ds(args.dataset_size_gb)
    if args.benchmark == "ingest":
       if args.streaming:
          run_ingest_streaming(ds)
       else:
          run_ingest_bulk(ds)
    elif args.benchmark == "infer":
       if args.streaming:
          run_infer_streaming(ds)
       else:
          run_infer_bulk(ds)
    else:
       assert False
