#!/usr/bin/env python3

import os
import PIL

import streaming

import ray


def preprocess_mosaic(input_dir, output_dir):
    print("Writing to mosaic...")
    ds = ray.data.read_images(input_dir, mode="RGB")
    it = ds.iter_rows()

    columns = {"image": "pil", "label": "int"}
    # If reading from local disk, should turn off compression and use
    # streaming.LocalDataset.
    # If uploading to S3, turn on compression (e.g., compression="snappy") and
    # streaming.StreamingDataset.
    with streaming.MDSWriter(out=output_dir, columns=columns, compression=None) as out:
        for i, img in enumerate(it):
            img = PIL.Image.fromarray(img["image"])
            out.write(
                {
                    "image": img,
                    "label": 0,
                }
            )
            if i % 10 == 0:
                print(f"Wrote {i} images.")


def preprocess_parquet(input_dir, output_dir, target_partition_size=None):
    print("Writing to parquet...")

    def to_bytes(row):
        row["height"] = row["image"].shape[0]
        row["width"] = row["image"].shape[1]
        row["image"] = row["image"].tobytes()
        return row

    if target_partition_size is not None:
        ctx = ray.data.context.DataContext.get_current()
        ctx.target_max_block_size = target_partition_size

    ds = ray.data.read_images(input_dir, mode="RGB")
    ds = ds.map(to_bytes)
    ds.write_parquet(output_dir)


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Preprocess images.")  # noqa: E501
    parser.add_argument(
        "--data-root",
        default="/tmp/imagenet-1gb-data",
        type=str,
        help="Raw images directory.",
    )
    parser.add_argument(
        "--mosaic-data-root",
        default=None,
        type=str,
        help="Output directory path for mosaic.",
    )
    parser.add_argument(
        "--parquet-data-root",
        default=None,
        type=str,
        help="Output directory path for parquet.",
    )
    parser.add_argument(
        "--max-mb-per-file",
        default=64,
        type=int,
    )

    args = parser.parse_args()

    ray.init()

    if args.mosaic_data_root is not None:
        os.makedirs(args.mosaic_data_root)
        preprocess_mosaic(args.data_root, args.mosaic_data_root)
    if args.parquet_data_root is not None:
        os.makedirs(args.parquet_data_root)
        preprocess_parquet(
            args.data_root,
            args.parquet_data_root,
            target_partition_size=args.max_mb_per_file * 1024 * 1024,
        )
