#!/bin/bash


# Exit if any of the test commands fail.
set -x -e pipeline

DIR="/mnt/cluster_storage/imagenet-1gb"
MOSAIC_DIR="/mnt/cluster_storage/mosaicml-data"
TFRECORDS_DIR="/mnt/cluster_storage/tf-data"
PARQUET_DIR="/mnt/cluster_storage/parquet-data"

rm -rf "$DIR"
rm -rf "$MOSAIC_DIR"
rm -rf "$TFRECORDS_DIR"
rm -rf "$PARQUET_DIR"

# Download 1GB dataset from S3 to local disk so we can preprocess with mosaic.
aws s3 sync s3://imagenetmini1000/1gb $DIR
# Generated with
# https://github.com/tensorflow/tpu/blob/master/tools/datasets/imagenet_to_gcs.py.
aws s3 sync s3://imagenetmini1000/1gb-tfrecords $TFRECORDS_DIR
# Preprocess parquet and mosaic files.
python preprocess_images.py --data-root "$DIR" --mosaic-data-root "$MOSAIC_DIR" --parquet-data-root "$PARQUET_DIR"

python image_loader_microbenchmark.py --data-root "$DIR" --mosaic-data-root "$MOSAIC_DIR" --parquet-data-root "$PARQUET_DIR" --tf-data-root "$TFRECORDS_DIR"/train
