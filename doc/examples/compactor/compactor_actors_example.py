import argparse
import boto3
import hashlib
import math
import re
import ray
import pandas as pd

from os import path

parser = argparse.ArgumentParser()
parser.add_argument(
    "bucket",
    help="S3 bucket containing input files of the form: "
    "input/{tableStreamId}-{eventTime}.[parq|parquet]. "
    "Compacted parquet file output will also be written "
    "to the output folder of this bucket.",
    type=str)
parser.add_argument(
    "table_stream_id",
    help="Globally unique table stream identifier that this "
    "compaction job is serving.",
    type=str)
parser.add_argument(
    "--primary-keys",
    help="Primary key columns of the table stream being "
    "compacted.",
    nargs="+",
    required=True)
parser.add_argument(
    "--sort-keys",
    help="Sort key columns of the table stream being "
    "compacted. Input event timestamp is always "
    "implicitly the last sort key.",
    nargs="*")
parser.add_argument(
    "--records-per-output-file",
    help="Max records per compacted output file.",
    default=500_000,
    type=int)
parser.add_argument(
    "--hash-bucket-count",
    help="Number of intermediate hash buckets to create.",
    default=100,
    type=int)


############
# IO Utils #
############
def read_parquet_files(file_paths):
    dataframes = {}
    for file_path in file_paths:
        dataframes[file_path] = pd.read_parquet(file_path)
    return dataframes


def write_parquet_files(dataframe, output_file_prefix, max_records_per_file):

    dataframes = split_dataframe(dataframe, max_records_per_file)
    for i in range(len(dataframes)):
        dataframes[i].to_parquet(
            "{}_{}.parq".format(output_file_prefix, i),
            flavor="spark",
        )


###################
# Dataframe Utils #
###################
def concat_dataframes(dataframes):
    if len(dataframes) == 1:
        return next(iter(dataframes))
    return pd.concat(dataframes, axis=0, copy=False)


def split_dataframe(dataframe, max_len):
    if max_len is None:
        return [dataframe]
    dataframes = []
    num_splits = math.ceil(len(dataframe) / max_len)
    for i in range(num_splits):
        dataframes.append(dataframe[i * max_len:(i + 1) * max_len])
    return dataframes


############
# S3 Utils #
############
def _get_s3_client():
    return boto3.client("s3")


def filter_file_paths_by_prefix(bucket, prefix):
    keys = filter_keys_by_prefix(bucket, prefix)
    for key in keys:
        yield "s3://{}/{}".format(bucket, key)


def filter_keys_by_prefix(bucket, prefix):
    s3 = _get_s3_client()
    params = {"Bucket": bucket, "Prefix": prefix}
    more_objects_to_list = True
    while more_objects_to_list:
        response = s3.list_objects_v2(**params)
        if "Contents" in response:
            for object in response["Contents"]:
                key = object["Key"]
                yield key
        params["ContinuationToken"] = response.get("NextContinuationToken")
        more_objects_to_list = params["ContinuationToken"] is not None


def read_parquet_files_by_prefix(bucket, prefix):
    input_file_paths = filter_file_paths_by_prefix(bucket, prefix)
    input_file_to_df = read_parquet_files(input_file_paths)
    return input_file_to_df


def get_dedupe_output_file_prefix(output_bucket, table_stream_id,
                                  hash_bucket_index):

    return "s3://{}/output/{}_{}_dedupe".format(
        output_bucket,
        table_stream_id,
        hash_bucket_index,
    )


#####################
# Hash Bucket Utils #
#####################
def group_by_pk_hash_bucket(dataframe, num_buckets, columns, hash_column_name):
    hash_bucket_column_name = hash_column_name + "_bucket"
    dataframe[hash_column_name] = \
        pd.DataFrame(dataframe[columns].astype("str").values.sum(axis=1))[0] \
        .astype("bytes") \
        .apply(lambda pk_bytes: hash_pk_bytes(pk_bytes))
    dataframe[hash_bucket_column_name] = dataframe[hash_column_name] \
        .apply(lambda digest: int.from_bytes(digest, "big") % num_buckets)
    return dataframe.groupby(hash_bucket_column_name)


def drop_hash_bucket_column(dataframe):
    return dataframe.iloc[:, :-1]


def hash_pk_bytes(pk_bytes):
    hasher = hashlib.sha1()
    hasher.update(pk_bytes)
    return hasher.digest()


#########################
# Event Timestamp Utils #
#########################
def read_files_add_event_timestamp(file_paths, event_timestamp_column_name):

    input_file_to_df = read_parquet_files(file_paths)
    for input_file, dataframe in input_file_to_df.items():
        event_timestamp = get_input_file_event_timestamp(input_file)
        dataframe[event_timestamp_column_name] = int(event_timestamp)
    return concat_dataframes(input_file_to_df.values())


def get_input_file_event_timestamp(input_file):
    input_file_name = path.basename(input_file)
    m = re.match(".+?-(?P<_0>\d+)\.[parq|parquet]", input_file_name)
    return next(iter(m.groupdict().values()))


#################
# Preconditions #
#################
def check_preconditions(primary_keys, sort_keys, max_records_per_output_file):

    assert len(primary_keys) == len(set(primary_keys)), \
        "Primary key names must be unique: {}".format(primary_keys)
    assert len(sort_keys) == len(set(sort_keys)), \
        "Sort key names must be unique: {}".format(sort_keys)
    assert max_records_per_output_file >= 1, \
        "Max records per output file must be a positive value"


#############
# Compactor #
#############
def compact(bucket, table_stream_id, primary_keys, sort_keys,
            max_records_per_output_file, num_hash_buckets):

    # check preconditions before doing any computationally expensive work
    check_preconditions(
        primary_keys,
        sort_keys,
        max_records_per_output_file,
    )

    # define distinct, but constant, pk hash and event timestamp column names
    col_uuid = "4000f124-dfbd-48c6-885b-7b22621a6d41"
    pk_hash_column_name = "{}_hash".format(col_uuid)
    event_timestamp_column_name = "{}_event_timestamp".format(col_uuid)

    # append the event timestamp column to the sort key list
    sort_keys.append(event_timestamp_column_name)

    # discover input file paths
    input_file_paths = filter_file_paths_by_prefix(
        bucket, "input/{}".format(table_stream_id))
    all_hash_bucket_indices = set()

    # create an actor to track dataset chunks for each hash bucket
    hb_actors = []
    for i in range(num_hash_buckets):
        hb_actors.append(HashBucket.remote())

    # group like primary keys together by hashing them into buckets
    hb_tasks_pending = []
    for input_file_path in input_file_paths:
        hb_task_promise = hash_bucket.remote(
            [input_file_path],
            primary_keys,
            num_hash_buckets,
            pk_hash_column_name,
            event_timestamp_column_name,
            hb_actors,
        )
        hb_tasks_pending.append(hb_task_promise)
    while len(hb_tasks_pending):
        hb_task_complete, hb_tasks_pending = ray.wait(hb_tasks_pending)
        all_hash_bucket_indices.update(ray.get(hb_task_complete[0]))

    # dedupe each bucket by primary key hash and sort key
    dd_tasks_pending = []
    for hb_index in all_hash_bucket_indices:
        dd_task_promise = dedupe.remote(
            bucket,
            hb_actors[hb_index].get.remote(),
            table_stream_id,
            hb_index,
            pk_hash_column_name,
            sort_keys,
            max_records_per_output_file,
        )
        dd_tasks_pending.append(dd_task_promise)
    while len(dd_tasks_pending):
        dd_task_complete, dd_tasks_pending = ray.wait(dd_tasks_pending)


@ray.remote
class HashBucket:
    def __init__(self, ):
        self.dataframes = list()

    def append(self, dataframe):
        self.dataframes.append(dataframe)

    def get(self):
        return self.dataframes


@ray.remote
def hash_bucket(input_file_paths, primary_keys, num_buckets, hash_column_name,
                event_timestamp_column_name, hb_actors):

    # read input parquet path into a single dataframe
    dataframe = read_files_add_event_timestamp(
        input_file_paths,
        event_timestamp_column_name,
    )

    # group the data by primary key hash value
    df_groups = group_by_pk_hash_bucket(
        dataframe,
        num_buckets,
        primary_keys,
        hash_column_name,
    )

    # write grouped output data to files including the group name
    hash_bucket_indices = []
    for hash_bucket_index, df_group in df_groups:
        hash_bucket_indices.append(hash_bucket_index)
        output = drop_hash_bucket_column(df_group)
        hb_actors[hash_bucket_index].append.remote(output)

    return hash_bucket_indices


@ray.remote
def dedupe(bucket, hb_dataframes, table_stream_id, hash_bucket_index,
           primary_keys, sort_keys, max_records_per_output_file):

    # read previously compacted input parquet files
    dedupe_output_file_prefix = get_dedupe_output_file_prefix(
        bucket,
        table_stream_id,
        hash_bucket_index,
    )
    prev_output_file_to_df = read_parquet_files_by_prefix(
        bucket,
        dedupe_output_file_prefix,
    )

    # concatenate uncompacted and previously compacted dataframes
    hb_dataframes.extend(prev_output_file_to_df.values())
    dataframe = pd.concat(hb_dataframes, axis=0, copy=False)

    # sort by sort keys
    dataframe.sort_values(sort_keys, inplace=True)

    # drop duplicates by primary key
    dataframe.drop_duplicates(primary_keys, inplace=True)

    # write sorted, compacted table back
    write_parquet_files(
        dataframe,
        dedupe_output_file_prefix,
        max_records_per_output_file,
    )


if __name__ == "__main__":
    args = parser.parse_args()
    ray.init(address="auto")
    compact(
        args.bucket,
        args.table_stream_id,
        args.primary_keys,
        args.sort_keys,
        args.records_per_output_file,
        args.hash_bucket_count,
    )
