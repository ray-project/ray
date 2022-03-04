import ray

# TODO: make this copy-paste-able
# read a local CSV file
csv_path = "path/to/file.csv"
ds = ray.data.read_csv(csv_path)

# read parquet from S3
parquet_path = "s3://ursa-labs-taxi-data/2019/06/data.parquet"
ds = ray.data.read_parquet(parquet_path)
