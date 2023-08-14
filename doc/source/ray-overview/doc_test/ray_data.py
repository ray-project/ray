import ray

# TODO: make this copy-paste-able
# read a local CSV file
csv_path = "path/to/file.csv"
ds = ray.data.read_csv(csv_path)

# read parquet from S3
parquet_path = (
    "s3://anonymous@air-example-data/ursa-labs-taxi-data/by_year/2019/01/data.parquet"
)
ds = ray.data.read_parquet(parquet_path)
