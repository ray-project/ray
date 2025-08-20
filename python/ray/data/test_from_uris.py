import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.fs as pafs

import ray
from ray.data._internal.util import make_async_gen

#### SETUP CODE ####
BUCKET = "anyscale-imagenet/"
METADATA_PATH = "s3://" + BUCKET + "metadata.parquet"
IMAGES_PATH = "s3://" + BUCKET + "ILSVRC/Data/CLS-LOC/train/"
NUM_IMAGES = 100
IMAGE_URI_COL_NAME = "key"


def convert_key(table: pa.Table) -> pa.Table:
    col = table["key"]
    t = col.type
    new_col = pc.binary_join_element_wise(
        pa.scalar(BUCKET, type=t), col, pa.scalar("", type=t)
    )
    return table.set_column(
        table.schema.get_field_index(IMAGE_URI_COL_NAME), IMAGE_URI_COL_NAME, new_col
    )


metadata_ds = (
    ray.data.read_parquet(METADATA_PATH)
    .map_batches(convert_key, batch_format="pyarrow")
    .limit(NUM_IMAGES)
)

read_images_ds = ray.data.read_images(IMAGES_PATH, mode="RGB").limit(NUM_IMAGES)
#### END OF SETUP CODE ####

#### READ FROM URIS PROTOTYPE CODE ####
def download_images_threaded(batch, max_workers=16):
    """Optimized version that uses make_async_gen for concurrent downloads."""

    def download_uris(uri_iterator):
        """Function that takes an iterator of URIs and yields downloaded bytes for each."""
        fs = pafs.S3FileSystem(region="us-west-2")
        for uri in uri_iterator:
            try:
                with fs.open_input_file(uri) as f:
                    yield f.read()
            except Exception as e:
                print(f"Error downloading {uri}: {e}")
                yield None

    # Extract URIs from PyArrow table
    uris = batch.column(IMAGE_URI_COL_NAME).to_pylist()

    # Use make_async_gen to download URIs concurrently
    # This preserves the order of results to match the input URIs
    images = list(
        make_async_gen(
            base_iterator=iter(uris),
            fn=download_uris,
            preserve_ordering=True,
            num_workers=max_workers,
        )
    )

    # Add the new column to the PyArrow table
    return batch.add_column(len(batch.column_names), "image_bytes", pa.array(images))


class PartitionActor:
    INIT_SAMPLE_BATCH_SIZE = 25

    def __init__(self):
        self._batch_size_estimate = None

    def _sample_sizes(self, uri_list, max_workers=16):
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri):
            # Each thread needs its own filesystem instance
            fs = pafs.S3FileSystem(region="us-west-2")
            try:
                return fs.get_file_info(uri).size
            except Exception as e:
                print(f"Error getting size for {uri}: {e}")
                return None

        file_sizes = []

        # Use ThreadPoolExecutor for concurrent size fetching
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all size fetch tasks
            futures = [executor.submit(get_file_size, uri) for uri in uri_list]

            # Collect results as they complete (order doesn't matter)
            for future in as_completed(futures):
                try:
                    size = future.result()
                    if size is not None:
                        file_sizes.append(size)
                except Exception as e:
                    print(f"Error in thread: {e}")

        return file_sizes

    def _arrow_batcher(self, table, n):
        """Batch a PyArrow table into smaller tables of size n using zero-copy slicing."""
        num_rows = table.num_rows
        for i in range(0, num_rows, n):
            end_idx = min(i + n, num_rows)
            # Use PyArrow's zero-copy slice operation
            batch_table = table.slice(i, end_idx - i)
            print(f"Yielding batch with len(batch): {batch_table.num_rows}")
            yield batch_table

    def __call__(self, batch):
        if self._batch_size_estimate is None:
            # Extract URIs from PyArrow table for sampling
            uris = batch.column(IMAGE_URI_COL_NAME).to_pylist()
            sample_uris = uris[: self.INIT_SAMPLE_BATCH_SIZE]
            file_sizes = self._sample_sizes(sample_uris)
            file_size_estimate = sum(file_sizes) / len(file_sizes)
            ctx = ray.data.context.DatasetContext.get_current()
            max_bytes = ctx.target_max_block_size
            self._batch_size_estimate = math.floor(max_bytes / file_size_estimate)
            print(f"Batch size estimate: {self._batch_size_estimate}")

        print(f"len(batch): {batch.num_rows}")
        yield from self._arrow_batcher(batch, self._batch_size_estimate)


#### END OF READ FROM URIS PROTOTYPE CODE ####

#### READ_IMAGES BENCHMARK ####
start_time = time.time()
for _ in read_images_ds.iter_internal_ref_bundles():
    pass
end_time = time.time()
print(f"Time elapsed optimized read_images: {end_time - start_time} seconds")
#### END OF READ_IMAGES BENCHMARK ####

#### READ FROM URI BENCHMARK ####
# pre-materialize the metadata to avoid including in the benchmark time
metadata_ds = metadata_ds.materialize()

read_from_uri_ds = metadata_ds.map_batches(
    PartitionActor, concurrency=1, batch_format="pyarrow", preserve_output_format=True
).map_batches(
    download_images_threaded, batch_format="pyarrow", preserve_input_format=True
)

start_time = time.time()
for _ in read_from_uri_ds.iter_internal_ref_bundles():
    pass
end_time = time.time()
print(
    f"Time elapsed optimized ds map_batches / from uris: {end_time - start_time} seconds"
)
#### END OF READ FROM URI BENCHMARK ####
