import math
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pyarrow as pa
import s3fs

import ray
from ray.data._internal.util import make_async_gen

NUM_IMAGES = 10000
IMAGE_URI_COL_NAME = "image_uri"

uris = [
    f"s3://air-example-data-2/20G-image-data-synthetic-raw/dog_{str(i)}.jpg"
    for i in range(1, NUM_IMAGES + 1)
]
ids = [str(i) for i in range(1, NUM_IMAGES + 1)]

# Create Arrow table directly to avoid pandas conversion
arrow_table = pa.Table.from_arrays(
    [pa.array(uris), pa.array(ids)], names=[IMAGE_URI_COL_NAME, "id"]
)
ds = ray.data.from_arrow(arrow_table)


def download_image(row):
    fs = s3fs.S3FileSystem(anon=True)
    with fs.open(row[IMAGE_URI_COL_NAME], "rb") as f:
        row["image_bytes"] = f.read()
    return row


# naive_map_ds = ds.map(download_image)

# start_time = time.time()
# for _ in naive_map_ds.iter_internal_ref_bundles():
#     pass
# end_time = time.time()
# print(f"Time elapsed naive ds map: {end_time - start_time} seconds")


def download_images(batch):
    fs = s3fs.S3FileSystem(anon=True)
    images = []
    for uri in batch[IMAGE_URI_COL_NAME]:
        with fs.open(uri, "rb") as f:
            images.append(f.read())
    batch["image_bytes"] = images
    return batch


naive_map_batches_ds = ds.map_batches(download_images)

start_time = time.time()
for _ in naive_map_batches_ds.iter_internal_ref_bundles():
    pass
end_time = time.time()
print(f"Time elapsed naive ds map_batches: {end_time - start_time} seconds")


def download_images_threaded(batch, max_workers=16):
    """Optimized version that uses make_async_gen for concurrent downloads."""

    def download_uris(uri_iterator):
        """Function that takes an iterator of URIs and yields downloaded bytes for each."""
        for uri in uri_iterator:
            fs = s3fs.S3FileSystem(anon=True)
            try:
                with fs.open(uri, "rb") as f:
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

    def _sample_sizes(self, uri_list, max_workers=8):
        """Fetch file sizes in parallel using ThreadPoolExecutor."""

        def get_file_size(uri):
            # Each thread needs its own filesystem instance
            fs = s3fs.S3FileSystem(anon=True)
            try:
                return fs.info(uri)["size"]
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


optimized_ds = ds.map_batches(
    PartitionActor, concurrency=1, batch_format="pyarrow"
).map_batches(download_images_threaded, batch_format="pyarrow")

start_time = time.time()
for _ in optimized_ds.iter_internal_ref_bundles():
    pass
end_time = time.time()
print(
    f"Time elapsed optimized ds map_batches / from uris: {end_time - start_time} seconds"
)
