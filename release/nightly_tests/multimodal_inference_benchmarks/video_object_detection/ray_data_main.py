from PIL import Image
import pyarrow as pa
import pyarrow.fs
import ray
from ray.data.datatype import DataType
from ray.data.expressions import col, udf
from ray.data.extensions import ArrowTensorArray
from ultralytics import YOLO
import torch
import torchvision
from torchvision.transforms.v2 import InterpolationMode
from torchvision.transforms.v2.functional import resize
import numpy as np
import io
import os
import shutil
import tempfile
import uuid
import warnings
from benchmark import Benchmark

# A copy of https://github.com/ultralytics/assets/releases/download/v8.3.0/yolo11n.pt
# (sha256 0ebbc80d4a7680d14987a577cd21342b65ecfd94632bd9a8da63ae6417644ee1).
YOLO_MODEL_URI = (
    "s3://ray-benchmark-data-internal-us-west-2/models/ultralytics/yolo11n.pt"
)
INPUT_PATH = "s3://anonymous@ray-example-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640
# This was a change made: Alter batch size accordingly
# batch_size = 100 for 1x, 2x, 4x, and 8x large
BATCH_SIZE = 100

FEATURES_TYPE = pa.list_(
    pa.struct(
        [
            ("label", pa.int64()),
            ("confidence", pa.float64()),
            ("bbox", pa.list_(pa.float64())),
        ]
    )
)

ray.init()


def to_frames(column: pa.ChunkedArray) -> list:
    """Convert a column of Ray tensors into a list of HxWxC frames."""
    return [frame for chunk in column.chunks for frame in chunk.to_numpy()]


@udf(
    return_dtype=DataType.tensor(
        shape=(IMAGE_HEIGHT, IMAGE_WIDTH, 3), dtype=DataType.uint8()
    )
)
def resize_frames(frames: pa.ChunkedArray) -> pa.Array:
    resized = [np.empty((0, IMAGE_HEIGHT, IMAGE_WIDTH, 3), dtype=np.uint8)]
    for chunk in frames.chunks:
        batch = chunk.to_numpy()
        if batch.dtype == object:
            # The frames in this chunk have different sizes, so resize them one at a
            # time.
            resized.extend(resize_batch(frame[np.newaxis]) for frame in batch)
        else:
            resized.append(resize_batch(batch))
    return ArrowTensorArray.from_numpy(np.concatenate(resized))


def resize_batch(batch: np.ndarray) -> np.ndarray:
    """Resize an (N, H, W, C) array of frames to (N, IMAGE_HEIGHT, IMAGE_WIDTH, C).

    Bicubic with antialiasing matches PIL's default ``Image.resize`` to within 1-2
    pixel values, and it's several times faster because it resizes the whole batch
    at once.
    """
    with warnings.catch_warnings():
        # `batch` is a read-only view of the block. That's fine because resizing
        # only reads it.
        warnings.filterwarnings(
            "ignore", message="The given NumPy array is not writable"
        )
        tensor = torch.from_numpy(batch).permute(0, 3, 1, 2)
    tensor = resize(
        tensor,
        [IMAGE_HEIGHT, IMAGE_WIDTH],
        interpolation=InterpolationMode.BICUBIC,
        antialias=True,
    )
    return tensor.permute(0, 2, 3, 1).numpy()


def download_model(uri: str) -> str:
    """Copy model weights from cloud storage to a local file and return its path.

    Given a model name like "yolo11n.pt", ultralytics downloads the weights from
    GitHub. If GitHub fails, ultralytics doesn't raise and loading fails with
    `FileNotFoundError`. Reading the weights from S3 keeps GitHub out of the test.
    """
    filesystem, path = pyarrow.fs.FileSystem.from_uri(uri)
    local_path = os.path.join(tempfile.mkdtemp(), os.path.basename(path))
    with filesystem.open_input_stream(path) as src, open(local_path, "wb") as dst:
        shutil.copyfileobj(src, dst)
    return local_path


@udf(return_dtype=DataType.from_arrow(FEATURES_TYPE))
class ExtractImageFeatures:
    def __init__(self):
        self.model = YOLO(download_model(YOLO_MODEL_URI))
        if torch.cuda.is_available():
            self.model.to("cuda")

    def to_features(self, res):
        return [
            {
                "label": label,
                "confidence": confidence.item(),
                "bbox": bbox.tolist(),  # TODO: Use numpy
            }
            for label, confidence, bbox in zip(
                res.names, res.boxes.conf, res.boxes.xyxy
            )
        ]

    def __call__(self, frames: pa.ChunkedArray) -> pa.Array:
        frames = to_frames(frames)
        features = []
        # `with_column` passes the UDF a whole block, so split it into batches of
        # BATCH_SIZE to keep the model's input (and memory use) the same size as
        # `map_batches(batch_size=BATCH_SIZE)` would.
        for start in range(0, len(frames), BATCH_SIZE):
            tensor_batch = [
                torchvision.transforms.functional.to_tensor(Image.fromarray(frame))
                for frame in frames[start : start + BATCH_SIZE]
            ]
            results = self.model(torch.stack(tensor_batch, dim=0))
            features.extend(self.to_features(res) for res in results)
        return pa.array(features, type=FEATURES_TYPE)


def explode_features(row):
    features_list = row["features"]
    for feature in features_list:
        row["features"] = feature
        yield row


@udf(return_dtype=DataType.binary())
def crop_objects(frames: pa.ChunkedArray, bboxes: pa.ChunkedArray) -> pa.Array:
    objects = []
    for frame, bbox in zip(to_frames(frames), bboxes.to_pylist()):
        x1, y1, x2, y2 = map(int, bbox)
        buf = io.BytesIO()
        Image.fromarray(frame).crop((x1, y1, x2, y2)).save(buf, format="PNG")
        objects.append(buf.getvalue())
    return pa.array(objects, type=pa.binary())


def run_pipeline():
    # These are best practices we recommend to avoid OOMs, though they're opt-in and
    # not enabled by default.
    ray.data.DataContext.get_current().isolate_read_workers = True
    ray.data.DataContext.get_current().default_map_logical_memory_enabled = True

    extract_image_features = ExtractImageFeatures()

    ds = ray.data.read_videos(INPUT_PATH, memory=3_628_835_840)  # ~3.4 GiB
    ds = ds.with_column("frame", resize_frames(col("frame")))
    ds = ds.with_column(
        "features",
        extract_image_features(col("frame")),
        num_gpus=1.0,
        # Ray Data can't prevent OOMs if you don't set `memory` for high-memory UDFs
        # like this one. We chose this value because it's what Ray Data's memory issue
        # detector recommended (observed max USS plus headroom) in previous nightly
        # test runs.
        memory=5_218_298_880,  # ~4.9 GiB
    )
    # There's no expression for exploding a list column into rows, so this stays a
    # `flat_map`.
    ds = ds.flat_map(explode_features)
    ds = ds.with_column(
        "object", crop_objects(col("frame"), col("features").struct["bbox"])
    )
    # `select_columns` is a projection, so it fuses with the `with_column` above.
    # (`drop_columns` falls back to `map_batches` here because the schema after
    # `flat_map` isn't known until execution.)
    ds = ds.select_columns(["frame_index", "features", "object"])
    ds.write_parquet(OUTPUT_PATH)


benchmark = Benchmark()
benchmark.run_fn("main", run_pipeline)
benchmark.write_result()
