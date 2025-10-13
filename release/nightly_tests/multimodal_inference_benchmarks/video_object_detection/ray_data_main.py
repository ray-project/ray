from PIL import Image
import ray
from ultralytics import YOLO
import torch
import torchvision
import numpy as np
import io
import uuid

NUM_GPU_NODES = 8
YOLO_MODEL = "yolo11n.pt"
INPUT_PATH = "s3://anonymous@ray-example-data/videos/Hollywood2-actions-videos/Hollywood2/AVIClips/"
OUTPUT_PATH = f"s3://ray-data-write-benchmark/{uuid.uuid4().hex}"
IMAGE_HEIGHT = 640
IMAGE_WIDTH = 640
# This was a change made: Alter batch size accordingly
# batch_size = 32 for 1x large
# batch_size = 100 for 2x, 4x, and 8x large
BATCH_SIZE = 32

ray.init()


class ExtractImageFeatures:
    def __init__(self):
        self.model = YOLO(YOLO_MODEL)
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

    def __call__(self, batch):
        frames = batch["frame"]
        if len(frames) == 0:
            batch["features"] = []
            return batch
        tensor_batch = [
            torchvision.transforms.functional.to_tensor(Image.fromarray(frame))
            for frame in frames
        ]
        stack = torch.stack(tensor_batch, dim=0)
        results = self.model(stack)
        features = [self.to_features(res) for res in results]
        batch["features"] = features
        return batch


def resize_frame(row):
    frame = row["frame"]
    pil_image = Image.fromarray(frame)
    resized_pil = pil_image.resize((IMAGE_HEIGHT, IMAGE_WIDTH))
    resized_frame = np.array(resized_pil)
    row["frame"] = resized_frame
    return row


def explode_features(row):
    features_list = row["features"]
    for feature in features_list:
        row["features"] = feature
        yield row


def crop_image(row):
    frame = row["frame"]
    bbox = row["features"]["bbox"]
    x1, y1, x2, y2 = map(int, bbox)
    pil_image = Image.fromarray(frame)
    cropped_pil = pil_image.crop((x1, y1, x2, y2))

    buf = io.BytesIO()
    # This was a change made: Use compress_level=2
    cropped_pil.save(buf, format="PNG", compress_level=2)
    cropped_pil_png = buf.getvalue()

    row["object"] = cropped_pil_png
    return row


ds = ray.data.read_videos(INPUT_PATH)
ds = ds.map(resize_frame)
ds = ds.map_batches(
    ExtractImageFeatures, batch_size=BATCH_SIZE, num_gpus=1.0, concurrency=NUM_GPU_NODES
)
ds = ds.flat_map(explode_features)
ds = ds.map(crop_image)
ds = ds.drop_columns(["frame"])
ds.write_parquet(OUTPUT_PATH)
