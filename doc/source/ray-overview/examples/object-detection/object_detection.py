import os
import io
from io import BytesIO
from typing import Dict

import torch
import requests
import numpy as np
from PIL import Image, ImageDraw, ImageFont
from fastapi import FastAPI
from fastapi.responses import Response
import torchvision
from torchvision import models

from ray import serve
from ray.serve.handle import DeploymentHandle
from smart_open import open as smart_open

# New dictionary mapping class names to labels.
CLASS_TO_LABEL: Dict[str, int] = {
    "background": 0,
    "with_mask": 1,
    "without_mask": 2,
    "mask_weared_incorrect": 3,
}

# Create the reverse mapping (label integer to class name) from CLASS_TO_LABEL.
LABEL_TO_CLASS: Dict[int, str] = {value: key for key, value in CLASS_TO_LABEL.items()}

LABEL_COLORS: Dict[str, str] = {
    "with_mask": "green",
    "without_mask": "red",
    "mask_weared_incorrect": "yellow",
}

# Model paths can be overridden using environment variables.
REMOTE_MODEL_PATH: str = os.getenv(
    "REMOTE_MODEL_PATH",
    "s3://face-masks-data/finetuned-models/fasterrcnn_model_mask_detection.pth",
)
CLUSTER_MODEL_PATH: str = os.getenv(
    "CLUSTER_MODEL_PATH", "/mnt/cluster_storage/fasterrcnn_model_mask_detection.pth"
)

app = FastAPI()


@serve.deployment(num_replicas=1)
@serve.ingress(app)
class APIIngress:
    def __init__(self, object_detection_handle: DeploymentHandle):
        self.handle = object_detection_handle

    @app.get(
        "/detect",
        responses={200: {"content": {"image/jpeg": {}}}},
        response_class=Response,
    )
    async def detect(self, image_url: str) -> Response:
        # Call the object detection service and return the processed image as JPEG.
        image = await self.handle.detect.remote(image_url)
        file_stream = BytesIO()
        image.save(file_stream, "jpeg")
        return Response(content=file_stream.getvalue(), media_type="image/jpeg")


@serve.deployment(
    ray_actor_options={"num_gpus": 1},
    autoscaling_config={"min_replicas": 1, "max_replicas": 10},
)
class ObjectDetection:
    def __init__(self):
        # Load the pre-trained Faster R-CNN model for mask detection.
        self.model = self._load_faster_rcnn_model()
        if torch.cuda.is_available():
            self.model = self.model.cuda()

    def _load_faster_rcnn_model(self):
        """Loads the Faster R-CNN model from a remote source if not already available locally."""
        # Download model only once from the remote storage to the cluster path.
        if not os.path.exists(CLUSTER_MODEL_PATH):
            with smart_open(REMOTE_MODEL_PATH, "rb") as s3_file:
                with open(CLUSTER_MODEL_PATH, "wb") as local_file:
                    local_file.write(s3_file.read())

        # Load the model with the correct number of classes and weights.
        loaded_model = models.detection.fasterrcnn_resnet50_fpn(
            num_classes=len(LABEL_TO_CLASS)
        )
        loaded_model.load_state_dict(torch.load(CLUSTER_MODEL_PATH, map_location="cpu"))
        loaded_model.eval()
        return loaded_model

    def _load_image_from_url(self, url: str) -> Image.Image:
        """
        Loads an image from the given URL and converts it to RGB format.

        :param url: URL of the image.
        :return: PIL Image in RGB format.
        """
        response = requests.get(url)
        response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert("RGB")

    def _predict_and_visualize(
        self, image: Image.Image, confidence_threshold: float = 0.5
    ) -> Image.Image:
        """
        Runs the detection model on the provided image and draws bounding boxes with labels.

        :param image: Input PIL Image.
        :param confidence_threshold: Score threshold to filter predictions.
        :return: PIL Image with visualized detections.
        """
        draw = ImageDraw.Draw(image)
        font = ImageFont.load_default()

        # Convert image to tensor and move to GPU if available.
        image_np = np.array(image)
        image_tensor = torch.from_numpy(image_np).permute(2, 0, 1).float() / 255.0
        image_tensor = image_tensor.to(
            torch.device("cuda:0" if torch.cuda.is_available() else "cpu")
        )

        with torch.no_grad():
            predictions = self.model([image_tensor])[0]

        # Filter predictions by confidence threshold.
        keep = predictions["scores"] > confidence_threshold
        boxes = predictions["boxes"][keep].cpu().numpy()
        labels = predictions["labels"][keep].cpu().numpy()
        scores = predictions["scores"][keep].cpu().numpy()

        for box, label, score in zip(boxes, labels, scores):
            x1, y1, x2, y2 = box
            class_name = LABEL_TO_CLASS.get(label, "unknown")
            box_color = LABEL_COLORS.get(class_name, "white")

            # Draw bounding box.
            draw.rectangle([x1, y1, x2, y2], outline=box_color, width=2)

            # Prepare and draw label text.
            text = f"{class_name} {score:.2f}"
            text_bbox = draw.textbbox((0, 0), text, font=font)
            text_height = text_bbox[3] - text_bbox[1]

            # Draw background for text.
            draw.rectangle(
                [x1, y1 - text_height - 2, x1 + (text_bbox[2] - text_bbox[0]), y1],
                fill=box_color,
            )
            # Draw text on top of the background.
            draw.text(
                (x1, y1 - text_height - 2),
                text,
                fill="black" if box_color == "yellow" else "white",
                font=font,
            )

        return image

    def detect(self, image_url: str) -> Image.Image:
        """
        Orchestrates the detection process: loads an image from a URL, runs prediction and visualization,
        and returns the annotated image.

        :param image_url: URL of the image to process.
        :return: Annotated PIL Image.
        """
        pil_image = self._load_image_from_url(image_url)
        result_image = self._predict_and_visualize(pil_image)
        return result_image


# Bind the deployments.
entrypoint = APIIngress.bind(ObjectDetection.bind())
