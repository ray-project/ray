# fmt: off
# __mxnetpredictor_impl_start__
from typing import Dict, Optional, Union

import mxnet as mx
import numpy as np
from mxnet import gluon

import ray
from ray.air import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.predictor import Predictor


class MXNetPredictor(Predictor):
    def __init__(
        self,
        net: gluon.Block,
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.net = net
        super().__init__(preprocessor)

    def _predict_numpy(
        self,
        data: Union[np.ndarray, Dict[str, np.ndarray]],
        dtype: Optional[np.dtype] = None,
    ) -> Dict[str, np.ndarray]:
        # If `data` looks like `{"features": array([...])}`, unwrap the `dict` and pass
        # the array directly to the model.
        if isinstance(data, dict) and len(data) == 1:
            data = next(iter(data.values()))

        inputs = mx.nd.array(data, dtype=dtype)
        outputs = self.net(inputs).asnumpy()

        return {"predictions": outputs}

    @classmethod
    def from_checkpoint(
        cls,
        checkpoint: Checkpoint,
        net: gluon.Block,
        preprocessor: Optional[Preprocessor] = None,
    ) -> Predictor:
        with checkpoint.as_directory() as directory:
            path = os.path.join(directory, "net.params")
            net.load_parameters(path)
        return cls(net, preprocessor=preprocessor)
# __mxnetpredictor_impl_end__
# fmt: on


def preprocess_batch(batch: Dict[str, np.ndarray]) -> Dict[str, np.ndarray]:
    def preprocess(image: mx.nd.NDArray) -> np.ndarray:
        image = mx.nd.array(image)
        # (C, H, W) -> (H, W, C)
        image.transpose((1, 2, 0))
        image = mx.image.imresize(image, 224, 224)
        image = mx.image.color_normalize(
            image.astype(dtype="float32") / 255,
            mean=mx.nd.array([0.485, 0.456, 0.406]),
            std=mx.nd.array([0.229, 0.224, 0.225]),
        )
        # (H, W, C) -> (C, H, W)
        image = image.transpose((2, 0, 1))
        return image.asnumpy()

    batch["image"] = np.array([preprocess(image) for image in batch["image"]])
    return batch


# __mxnetpredictor_usage_start__
import os

from ray.train.batch_predictor import BatchPredictor

net = gluon.model_zoo.vision.resnet50_v1(pretrained=True)

os.makedirs("checkpoint", exist_ok=True)
net.save_parameters("checkpoint/net.params")
checkpoint = Checkpoint.from_directory("checkpoint")

predictor = BatchPredictor(checkpoint, MXNetPredictor, net=net)
# __mxnetpredictor_usage_end__

# NOTE: This is to ensure the code runs. It shouldn't be part of the documentation.
dataset = ray.data.read_images("s3://air-example-data-2/imagenet-sample-images")
predictor.predict(dataset)
