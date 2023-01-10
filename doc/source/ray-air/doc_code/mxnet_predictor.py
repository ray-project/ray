# fmt: off
# __mxnetpredictor_imports_start__
import os
from typing import Dict, Optional, Union

import mxnet as mx
import numpy as np
from mxnet import gluon

import ray
from ray.air import Checkpoint
from ray.data.preprocessor import Preprocessor
from ray.train.batch_predictor import BatchPredictor
from ray.train.predictor import Predictor
# __mxnetpredictor_imports_end__


# __mxnetpredictor_signature_start__
class MXNetPredictor(Predictor):
    ...
    # __mxnetpredictor_signature_end__

    # __mxnetpredictor_init_start__
    def __init__(
        self,
        net: gluon.Block,
        preprocessor: Optional[Preprocessor] = None,
    ):
        self.net = net
        super().__init__(preprocessor)
    # __mxnetpredictor_init_end__

    # __mxnetpredictor_from_checkpoint_start__
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
    # __mxnetpredictor_from_checkpoint_end__

    # __mxnetpredictor_predict_numpy_start__
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
# __mxnetpredictor_predict_numpy_end__


# __mxnetpredictor_model_start__
net = gluon.model_zoo.vision.resnet50_v1(pretrained=True)
# __mxnetpredictor_model_end__

# __mxnetpredictor_checkpoint_start__
os.makedirs("checkpoint", exist_ok=True)
net.save_parameters("checkpoint/net.params")
checkpoint = Checkpoint.from_directory("checkpoint")
# __mxnetpredictor_checkpoint_end__

# __mxnetpredictor_predict_start__
predictor = BatchPredictor.from_checkpoint(checkpoint, MXNetPredictor, net=net)
# These images aren't normalized. In practice, preprocess images before inference.
dataset = ray.data.read_images("s3://anonymous@air-example-data-2/imagenet-sample-images")
predictor.predict(dataset)
# __mxnetpredictor_predict_end__
# fmt: on
