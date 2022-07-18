import pandas as pd

from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.air.util.tensor_extensions.pandas import TensorArray
from ray.train.torch import to_air_checkpoint, TorchPredictor
from ray.train.batch_predictor import BatchPredictor
from ray.data.preprocessors import BatchMapper
from ray.data.datasource import ImageFolderDatasource


def preprocess(df: pd.DataFrame) -> pd.DataFrame:
    """
    User Pytorch code to transform user image. Note we still use pandas as
    intermediate format to hold images as shorthand of python dictionary.
    """
    preprocess = transforms.Compose(
        [
            transforms.ToTensor(),
            transforms.Resize(256),
            transforms.CenterCrop(224),
            transforms.Normalize(mean=[0.485, 0.456, 0.406], std=[0.229, 0.224, 0.225]),
        ]
    )
    df["image"] = TensorArray([preprocess(x.to_numpy()) for x in df["image"]])
    return df


data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
print(f"Running GPU batch prediction with 1GB data from {data_url}")
dataset = ray.data.read_datasource(ImageFolderDatasource(), paths=[data_url])

model = resnet18(pretrained=True)

preprocessor = BatchMapper(preprocess)
ckpt = to_air_checkpoint(model=model, preprocessor=preprocessor)

predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
predictor.predict(dataset, feature_columns=["image"])
