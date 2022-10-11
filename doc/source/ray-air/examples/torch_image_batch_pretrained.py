import pandas as pd

from torchvision import transforms
from torchvision.models import resnet18

import ray
from ray.train.torch import TorchCheckpoint, TorchPredictor
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
    df.loc[:, "image"] = [preprocess(x).numpy() for x in df["image"]]
    return df


data_url = "s3://anonymous@air-example-data-2/1G-image-data-synthetic-raw"
print(f"Running GPU batch prediction with 1GB data from {data_url}")
dataset = ray.data.read_datasource(
    ImageFolderDatasource(), root=data_url, size=(256, 256)
)

model = resnet18(pretrained=True)

preprocessor = BatchMapper(preprocess)
ckpt = TorchCheckpoint.from_model(model=model, preprocessor=preprocessor)

predictor = BatchPredictor.from_checkpoint(ckpt, TorchPredictor)
predictor.predict(dataset, feature_columns=["image"], batch_size=80)
