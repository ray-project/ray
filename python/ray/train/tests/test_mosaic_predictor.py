import pytest
from typing import Tuple
import pandas as pd
import numpy as np

# torch libraries
import torch
import torch.utils.data

# torchvision libraries
import torchvision
from torchvision import transforms

# Import Ray libraries
import ray
from ray.data.datasource import SimpleTorchDatasource
from ray.air.config import ScalingConfig

# import ray-mosaic integration libraries
from ray.train.mosaic import MosaicTrainer
from ray.train.mosaic import MosaicPredictor

# import composer training libraries
from torchmetrics.classification.accuracy import Accuracy
from composer.core.evaluator import Evaluator
from composer.models.tasks import ComposerClassifier
import composer.optim
from ray.train.batch_predictor import BatchPredictor
from ray.air.util.data_batch_conversion import convert_batch_type_to_pandas


BATCH_SIZE = 1024

@pytest.fixture(autouse=True, scope="session")
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.fixture(autouse=True, scope="module")
def prepare_dataset():
    transform = transforms.Compose(
        [transforms.ToTensor(), transforms.Normalize((0.5, 0.5, 0.5), (0.5, 0.5, 0.5))]
    )

    def train_dataset_factory():
        return torchvision.datasets.CIFAR10(
            root="./data", download=True, train=True, transform=transform
        )

    def test_dataset_factory():
        return torchvision.datasets.CIFAR10(
            root="./data", download=True, train=False, transform=transform
        )

    train_dataset_raw: ray.data.Dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=train_dataset_factory
    )
    test_dataset_raw: ray.data.Dataset = ray.data.read_datasource(
        SimpleTorchDatasource(), dataset_factory=test_dataset_factory
    )

    # Map into pandas
    def convert_batch_to_pandas(batch: Tuple[torch.Tensor, int]) -> pd.DataFrame:
        images = [image.numpy() for image, _ in batch]
        labels = [label for _, label in batch]
        return pd.DataFrame({"image": images, "label": labels}).head(10)

    global train_dataset
    train_dataset = train_dataset_raw.map_batches(convert_batch_to_pandas)
    global test_dataset
    test_dataset = test_dataset_raw.map_batches(convert_batch_to_pandas)

scaling_config = ScalingConfig(num_workers=6, use_gpu=False)

def trainer_init_per_worker(train_dataset, eval_dataset=None, **config):
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))

    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    optimizer = composer.optim.DecoupledSGDW(
        model.parameters(),
        lr=0.05,
        momentum=0.9,
        weight_decay=2.0e-3,
    )

    lr_scheduler = composer.optim.LinearWithWarmupScheduler(
        t_warmup="1ep",
        alpha_i=1.0,
        alpha_f=1.0,
    )

    evaluator = Evaluator(
        dataloader=eval_dataset, label="my_evaluator", metrics=Accuracy()
    )

    return composer.trainer.Trainer(
        model=model,
        train_dataloader=train_dataset,
        eval_dataloader=evaluator,
        optimizers=optimizer,
        schedulers=lr_scheduler,
        **config
    )

def convert_logits_to_classes(df):
    best_class = df["predictions"].map(lambda x: x.argmax())
    df["prediction"] = best_class
    return df

def calculate_prediction_scores(df):
    df["correct"] = df["prediction"] == df["label"]
    return df[["prediction", "label", "correct"]]

def get_prediction_scores(predictor):
    # make predictions
    outputs = predictor.predict(
        data=test_dataset, dtype=torch.float, feature_columns=["image"], keep_columns=["label"]
    )

    predictions = outputs.map_batches(
        convert_logits_to_classes, batch_format="pandas"
    )

    # get scores from the prediction
    scores = predictions.map_batches(calculate_prediction_scores)
    
    return scores.sum(on="correct") / scores.count()

trainer_init_per_worker.__test__ = False
convert_logits_to_classes.__test__ = False
calculate_prediction_scores.__test__ = False
get_prediction_scores.__test__ = False

tested_models = [
    torchvision.models.resnet18(num_classes=10),
]


@pytest.mark.parametrize('model', tested_models, scope="session")
class TestResnet():
    @pytest.fixture(scope="class")
    def train_model(self, model):
        trainer_init_config = {
            "model" : model,
            "batch_size": BATCH_SIZE,
            "max_duration": "1ep",
            "labels": ["image", "label"],
        }

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            datasets={"train": train_dataset, "evaluation": test_dataset},
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()
        return result

    def test_predict(self,model, train_model):
        checkpoint_dict = train_model.checkpoint.to_dict()

        predictor = MosaicPredictor.from_save_path(
            checkpoint_dict["last_checkpoint"][-1],
            model=model
        )

        test_batch = np.array(next(test_dataset.iter_torch_batches())["image"])
        _ = predictor.predict(data=test_batch, dtype=torch.float)

    def test_batch_predict(self,model, train_model):
        predictor = BatchPredictor.from_checkpoint(
            checkpoint=train_model.checkpoint,
            predictor_cls=MosaicPredictor,
            model=model
        )

        _ = get_prediction_scores(predictor)


if __name__ == "__main__":
    import sys
    sys.exit(pytest.main(["-v", "-x", __file__]))