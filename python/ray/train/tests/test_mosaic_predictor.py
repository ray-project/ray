import pytest
import os
import numpy as np

# torch libraries
import torch
import torch.utils.data

# torchvision libraries
import torchvision
from torchvision import transforms, datasets

# Import Ray libraries
import ray
from ray.air.config import ScalingConfig
import ray.train as train
from ray.air import session
from ray.train.batch_predictor import BatchPredictor

# import ray-mosaic integration libraries
from ray.train.mosaic import MosaicTrainer
from ray.train.mosaic import MosaicPredictor

# import composer training libraries
from composer.models.tasks import ComposerClassifier
import composer.optim


@pytest.fixture(autouse=True, scope="session")
def ray_start_8_cpus():
    address_info = ray.init(num_cpus=8)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


mean = (0.507, 0.487, 0.441)
std = (0.267, 0.256, 0.276)
cifar10_transforms = transforms.Compose(
    [transforms.ToTensor(), transforms.Normalize(mean, std)]
)
# data_directory = "./data" ## TODO : remove the following line
data_directory = "~/Desktop/workspace/data"
train_dataset = datasets.CIFAR10(
    data_directory, train=True, download=True, transform=cifar10_transforms
)
test_dataset = datasets.CIFAR10(
    data_directory, train=False, download=True, transform=cifar10_transforms
)

test_images = [x.numpy() for x, _ in test_dataset]

scaling_config = ScalingConfig(num_workers=2, use_gpu=False)


def trainer_init_per_worker(**config):
    BATCH_SIZE = 1024
    # prepare the model for distributed training and wrap with ComposerClassifier for
    # Composer Trainer compatibility
    model = config.pop("model", torchvision.models.resnet18(num_classes=10))
    model = ComposerClassifier(ray.train.torch.prepare_model(model))

    # prepare train/test dataset
    train_dataset = config.pop("train_dataset")

    batch_size_per_worker = BATCH_SIZE // session.get_world_size()
    train_dataloader = torch.utils.data.DataLoader(
        train_dataset, batch_size=batch_size_per_worker, shuffle=True
    )

    train_dataloader = train.torch.prepare_data_loader(train_dataloader)

    # prepare optimizer
    optimizer = composer.optim.DecoupledSGDW(
        model.parameters(),
        lr=0.05,
        momentum=0.9,
        weight_decay=2.0e-3,
    )

    return composer.trainer.Trainer(
        model=model, train_dataloader=train_dataloader, optimizers=optimizer, **config
    )


trainer_init_per_worker.__test__ = False


def convert_logits_to_classes(df):
    best_class = df["predictions"].map(lambda x: x.argmax())
    df["prediction"] = best_class
    return df


def calculate_prediction_scores(df):
    df["correct"] = df["prediction"] == df["label"]
    return df[["prediction", "label", "correct"]]


def get_prediction(predictor, input):
    return predictor.predict(input)


def get_prediction_scores(predictor, input):
    # make predictions
    outputs = get_prediction(predictor, input)
    predictions = outputs.map_batches(convert_logits_to_classes, batch_format="pandas")

    # add label column
    prediction_df = predictions.to_pandas()
    prediction_df["label"] = [y for _, y in test_dataset]
    predictions = ray.data.from_pandas(prediction_df)

    # score prediction
    scores = predictions.map_batches(calculate_prediction_scores)

    return scores.sum(on="correct") / scores.count()


trainer_init_per_worker.__test__ = False
convert_logits_to_classes.__test__ = False
calculate_prediction_scores.__test__ = False
get_prediction.__test__ = False
get_prediction_scores.__test__ = False


class TestResnet:
    """
    Test making predictions with MosaicPredictor for Resnet
    """

    @pytest.fixture(scope="class")
    def model(self):
        return torchvision.models.resnet18(num_classes=10)

    @pytest.fixture(scope="class")
    def train_model(self, model):
        trainer_init_config = {
            "model": model,
            "max_duration": "1ba",
            "train_dataset": train_dataset,
        }

        trainer = MosaicTrainer(
            trainer_init_per_worker=trainer_init_per_worker,
            trainer_init_config=trainer_init_config,
            scaling_config=scaling_config,
        )

        result = trainer.fit()
        return result

    def test_predict(self, model, train_model):
        """
        Basic prediction using MosaicPredictor, for which the model is loaded from save
        path.
        """
        checkpoint_dict = train_model.checkpoint.to_dict()
        save_path = os.path.join(
            checkpoint_dict["working_directory"], checkpoint_dict["all_checkpoints"][-1]
        )

        predictor = MosaicPredictor.from_save_path(save_path, model=model)

        test_batch = np.array(test_images)
        _ = get_prediction(predictor, test_batch)

    def test_batch_predict(self, model, train_model):
        """
        Use BatchPredictor to make predictions
        """
        predictor = BatchPredictor.from_checkpoint(
            checkpoint=train_model.checkpoint,
            predictor_cls=MosaicPredictor,
            model=model,
        )

        test_input = ray.data.from_items(test_images)

        _ = get_prediction(predictor, test_input)

    def test_batch_predict_and_score(self, model, train_model):
        """
        Use BatchPredictor to make predictions and get scores
        """
        predictor = BatchPredictor.from_checkpoint(
            checkpoint=train_model.checkpoint,
            predictor_cls=MosaicPredictor,
            model=model,
        )

        test_input = ray.data.from_items(test_images)

        _ = get_prediction_scores(predictor, test_input)


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main(["-v", "-x", __file__]))
