#  Adapted from Highly Performant TensorFlow Batch Inference on Image Data
#  Using the SageMaker Python SDK
#  https://github.com/aws/amazon-sagemaker-examples/blob/main/sagemaker_batch_transform/tensorflow_open-images_jpg/  # noqa
import pandas as pd
import tensorflow as tf

import ray
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.preprocessors import BatchMapper
from ray.ml.train.integrations.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray import train


# Should the logic here be consolidated into some preprocessor or DataSource to reuse?
def per_element_transform(x: bytes) -> tf.Tensor:
    image_tensor = tf.io.decode_image(x)
    resized_image_tensor = tf.image.resize(image_tensor, [224, 224])
    processed_new_image_tensor = tf.keras.applications.resnet_v2.preprocess_input(
        resized_image_tensor
    )
    return processed_new_image_tensor


def per_dataframe_transform(df: pd.DataFrame) -> pd.DataFrame:
    df["value"] = df["value"].apply(per_element_transform)
    return df


preprocessor = BatchMapper(per_dataframe_transform)


def train_loop_per_worker(config):
    model = tf.keras.applications.ResNet50V2()
    train.save_checkpoint(epoch=0, model=model.get_weights())


trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    preprocessor=preprocessor,
    scaling_config={"num_workers": 1},
)

result = trainer.fit()

batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint,
    TensorflowPredictor,
    model_definition=tf.keras.applications.ResNet50V2,
)

# test_ds = ray.data.read_binary_files(
# "s3://sagemaker-sample-data-us-west-2/batch-transform/open-images/jpg/"
# )
test_ds = ray.data.read_binary_files("jpg_dir", parallelism=2)

batch_prediction_result = batch_predictor.predict(
    data=test_ds,
    min_scoring_workers=2,
    max_scoring_workers=2,
    feature_columns=["value"],
)
