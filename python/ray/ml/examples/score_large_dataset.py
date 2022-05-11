#  Adapted from Highly Performant TensorFlow Batch Inference on Image Data Using the SageMaker Python SDK
#  https://github.com/aws/amazon-sagemaker-examples/blob/main/sagemaker_batch_transform/tensorflow_open-images_jpg/
import boto3
import os
import tarfile
from tempfile import TemporaryDirectory
import tensorflow as tf

import ray
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.train.integrations.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray import train


class _LayerFromSavedModel(tf.keras.layers.Layer):
    def __init__(self, loaded):
        super(_LayerFromSavedModel, self).__init__()
        self.vars = loaded.variables
        self.loaded = loaded

    def __call__(self, inputs):
        return self.loaded.signatures['serving_default'](inputs)


def _convert_to_keras_model(loaded):
    inputs = tf.keras.Input(shape=(None,), dtype=tf.string)
    model = tf.keras.Model(inputs, _LayerFromSavedModel(loaded)(inputs))
    return model


def train_loop_per_worker(config):
    # Download a trained model and immediately save it.
    # For this to work, you need aws credentials set up.
    s3_client = boto3.client("s3")
    with TemporaryDirectory() as tmp_dir:
        zipped_model_file = os.path.join(tmp_dir, "resnet_v2_fp32_savedmodel_NCHW_jpg.tar.gz")
        s3_client.download_file(
            "sagemaker-sample-data-us-west-2", "batch-transform/open-images/model/resnet_v2_fp32_savedmodel_NCHW_jpg.tar.gz", zipped_model_file)
        with tarfile.open(zipped_model_file, "r") as tar:
            tar.extractall(tmp_dir)
        model_file = os.path.join(tmp_dir, "resnet_v2_fp32_savedmodel_NCHW_jpg/1538687370")
        model = _convert_to_keras_model(tf.saved_model.load(model_file))
        train.save_checkpoint(epoch=0, model=model.get_weights())


trainer = TensorflowTrainer(train_loop_per_worker=train_loop_per_worker,
                            scaling_config={"num_workers": 1},)

result = trainer.fit()

batch_predictor = BatchPredictor.from_checkpoint(result.checkpoint, TensorflowPredictor, model_definition=tf.keras.applications.ResNet50V2)

# try out with one image first...
test_ds = ray.data.read_binary_files("s3://sagemaker-sample-data-us-west-2/batch-transform/open-images/jpg/00000b4dcff7f799.jpg")

batch_prediction_result = batch_predictor.predict(data=test_ds)
