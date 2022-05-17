# This notebook is based on a GPU batch inference example on a real size dataset from
# [Sagemaker](https://aws.amazon.com/blogs/machine-learning/
# performing-batch-inference-with-tensorflow-serving-in-amazon-sagemaker/).
# The main purpose of this notebook is to show that [Ray Air]
# (https://docs.ray.io/en/latest/ray-air/getting-started.html)
# can make batch inference workflow a lot easier and streamlined.

from matplotlib import pyplot as plt
from matplotlib import image as img
import numpy as np
import os
import pandas as pd
import tensorflow as tf

import ray
from ray.data.extensions import TensorArray
from ray.ml.batch_predictor import BatchPredictor
from ray.ml.predictors.integrations.tensorflow import TensorflowPredictor
from ray.ml.preprocessors import BatchMapper
from ray.ml.train.integrations.tensorflow.tensorflow_trainer import TensorflowTrainer
from ray import train


# TODO: Provide some kind of ImageDataSource that converts to Tensor already.
# TODO: Image resizing and preprocessing steps are model specific and should be part of
#  "last-mile-preprocessing" (once we figure that out).
# TODO: Instruction text pending as the landscape is still changing.
def per_element_transform(x: bytes) -> np.array:
    image_tensor = tf.io.decode_image(x)
    resized_image_tensor = tf.image.resize(image_tensor, [224, 224])
    processed_new_image_tensor = tf.keras.applications.resnet_v2.preprocess_input(
        resized_image_tensor
    )
    res = processed_new_image_tensor.numpy()
    return res


def per_dataframe_transform(df: pd.DataFrame) -> pd.DataFrame:
    numpy_array = df["value"].apply(per_element_transform).to_numpy()
    return pd.DataFrame({"value": TensorArray(numpy_array)})


# Here we instantiate a BatchMapper based on a udf function that defines
# dataframe transform.
preprocessor = BatchMapper(per_dataframe_transform)


# Here we instantiate a dummy Trainer to have access to `Checkpoint`
# object to be used together with `ray.ml.predictor.Predictor`. This is
# needed if one wants to use AIR batch inference in a piecemeal fashion
# (without using `ray.ml.trainer` for training). This will be replaced
# by a real Trainer if one uses AIR for both training and batch inference.
def train_loop_per_worker(config):
    model = tf.keras.applications.ResNet50V2()
    train.save_checkpoint(epoch=0, model=model.get_weights())


trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    preprocessor=preprocessor,
    scaling_config={"num_workers": 1},
)

# `result.checkpoint` is a `ray.ml.checkpoint.Checkpoint` object which includes
# both the preprocessing steps as well as model itself, both of which are used
# by the predictor. This ensures that preprocessing is consistent between model
# training and prediction.
result = trainer.fit()

# Here we instantiate a BatchPredictor, which is the main class that one interacts
# for batch inference workload. Notice here it takes in a TensorflowPredictor, which
# is written in a fashion that works coherently with TensorflowTrainer.
# In general, if one implements their own Trainer, one should also implement a
# corresponding Predictor.
batch_predictor = BatchPredictor.from_checkpoint(
    result.checkpoint,
    TensorflowPredictor,
    model_definition=tf.keras.applications.ResNet50V2,
)

# Let's first work on a dummy dataset that only contains 4 images.
NUM_IMAGES = 4
# Reads in image files into datasets.
test_ds_dummy = ray.data.read_binary_files("jpg_dir_dummy", parallelism=2)

batch_prediction_result_dummy = batch_predictor.predict(
    data=test_ds_dummy,
    min_scoring_workers=2,
    max_scoring_workers=2,
)

# Let's now inspect the result.
output_class_result = tf.keras.applications.resnet50.decode_predictions(
    tf.convert_to_tensor(
        batch_prediction_result_dummy.to_pandas()["__RAY_TC__"]
    ).numpy(),
    top=1,
)
print("The categories of the images are respectively: ")
for i in range(NUM_IMAGES):
    print(output_class_result[i][0][1])

# Now let's inspect the original images.
for i, file in enumerate(os.listdir("jpg_dir_dummy")[:NUM_IMAGES]):
    img_path = os.path.join("jpg_dir_dummy", file)
    image = img.imread(img_path)
    plt.subplot(1, 4, i + 1)
    plt.imshow(image)

plt.show()
# You can see the scoring result matches what is shown in the images!


# Now let's play with the real dataset.
# Download S3 images via the following command:
# `aws s3 cp s3://sagemaker-sample-data-us-west-2/batch-transform/open-images/jpg/
# jpg_dir`.
# Notice aws cli needs to be set up (including credentials) for this to work.
# `aws s3 ls s3://sagemaker-sample-data-us-west-2/batch-transform/open-images/jpg/
# | wc -l`
# There are all together 99999 images to be scored.
# Our benchmark shows a throughput of 45 images per second.
# Feel free to adjust `min_scoring_workers` and `max_scoring_workers` according to
# cluster resources.
# We also flip `num_gpus_per_worker` to be 1 - make sure that you run the following on a
# GPU cluster!
# `batch_size` should be adjusted accordingly to avoid OOM error from GPU!
# Uncomment the following if you want to run on a GPU cluster!
# test_ds = ray.data.read_binary_files("jpg_dir_dummy", parallelism=2)
# batch_prediction_result = batch_predictor.predict(
#     data=test_ds,
#     min_scoring_workers=2,
#     max_scoring_workers=2,
#     batch_size=1024,
#     num_gpus_per_worker=1,
# )
