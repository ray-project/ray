import ray
from keras import layers
from keras.applications import EfficientNetB0
from ray.train.batch_predictor import BatchPredictor
from ray.train.tensorflow import TensorflowCheckpoint, TensorflowPredictor
from torchvision import models
from torch import nn

# checkpoint = result.checkpoint
checkpoint = TensorflowCheckpoint.from_directory("/tmp/tensorflow-image.checkpoint")

# Read some
inference_data = ray.data.read_images(
    f"s3://anonymous@air-example-data/food-101-tiny/valid", size=(256, 256), mode="RGB"
)


# We have to specify our model definition again


def build_model():
    inputs = layers.Input(shape=(256, 256, 3))
    # x = img_augmentation(inputs)
    x = inputs
    model = EfficientNetB0(include_top=False, input_tensor=x, weights="imagenet")

    # Freeze the pretrained weights
    model.trainable = True

    # Rebuild top
    x = layers.GlobalAveragePooling2D(name="avg_pool")(model.output)
    x = layers.BatchNormalization()(x)

    top_dropout_rate = 0.2
    x = layers.Dropout(top_dropout_rate, name="top_dropout")(x)
    outputs = layers.Dense(NUM_CLASSES, activation="linear", name="pred")(x)

    # Compile
    model = tf.keras.Model(inputs, outputs, name="EfficientNet")
    return model


# Create a batch predictor from the checkpointed model state dict,
# providing the model class as an input.
predictor = BatchPredictor.from_checkpoint(
    checkpoint, TensorflowPredictor, model_definition=build_model
)

# Actually run predictions
predictions = predictor.predict(inference_data)

# We can now process the results, e.g. inspect the dataframe
df = predictions.to_pandas()
print(df)
