"""
#Train a simple deep CNN on the CIFAR10 small images dataset.

It gets to 75% validation accuracy in 25 epochs, and 79% after 50 epochs.
(it"s still underfitting at that point, though).
"""
import argparse
import time

from tensorflow.keras.datasets import cifar10
from tensorflow.keras.preprocessing.image import ImageDataGenerator
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import Dense, Dropout, Activation, Flatten
from tensorflow.keras.layers import Conv2D, MaxPooling2D
import os
from filelock import FileLock

import ray
from ray.util.sgd.tf.tf_trainer import TFTrainer

num_classes = 10


def fetch_keras_data():
    import tensorflow as tf

    # The data, split between train and test sets:
    with FileLock(os.path.expanduser("~/.cifar.lock")):
        (x_train, y_train), (x_test, y_test) = cifar10.load_data()

    # Convert class vectors to binary class matrices.
    y_train = tf.keras.utils.to_categorical(y_train, num_classes)
    y_test = tf.keras.utils.to_categorical(y_test, num_classes)

    x_train = x_train.astype("float32")
    x_test = x_test.astype("float32")
    x_train /= 255
    x_test /= 255
    return (x_train, y_train), (x_test, y_test)


(x_train, y_train), (x_test, y_test) = fetch_keras_data()
input_shape = x_train.shape[1:]


def create_model(config):
    import tensorflow as tf

    model = Sequential()
    model.add(Conv2D(32, (3, 3), padding="same", input_shape=input_shape))
    model.add(Activation("relu"))
    model.add(Conv2D(32, (3, 3)))
    model.add(Activation("relu"))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    model.add(Dropout(0.25))

    model.add(Conv2D(64, (3, 3), padding="same"))
    model.add(Activation("relu"))
    model.add(Conv2D(64, (3, 3)))
    model.add(Activation("relu"))
    model.add(MaxPooling2D(pool_size=(2, 2)))
    model.add(Dropout(0.25))

    model.add(Flatten())
    model.add(Dense(64))
    model.add(Activation("relu"))
    model.add(Dropout(0.5))
    model.add(Dense(num_classes))
    model.add(Activation("softmax"))

    # initiate RMSprop optimizer
    opt = tf.keras.optimizers.RMSprop(lr=0.001, decay=1e-6)

    # Let"s train the model using RMSprop
    model.compile(loss="categorical_crossentropy", optimizer=opt, metrics=["accuracy"])
    return model


def data_creator(config):
    import tensorflow as tf

    batch_size = config["batch_size"]
    (x_train, y_train), (x_test, y_test) = fetch_keras_data()
    train_dataset = tf.data.Dataset.from_tensor_slices((x_train, y_train))
    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))

    # Repeat is needed to avoid
    train_dataset = train_dataset.repeat().shuffle(len(x_train)).batch(batch_size)
    test_dataset = test_dataset.repeat().batch(batch_size)
    return train_dataset, test_dataset


def _make_generator(x_train, y_train, batch_size):
    # This will do preprocessing and realtime data augmentation:
    datagen = ImageDataGenerator(
        featurewise_center=False,  # set input mean to 0 over the dataset
        samplewise_center=False,  # set each sample mean to 0
        # divide inputs by std of the dataset
        featurewise_std_normalization=False,
        samplewise_std_normalization=False,  # divide each input by its std
        zca_whitening=False,  # apply ZCA whitening
        zca_epsilon=1e-06,  # epsilon for ZCA whitening
        # randomly rotate images in the range (degrees, 0 to 180)
        rotation_range=0,
        # randomly shift images horizontally (fraction of total width)
        width_shift_range=0.1,
        # randomly shift images vertically (fraction of total height)
        height_shift_range=0.1,
        shear_range=0.0,  # set range for random shear
        zoom_range=0.0,  # set range for random zoom
        channel_shift_range=0.0,  # set range for random channel shifts
        # set mode for filling points outside the input boundaries
        fill_mode="nearest",
        cval=0.0,  # value used for fill_mode = "constant"
        horizontal_flip=True,  # randomly flip images
        vertical_flip=False,  # randomly flip images
        # set rescaling factor (applied before any other transformation)
        rescale=None,
        # set function that will be applied on each input
        preprocessing_function=None,
        # image data format, either "channels_first" or "channels_last"
        data_format=None,
        # fraction of images reserved for validation (strictly between 0 and 1)
        validation_split=0.0,
    )

    # Compute quantities required for feature-wise normalization
    # (std, mean, and principal components if ZCA whitening is applied).
    datagen.fit(x_train)
    return datagen.flow(x_train, y_train, batch_size=batch_size)


def data_augmentation_creator(config):
    import tensorflow as tf

    batch_size = config["batch_size"]
    (x_train, y_train), (x_test, y_test) = fetch_keras_data()
    trainset = tf.data.Dataset.from_generator(
        lambda: _make_generator(x_train, y_train, batch_size),
        output_types=(tf.float32, tf.float32),
        # https://github.com/tensorflow/tensorflow/issues/24520
        output_shapes=(
            tf.TensorShape((None, None, None, None)),
            tf.TensorShape((None, 10)),
        ),
    )
    trainset = trainset.repeat()

    test_dataset = tf.data.Dataset.from_tensor_slices((x_test, y_test))
    test_dataset = test_dataset.repeat().batch(batch_size)
    return trainset, test_dataset


def main(smoke_test, num_replicas, use_gpu=False, augment_data=False, batch_size=32):
    data_size = 60000
    test_size = 10000
    batch_size = batch_size

    num_train_steps = 10 if smoke_test else data_size // batch_size
    num_eval_steps = 10 if smoke_test else test_size // batch_size

    trainer = TFTrainer(
        model_creator=create_model,
        data_creator=(data_augmentation_creator if augment_data else data_creator),
        num_replicas=num_replicas,
        use_gpu=use_gpu,
        verbose=True,
        config={
            "batch_size": batch_size,
            "fit_config": {
                "steps_per_epoch": num_train_steps,
            },
            "evaluate_config": {
                "steps": num_eval_steps,
            },
        },
    )

    training_start = time.time()
    num_epochs = 1 if smoke_test else 3
    for i in range(num_epochs):
        # Trains num epochs
        train_stats1 = trainer.train()
        train_stats1.update(trainer.validate())
        print(f"iter {i}:", train_stats1)

    dt = (time.time() - training_start) / 3
    print(f"Training on workers takes: {dt:.3f} seconds/epoch")

    model = trainer.get_model()
    trainer.shutdown()
    dataset, test_dataset = data_augmentation_creator(dict(batch_size=batch_size))

    training_start = time.time()
    model.fit(dataset, steps_per_epoch=num_train_steps, epochs=1)
    dt = time.time() - training_start
    print(f"Training on workers takes: {dt:.3f} seconds/epoch")

    scores = model.evaluate(test_dataset, steps=num_eval_steps)
    print("Test loss:", scores[0])
    print("Test accuracy:", scores[1])


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-replicas",
        "-n",
        type=int,
        default=1,
        help="Sets number of replicas for training.",
    )
    parser.add_argument("--batch-size", type=int, default=32, help="Sets batch size.")
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )
    parser.add_argument(
        "--augment-data",
        action="store_true",
        default=False,
        help="Sets data augmentation.",
    )
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        default=False,
        help="Finish quickly for testing. Assume False for users.",
    )

    args, _ = parser.parse_known_args()
    if args.smoke_test:
        ray.init(num_cpus=2)
    else:
        ray.init(address=args.address)

    main(
        smoke_test=args.smoke_test,
        batch_size=args.batch_size,
        augment_data=args.augment_data,
        use_gpu=args.use_gpu,
        num_replicas=args.num_replicas,
    )
