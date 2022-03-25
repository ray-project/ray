import json
import os

import ray
import ray.util.data as ml_data
import ray.util.iter as parallel_it
from ray.util.sgd.tf.tf_dataset import TFMLDataset
from ray.util.sgd.tf.tf_trainer import TFTrainer


def model_creator(config):
    import tensorflow as tf

    model = tf.keras.models.Sequential(
        [
            tf.keras.Input(shape=(1,)),
            tf.keras.layers.Dense(128, activation="relu"),
            tf.keras.layers.Dense(1),
        ]
    )
    optimizer = tf.keras.optimizers.Adam(lr=1e-4)
    model.compile(optimizer=optimizer, loss="mse", metrics=["accuracy"])
    return model


def make_data_creator(tf_ds: TFMLDataset):
    def data_creator(config):
        world_rank = None
        if "TF_CONFIG" in os.environ:
            tf_config = json.loads(os.environ["TF_CONFIG"])
            world_rank = tf_config["task"]["index"]
        else:
            world_rank = -1
        batch_size = config["batch_size"]
        ds = tf_ds.get_shard(shard_index=world_rank).batch(batch_size).repeat()
        return ds, None

    return data_creator


def main():
    num_points = 32 * 100 * 2
    data = [i * (1 / num_points) for i in range(num_points)]
    it = parallel_it.from_items(data, 2, False).for_each(lambda x: [x, x])
    # this will create MLDataset with column RangeIndex(range(2))
    ds = ml_data.from_parallel_iter(it, True, batch_size=32, repeated=False)
    tf_ds = ds.to_tf(feature_columns=[0], label_column=1)

    trainer = TFTrainer(
        model_creator=model_creator,
        data_creator=make_data_creator(tf_ds),
        num_replicas=2,
        config={
            "batch_size": 32,
            "fit_config": {
                "steps_per_epoch": 100,
            },
        },
    )

    for _ in range(10):
        trainer.train()

    model = trainer.get_model()
    print("f(0.5)=", float(model.predict([0.5])))


if __name__ == "__main__":
    ray.init()
    main()
