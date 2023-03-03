import tensorflow as tf
import transformers
from datasets import load_dataset
from transformers import TFAutoModelForSequenceClassification, AutoTokenizer
import ray
from ray import air
from ray.air import session
from ray.train.tensorflow import TensorflowTrainer

# -------------------------------
# Data Ingestion with Ray Dataset
# -------------------------------


def get_train_val_ray_datasets():
    # Split into training and validation sets
    train_dataset = load_dataset("imdb", split="train")
    val_dataset = load_dataset("imdb", split="test")

    train_ray_dataset = ray.data.from_items(list(train_dataset))
    val_ray_dataset = ray.data.from_items(list(val_dataset))

    # Load the tokenizer
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    def build_input_tensors(batch):
        batch["input_ids"] = []
        batch["attention_mask"] = []

        for text in batch["text"]:
            encoding = tokenizer(
                text,
                truncation=True,
                padding="max_length",
                max_length=128,
                return_tensors="np",
            )
            batch["input_ids"].append(encoding["input_ids"].squeeze())
            batch["attention_mask"] = encoding["attention_mask"].squeeze()
        return batch

    train_ray_dataset = train_ray_dataset.map_batches(build_input_tensors)
    val_ray_dataset = val_ray_dataset.map_batches(build_input_tensors)
    return {"train": train_ray_dataset, "val": val_ray_dataset}


# ------------------------
# Training Loop
# ------------------------


def train_loop_per_worker(config):
    # Synchronized model setup
    strategy = tf.distribute.MultiWorkerMirroredStrategy()
    with strategy.scope():
        # Define the optimizer and loss function
        optimizer = tf.keras.optimizers.Adam(
            learning_rate=config["lr"],
            epsilon=config["epsilon"],
            clipnorm=config["clipnorm"],
        )
        loss_fn = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
        # Load the pre-trained model
        model = transformers.TFAutoModelForSequenceClassification.from_pretrained(
            "bert-base-cased", num_labels=2
        )

        # Add a linear layer for binary classification
        model.layers[-1].activation = tf.keras.activations.softmax
        model.layers[-1].trainable = True

    train_dataset = session.get_dataset_shard("train")
    val_dataset = session.get_dataset_shard("val")

    worker_batch_size = config["batch_size"] // session.get_world_size()

    # Train the model
    for epoch in range(config["epochs"]):
        for batch in train_dataset.iter_batches(batch_size=worker_batch_size):
            inputs = tf.convert_to_tensor(batch["input_ids"], dtype=tf.float32)
            labels = tf.convert_to_tensor(batch["labels"], dtype=tf.float32)

            with tf.GradientTape() as tape:
                outputs = model(inputs)
                loss = loss_fn(labels, outputs.logits)

            grads = tape.gradient(loss, model.trainable_variables)
            optimizer.apply_gradients(zip(grads, model.trainable_variables))

        # Evaluate the model on the validation set
        val_loss, val_acc = 0, 0
        for batch in val_dataset.iter_batches(batch_size=worker_batch_size):
            inputs = tf.convert_to_tensor(batch["input_ids"], dtype=tf.float32)
            labels = tf.convert_to_tensor(batch["labels"], dtype=tf.float32)

            outputs = model(inputs)
            loss = loss_fn(labels, outputs.logits)

            val_loss += loss.numpy().mean()
            val_acc += (
                tf.argmax(outputs.logits, axis=-1).numpy() == labels.numpy()
            ).mean()

        val_loss /= len(val_dataset)
        val_acc /= len(val_dataset)

        if session.get_world_rank() == 0:
            print(
                f'Epoch {epoch+1}/{config["epochs"]} - val_loss: {val_loss:.4f} - val_acc: {val_acc:.4f}'
            )


# ------------------------
# Setup TensorflowTrainer
# ------------------------

train_loop_config = {
    "epsilon": 1e-8,
    "clipnorm": 1.0,
    "epochs": 10,
    "lr": 3e-5,
}

scaling_config = air.ScalingConfig(
    num_workers=2,
    use_gpu=True,
    trainer_resources={"CPU": 0},
    resources_per_worker={"CPU": 5.0, "GPU": 1.0},
)

ray_datasets = get_train_val_ray_datasets()

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_loop_config,
    scaling_config=scaling_config,
    datasets=ray_datasets,
)

result = trainer.fit()
