import tensorflow as tf
import transformers
from datasets import load_dataset
from transformers import TFAutoModelForSequenceClassification, AutoTokenizer
from ray import air
from ray.air import session
from ray.train.tensorflow import TensorflowTrainer

# -------------------------------
# Data Ingestion with TF Dataset
# -------------------------------


def get_train_val_datasets():
    # Split into training and validation sets
    train_dataset = load_dataset("imdb", split="train")
    val_dataset = load_dataset("imdb", split="test")

    # Load the tokenizer
    tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")

    # Tokenize the input text
    def tokenize_function(examples):
        return tokenizer(
            examples["text"], padding="max_length", truncation=True, max_length=512
        )

    train_dataset = train_dataset.map(tokenize_function, batched=True)
    val_dataset = val_dataset.map(tokenize_function, batched=True)

    # Convert the labels to numerical values
    def label_function(examples):
        return {
            "labels": [1 if label == "positive" else 0 for label in examples["label"]]
        }

    train_dataset = train_dataset.map(label_function)
    val_dataset = val_dataset.map(label_function)

    # Define the TensorFlow Dataset
    train_dataset = train_dataset.shuffle(10000).batch(32).prefetch(tf.data.AUTOTUNE)
    val_dataset = val_dataset.batch(32).prefetch(tf.data.AUTOTUNE)
    return train_dataset, val_dataset


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

    train_dataset, val_dataset = get_train_val_datasets()

    # Train the model
    for epoch in range(config["epochs"]):
        for batch in train_dataset:
            inputs = {key: batch[key] for key in batch.keys() if key != "labels"}
            labels = batch["labels"]

            with tf.GradientTape() as tape:
                outputs = model(inputs)
                loss = loss_fn(labels, outputs.logits)

            grads = tape.gradient(loss, model.trainable_variables)
            optimizer.apply_gradients(zip(grads, model.trainable_variables))

        # Evaluate the model on the validation set
        val_loss, val_acc = 0, 0
        for batch in val_dataset:
            inputs = {key: batch[key] for key in batch.keys() if key != "labels"}
            labels = batch["labels"]

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

trainer = TensorflowTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config=train_loop_config,
    scaling_config=scaling_config,
)

result = trainer.fit()
