import json
import os
import shutil
import tempfile

import mlflow
import numpy as np
import torch
import torch.nn.functional as F
from doggos.data import Preprocessor
from doggos.model import ClassificationModel, collate_fn
from doggos.utils import add_class, set_seeds
from ray.train.torch import TorchTrainer

import ray


def train_epoch(ds, batch_size, model, num_classes, loss_fn, optimizer):
    model.train()
    loss = 0.0
    ds_generator = ds.iter_torch_batches(batch_size=batch_size, collate_fn=collate_fn)
    for i, batch in enumerate(ds_generator):
        optimizer.zero_grad()  # reset gradients
        z = model(batch)  # forward pass
        targets = F.one_hot(batch["label"], num_classes=num_classes).float()
        J = loss_fn(z, targets)  # define loss
        J.backward()  # backward pass
        optimizer.step()  # update weights
        loss += (J.detach().item() - loss) / (i + 1)  # cumulative loss
    return loss


def eval_epoch(ds, batch_size, model, num_classes, loss_fn):
    model.eval()
    loss = 0.0
    y_trues, y_preds = [], []
    ds_generator = ds.iter_torch_batches(batch_size=batch_size, collate_fn=collate_fn)
    with torch.inference_mode():
        for i, batch in enumerate(ds_generator):
            z = model(batch)
            targets = F.one_hot(
                batch["label"], num_classes=num_classes
            ).float()  # one-hot (for loss_fn)
            J = loss_fn(z, targets).item()
            loss += (J - loss) / (i + 1)
            y_trues.extend(batch["label"].cpu().numpy())
            y_preds.extend(torch.argmax(z, dim=1).cpu().numpy())
    return loss, np.vstack(y_trues), np.vstack(y_preds)


def train_loop_per_worker(config):
    # Hyperparameters
    model_registry = config["model_registry"]
    experiment_name = config["experiment_name"]
    embedding_dim = config["embedding_dim"]
    hidden_dim = config["hidden_dim"]
    dropout_p = config["dropout_p"]
    lr = config["lr"]
    lr_factor = config["lr_factor"]
    lr_patience = config["lr_patience"]
    num_epochs = config["num_epochs"]
    batch_size = config["batch_size"]
    num_classes = config["num_classes"]

    # Experiment tracking
    if ray.train.get_context().get_world_rank() == 0:
        mlflow.set_tracking_uri(f"file:{model_registry}")
        mlflow.set_experiment(experiment_name)
        mlflow.start_run()
        mlflow.log_params(config)

    # Datasets
    set_seeds()
    train_ds = ray.train.get_dataset_shard("train")
    val_ds = ray.train.get_dataset_shard("val")

    # Model
    model = ClassificationModel(
        embedding_dim=embedding_dim,
        hidden_dim=hidden_dim,
        dropout_p=dropout_p,
        num_classes=num_classes,
    )
    model = ray.train.torch.prepare_model(model)

    # Training components
    loss_fn = torch.nn.CrossEntropyLoss()
    optimizer = torch.optim.Adam(model.parameters(), lr=lr)
    scheduler = torch.optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, mode="min", factor=lr_factor, patience=lr_patience
    )

    # Training
    best_val_loss = float("inf")
    for epoch in range(num_epochs):
        # Steps
        train_loss = train_epoch(
            train_ds, batch_size, model, num_classes, loss_fn, optimizer
        )
        val_loss, _, _ = eval_epoch(val_ds, batch_size, model, num_classes, loss_fn)
        scheduler.step(val_loss)

        # Checkpoint
        with tempfile.TemporaryDirectory() as dp:
            model.module.save(dp=dp)
            metrics = dict(
                lr=optimizer.param_groups[0]["lr"],
                train_loss=train_loss,
                val_loss=val_loss,
            )
            with open(os.path.join(dp, "class_to_label.json"), "w") as fp:
                json.dump(config["class_to_label"], fp, indent=4)
            if ray.train.get_context().get_world_rank() == 0:
                mlflow.log_metrics(metrics, step=epoch)
                if val_loss < best_val_loss:
                    best_val_loss = val_loss
                    mlflow.log_artifacts(dp)

    # End experiment tracking
    if ray.train.get_context().get_world_rank() == 0:
        mlflow.end_run()


if __name__ == "__main__":

    # Train loop config
    model_registry = "/mnt/user_storage/mlflow/doggos"
    if os.path.isdir(model_registry):
        shutil.rmtree(model_registry)  # clean up
    os.makedirs(model_registry, exist_ok=True)
    experiment_name = "doggos"
    train_loop_config = {
        "model_registry": model_registry,
        "experiment_name": experiment_name,
        "embedding_dim": 512,
        "hidden_dim": 256,
        "dropout_p": 0.3,
        "lr": 1e-3,
        "lr_factor": 0.8,
        "lr_patience": 3,
        "num_epochs": 20,
        "batch_size": 256,
    }

    # Scaling config
    num_workers = 2
    scaling_config = ray.train.ScalingConfig(
        num_workers=num_workers,
        use_gpu=True,
        resources_per_worker={"CPU": 8, "GPU": 2},
        accelerator_type="T4",
    )

    # Datasets
    set_seeds()
    train_ds = ray.data.read_images(
        "s3://doggos-dataset/train",
        include_paths=True,
        shuffle="files",
    )
    train_ds = train_ds.map(add_class)
    val_ds = ray.data.read_images(
        "s3://doggos-dataset/val",
        include_paths=True,
    )
    val_ds = val_ds.map(add_class)

    # Preprocess
    preprocessor = Preprocessor()
    preprocessor = preprocessor.fit(train_ds, column="class")
    train_ds = preprocessor.transform(ds=train_ds)
    val_ds = preprocessor.transform(ds=val_ds)

    # Write processed data to cloud storage
    preprocessed_data_path = os.path.join(
        "/mnt/cluster_storage", "doggos/preprocessed_data"
    )
    if os.path.exists(preprocessed_data_path):
        shutil.rmtree(preprocessed_data_path)  # clean up
    preprocessed_train_path = os.path.join(preprocessed_data_path, "preprocessed_train")
    preprocessed_val_path = os.path.join(preprocessed_data_path, "preprocessed_val")
    train_ds.write_parquet(preprocessed_train_path)
    val_ds.write_parquet(preprocessed_val_path)

    # Load preprocessed datasets
    preprocessed_train_ds = ray.data.read_parquet(preprocessed_train_path)
    preprocessed_val_ds = ray.data.read_parquet(preprocessed_val_path)

    # Trainer
    train_loop_config["class_to_label"] = preprocessor.class_to_label
    train_loop_config["num_classes"] = len(preprocessor.class_to_label)
    trainer = TorchTrainer(
        train_loop_per_worker=train_loop_per_worker,
        train_loop_config=train_loop_config,
        scaling_config=scaling_config,
        datasets={"train": preprocessed_train_ds, "val": preprocessed_val_ds},
    )

    # Train
    results = trainer.fit()
