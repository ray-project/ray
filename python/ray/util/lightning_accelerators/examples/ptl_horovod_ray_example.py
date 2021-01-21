import ray
from ray.util.lightning_accelerators import HorovodRayAccelerator

import pytorch_lightning as pl
from pl_bolts.datamodules import MNISTDataModule

from ray.tune.examples.mnist_ptl_mini import LightningMNISTClassifier

ray.init()

config = {"layer_1": 32, "layer_2": 64, "lr": 1e-1, "batch_size": 32}

data_dir = "./mnist_data"
model = LightningMNISTClassifier(config, data_dir)
dm = MNISTDataModule(
    data_dir=data_dir, num_workers=0, batch_size=config["batch_size"])
trainer = pl.Trainer(
    max_epochs=1,
    gpus=0,
    # Use 1 machine.
    num_nodes=1,
    # A total of 4 workers per machine.
    num_processes=4,
    accelerator=HorovodRayAccelerator())
trainer.fit(model, dm)
