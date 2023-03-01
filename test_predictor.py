from ray.train.lightning import LightningPredictor, LightningCheckpoint
from ptl_tests.utils import LightningMNISTClassifier, MNISTDataModule, LightningMNISTModelConfig
from ray.air import Checkpoint

import gc

import ray

# ckpt_path = "/mnt/cluster_storage/ray_lightning_results/ptl-e2e-classifier/LightningTrainer_89272_00000_0_2023-02-28_14-30-48/checkpoint_000009"
# checkpoint_local = LightningCheckpoint.from_directory(ckpt_path)

checkpoint_uri = "s3://anyscale-yunxuanx-demo/checkpoint_000008/"
# checkpoint_s3 = LightningCheckpoint.from_uri(checkpoint_uri)

Checkpoint.from_uri(checkpoint_uri)


def predict_one(checkpoint, dataloader):
    model_init_config = {"config": LightningMNISTModelConfig}
    predictor = LightningPredictor.from_checkpoint(
        checkpoint=checkpoint, model=LightningMNISTClassifier, model_init_config=model_init_config, use_gpu=False)
    batch = next(iter(dataloader))
    input_batch = batch[0].numpy()
    print(predictor.predict(input_batch))

if __name__ == "__main__":
    pass
    # datamodule = MNISTDataModule(batch_size=10)
    # datamodule.setup()
    # dataloader = datamodule.val_dataloader()

    # predict_one(checkpoint_local, dataloader)
    # predict_one(checkpoint_s3, dataloader)
    
    # batch = next(iter(dataloader))
    # input_batch = batch[0]

    # model = LightningMNISTClassifier(LightningMNISTModelConfig)
    # model.predict_step(input_batch, 0)