from ray.train.lightning import LightningPredictor, LightningCheckpoint
from utils import LightningMNISTClassifier, MNISTDataModule, LightningMNISTModelConfig

ckpt_path = "/mnt/cluster_storage/ray_lightning_results/ptl-e2e-classifier/LightningTrainer_89272_00000_0_2023-02-28_14-30-48/checkpoint_000009"
checkpoint_local = LightningCheckpoint.from_directory(ckpt_path)

checkpoint_uri = ""
checkpoint_s3 = LightningCheckpoint.from_uri(checkpoint_uri)

def predict_one(checkpoint, dataloader):
    model_init_config = {"config": LightningMNISTModelConfig}
    predictor = LightningPredictor.from_checkpoint(
        checkpoint=checkpoint, model=LightningMNISTClassifier, model_init_config=model_init_config, use_gpu=True)
    batch = next(iter(dataloader))
    input_batch = batch[0].numpy()
    print(predictor.predict(input_batch))

if __name__ == "__main__":
    datamodule = MNISTDataModule(batch_size=10)
    datamodule.setup()
    dataloader = datamodule.val_dataloader()

    predict_one(checkpoint_local, dataloader)
    predict_one(checkpoint_s3, dataloader)