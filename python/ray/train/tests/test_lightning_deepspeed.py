import os
import pytest

import ray
from ray.train import CheckpointConfig, RunConfig
from ray.air.constants import MODEL_KEY
from ray.train.lightning import LightningConfigBuilder, LightningTrainer
from ray.train.tests.lightning_test_utils import (
    LinearModule,
    DummyDataModule,
)


@pytest.fixture
def ray_start_6_cpus_4_gpus():
    address_info = ray.init(num_cpus=6, num_gpus=4)
    yield address_info
    # The code after the yield will run as teardown code.
    ray.shutdown()


@pytest.mark.parametrize("stage", [1, 2, 3])
@pytest.mark.parametrize("test_restore", [True, False])
def test_deepspeed_stages(ray_start_6_cpus_4_gpus, tmpdir, stage, test_restore):
    # Reduce tests number
    if test_restore and stage < 3:
        return

    exp_name = f"test_deepspeed_stage_{stage}"
    num_epochs = 5
    batch_size = 8
    num_workers = 4
    dataset_size = 256

    config_builder = (
        LightningConfigBuilder()
        .module(
            LinearModule,
            input_dim=32,
            output_dim=4,
            strategy="deepspeed",
            fail_epoch=3 if test_restore else -1,
        )
        .trainer(max_epochs=num_epochs, accelerator="gpu")
        .strategy(name="deepspeed", stage=stage)
        .fit_params(datamodule=DummyDataModule(batch_size, dataset_size))
        .checkpointing(dirpath="my/ckpt/dir", monitor="val_loss", save_top_k=3)
    )

    scaling_config = ray.train.ScalingConfig(num_workers=num_workers, use_gpu=True)

    trainer = LightningTrainer(
        lightning_config=config_builder.build(),
        scaling_config=scaling_config,
        run_config=RunConfig(
            name=exp_name,
            storage_path=str(tmpdir),
            checkpoint_config=CheckpointConfig(
                num_to_keep=3,
                checkpoint_score_attribute="val_loss",
                checkpoint_score_order="min",
                _checkpoint_keep_all_ranks=True,
            ),
        ),
    )

    if test_restore:
        with pytest.raises(RuntimeError):
            trainer.fit()

        # Check reloading deepspeed checkpoint and resume training
        trainer = LightningTrainer.restore(str(tmpdir / exp_name))
        trainer.fit()
    else:
        result = trainer.fit()

        # Check all deepspeed model/optimizer shards are saved
        all_files = os.listdir(f"{result.checkpoint.path}/{MODEL_KEY}/checkpoint")
        for rank in range(num_workers):
            full_model = "mp_rank_00_model_states.pt"
            model_shard = f"zero_pp_rank_{rank}_mp_rank_00_model_states.pt"
            optim_shard = f"zero_pp_rank_{rank}_mp_rank_00_optim_states.pt"

            assert (
                optim_shard in all_files
            ), f"[stage-{stage}] Optimizer states `{optim_shard}` doesn't exist!"

            if stage == 3:
                assert (
                    model_shard in all_files
                ), f"[stage-{stage}] Model states {model_shard} doesn't exist!"
            else:
                assert (
                    full_model in all_files
                ), f"[stage-{stage}] Model states {full_model} doesn't exist!"


if __name__ == "__main__":
    import sys

    import pytest

    sys.exit(pytest.main(["-v", "-x", __file__]))
