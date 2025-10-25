import argparse

from ray.train import RunConfig, ScalingConfig
from ray.train.v2.jax import JaxTrainer


def train_loop():
    import jax
    from ray import train

    devices = jax.devices()
    train.report({"devices": [str(d) for d in devices]})


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--num-workers", type=int, default=1)
    parser.add_argument("--use-tpu", type=bool, default=False)
    parser.add_argument("--accelerator-per-worker", type=int, default=8)
    parser.add_argument("--accelerator-type", type=str, default="TPU-V6E")
    args = parser.parse_args()

    trainer = JaxTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(
            num_workers=args.num_workers,
            resources_per_worker={"TPU": args.accelerator_per_worker},
            accelerator_type=args.accelerator_type,
            use_tpu=args.use_tpu,
        ),
        run_config=RunConfig(),
    )
    result = trainer.fit()
    assert result.error is None


if __name__ == "__main__":
    main()
