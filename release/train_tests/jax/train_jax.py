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
    parser.add_argument("--use-tpu", action="store_true", default=False)
    args = parser.parse_args()

    trainer = JaxTrainer(
        train_loop_per_worker=train_loop,
        scaling_config=ScalingConfig(
            num_workers=args.num_workers,
            use_tpu=args.use_tpu,
        ),
        run_config=RunConfig(),
    )
    result = trainer.fit()
    assert result.error is None


if __name__ == "__main__":
    main()
