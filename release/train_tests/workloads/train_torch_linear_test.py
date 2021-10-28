import ray
from ray.train import Trainer
from ray.train.callbacks import JsonLoggerCallback, TBXLoggerCallback

from ray.train.examples.train_linear_example import train_func

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test",
        action="store_true",
        help=("Finish quickly for testing."))
    args = parser.parse_args()

    ray.init(address="auto")

    trainer = Trainer("torch", num_workers=4, use_gpu=True)
    config = {"lr": 1e-2, "hidden_size": 1, "batch_size": 4, "epochs": 3}
    trainer.start()
    results = trainer.run(
        train_func,
        config,
        callbacks=[JsonLoggerCallback(),
                   TBXLoggerCallback()])
    trainer.shutdown()
