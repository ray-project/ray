import argparse

import torch
from torch.nn.modules.utils import consume_prefix_in_state_dict_if_present
from torch.profiler import profile, record_function, schedule

import ray
import ray.train as train
from ray.train import Trainer
from ray.train.callbacks import TBXLoggerCallback
from ray.train.callbacks.profile import TorchTensorboardProfilerCallback
from ray.train.torch import TorchWorkerProfiler


def train_func():
    twp = TorchWorkerProfiler()
    with profile(
        activities=[],
        schedule=schedule(wait=0, warmup=0, active=1),
        on_trace_ready=twp.trace_handler,
    ) as p:

        # Setup model.
        model = torch.nn.Linear(1, 1)
        model = train.torch.prepare_model(model)
        loss_fn = torch.nn.MSELoss()
        optimizer = torch.optim.SGD(model.parameters(), lr=1e-2)

        # Setup data.
        input = torch.randn(1000, 1)
        labels = input * 2
        dataset = torch.utils.data.TensorDataset(input, labels)
        dataloader = torch.utils.data.DataLoader(dataset, batch_size=32)
        dataloader = train.torch.prepare_data_loader(dataloader)

        # Train.
        for epoch in range(5):
            with record_function("train_epoch"):
                for X, y in dataloader:
                    pred = model(X)
                    loss = loss_fn(pred, y)
                    optimizer.zero_grad()
                    loss.backward()
                    optimizer.step()

            with record_function("train_checkpoint"):
                state_dict = model.state_dict()
                consume_prefix_in_state_dict_if_present(state_dict, "module.")
                train.save_checkpoint(epoch=epoch, model_weights=state_dict)

            p.step()

            with record_function("train_report"):
                profile_results = twp.get_and_clear_profile_traces()
                train.report(epoch=epoch, **profile_results)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--address", required=False, type=str, help="the address to use for Ray"
    )
    parser.add_argument(
        "--num-workers",
        "-n",
        type=int,
        default=2,
        help="Sets number of workers for training.",
    )
    parser.add_argument(
        "--use-gpu", action="store_true", default=False, help="Enables GPU training"
    )

    args = parser.parse_args()

    ray.init(address=args.address)

    callbacks = [TorchTensorboardProfilerCallback(), TBXLoggerCallback()]
    trainer = Trainer(
        backend="torch", num_workers=args.num_workers, use_gpu=args.use_gpu
    )
    trainer.start()
    trainer.run(train_func, callbacks=callbacks)
    trainer.shutdown()
