import os
import ray

from ray.util.sgd.data import ImageNetDataset

from ray.util.sgd.torch import TorchTrainer

import torch
import torch.nn as nn

MODEL_IN_DIMS = (3, 224, 224)
NUM_ACTORS = 4


def initialization_hook():
    print("NCCL DEBUG SET")
    # Need this for avoiding a connection restart issue
    os.environ["NCCL_SOCKET_IFNAME"] = "^docker0,lo"
    os.environ["NCCL_LL_THRESHOLD"] = "0"
    os.environ["NCCL_DEBUG"] = "INFO"


def optimizer_creator(model, config):
    """Returns optimizer"""
    return torch.optim.SGD(model.parameters(), lr=config.get("lr", 0.1))


def scheduler_creator(optimizer, config):
    return torch.optim.lr_scheduler.MultiStepLR(
        optimizer, milestones=[150, 250, 350], gamma=0.1)


def model_creator(_config):
    from timm import models
    return models.res2net50_26w_4s()


def make_data_creator(train_set, valid_set):
    def data_creator(config):
        train_loader = train_set.get_sharded_loader(NUM_ACTORS,
                                                    config["world_rank"])
        valid_loader = valid_set.get_sharded_loader(NUM_ACTORS,
                                                    config["world_rank"])
        return train_loader, valid_loader

    return data_creator


if __name__ == "__main__":
    ray.init("auto")
    print("Connected to cluster")
    print("Cluster resources:", ray.cluster_resources())
    prefix = "/media/imagenet/"
    # prefix = "/users/alex/anyscale/data/"
    train_loc = prefix + "ILSVRC/Data/CLS-LOC/train"
    valid_loc = prefix + "ILSVRC/Data/CLS-LOC/val"
    val_sol_path = prefix + "LOC_val_solution.csv"

    # Our only reliance on timm is for some convenient preprocessing
    from timm.data import transforms_factory
    transform = transforms_factory.create_transform(MODEL_IN_DIMS)

    train_dataset = ImageNetDataset(
        train_loc, "train", max_paths=10000, transform=transform)
    print("Done loading training dataset: ", len(train_dataset))
    valid_dataset = ImageNetDataset(
        valid_loc,
        "validate",
        val_sol_path=val_sol_path,
        max_paths=10000,
        transform=transform)
    print("Done loading validation dataset: ", len(valid_dataset))
    data_creator = make_data_creator(train_dataset, valid_dataset)

    use_gpu = True

    trainer1 = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=nn.CrossEntropyLoss,
        scheduler_creator=scheduler_creator,
        initialization_hook=initialization_hook,
        num_workers=NUM_ACTORS,
        use_gpu=use_gpu,
        backend="nccl" if use_gpu else "gloo",
        scheduler_step_freq="epoch",
        use_fp16=use_gpu)

    for i in range(5):
        print("Starting epoch!")
        # Increase `max_retries` to turn on fault tolerance.
        stats = trainer1.train(max_retries=0)
        print(stats)

    # print(trainer1.validate())
    # trainer1.shutdown()
    # print("success!")
