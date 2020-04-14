# Based on work by Ross Wightman as part of the timm package
# (see LICENSE_THIRDPARTY)
#
# As modified by
# - Maksim Smolin in 2020

# Note: other authors MUST include themselves in the above copyright notice
#       in order to abide by the terms of the Apache license

from os.path import join

from tqdm import trange

import torch.nn as nn

from timm.data import Dataset, create_loader, resolve_data_config, FastCollateMixup
from timm.models import create_model
from timm.optim import create_optimizer

import ray
from ray.util.sgd.utils import BATCH_SIZE

from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

from args import parse_args

class Namespace(dict):
    def __init__(self):
        pass

    def __getattr__(self, attr):
        if attr not in self:
            raise AttributeError(attr)

        return self[attr]

    def __setattr__(self, attr, value):
        self[attr] = value

# class TrainOp(TrainingOperator):
#     def setup(self, config):
#         pass

#     @override(TrainingOperator)
#     def train_batch(self, batch, batch_info):
#         pass

def model_creator(config):
    args = config["args"]

    return create_model(
        "resnet101", # args.model,
        pretrained=args.pretrained,
        num_classes=args.num_classes,
        drop_rate=args.drop,
        drop_connect_rate=args.drop_connect,  # DEPRECATED, use drop_path
        drop_path_rate=args.drop_path,
        drop_block_rate=args.drop_block,
        global_pool=args.gp,
        bn_tf=args.bn_tf,
        bn_momentum=args.bn_momentum,
        bn_eps=args.bn_eps,
        checkpoint_path=args.initial_checkpoint)

def data_creator(config):
    args = config["args"]

    data_config = resolve_data_config(vars(args))

    dataset_train = Dataset(join(args.data, "train"))
    dataset_eval = Dataset(join(args.data, "val"))

    num_aug_splits = 0 # todo: support aug splits

    collate_fn = None
    if args.prefetcher and args.mixup > 0:
        assert not num_aug_splits  # collate conflict (need to support deinterleaving in collate mixup)
        collate_fn = FastCollateMixup(args.mixup, args.smoothing, args.num_classes)

    common_params = dict(
        input_size=data_config["input_size"],
        use_prefetcher=args.prefetcher,
        mean=data_config["mean"],
        std=data_config["std"],
        num_workers=args.workers,
        distributed=args.distributed,
        pin_memory=args.pin_mem)

    train_loader = create_loader(
        dataset_train,
        is_training=True,
        batch_size=config[BATCH_SIZE],
        re_prob=args.reprob,
        re_mode=args.remode,
        re_count=args.recount,
        re_split=args.resplit,
        collate_fn=collate_fn,
        color_jitter=args.color_jitter,
        auto_augment=args.aa,
        interpolation=args.train_interpolation,
        num_aug_splits=num_aug_splits,
        **common_params)
    eval_loader = create_loader(
        dataset_eval,
        is_training=False,
        batch_size=args.validation_batch_size_multiplier * config[BATCH_SIZE],
        interpolation=data_config["interpolation"],
        crop_pct=data_config["crop_pct"],
        **common_params)

    return train_loader, eval_loader

def optimizer_creator(model, config):
    args = config["args"]
    return create_optimizer(args, model)

def loss_creator(config):
    # there should be more complicated logic here, but we don't support
    # separate train and eval losses yet
    return nn.CrossEntropyLoss()

def main():
    args, args_text = parse_args()

    ray.init(address=args.ray_address)

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=loss_creator,
        use_tqdm=True,
        config={
            "args": args,
            BATCH_SIZE: args.batch_size
        },
        num_workers=args.ray_num_workers)

    pbar = trange(args.epochs, unit="epoch")
    for i in pbar:
        trainer.train()

        val_stats = trainer1.validate()
        pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))

    trainer.shutdown()

if __name__ == "__main__":
    main()
