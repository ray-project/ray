# Based on work by Ross Wightman as part of the timm package
# (see LICENSE_THIRDPARTY)
#
# As modified by
# - Maksim Smolin in 2020

# Note: other authors MUST include themselves in the above copyright notice
#       in order to abide by the terms of the Apache license

from os.path import join

from ray.util.sgd.torch import TrainingOperator
from tqdm import trange

import torch.nn as nn

from timm.data import Dataset, create_loader
from timm.data import resolve_data_config, FastCollateMixup
from timm.models import create_model, convert_splitbn_model
from timm.optim import create_optimizer
from timm.utils import setup_default_logging

import ray
from ray.util.sgd.utils import BATCH_SIZE

from ray.util.sgd import TorchTrainer
# from ray.util.sgd.torch import TrainingOperator

from ray.util.sgd.torch.examples.image_models.args import parse_args
import ray.util.sgd.torch.examples.image_models.util as util


def model_creator(config):
    args = config["args"]

    model = create_model(
        args.model,
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

    # always false right now
    if args.split_bn:
        assert args.num_aug_splits > 1 or args.resplit
        model = convert_splitbn_model(model, max(args.num_aug_splits, 2))

    return model


def data_creator(config):
    # torch.manual_seed(args.seed + torch.distributed.get_rank())

    args = config["args"]

    train_dir = join(args.data, "train")
    val_dir = join(args.data, "val")

    if args.mock_data:
        util.mock_data(train_dir, val_dir)

    # todo: verbose should depend on rank
    data_config = resolve_data_config(vars(args), verbose=True)

    dataset_train = Dataset(join(args.data, "train"))
    dataset_eval = Dataset(join(args.data, "val"))

    collate_fn = None
    if args.prefetcher and args.mixup > 0:
        # collate conflict (need to support deinterleaving in collate mixup)
        assert args.num_aug_splits == 0
        collate_fn = FastCollateMixup(args.mixup, args.smoothing,
                                      args.num_classes)

    common_params = dict(
        input_size=data_config["input_size"],
        use_prefetcher=args.prefetcher,
        mean=data_config["mean"],
        std=data_config["std"],
        num_workers=1,
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
        num_aug_splits=args.num_aug_splits,  # always 0 right now
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
    setup_default_logging()

    args, args_text = parse_args()
    if args.smoke_test:
        ray.init(num_cpus=int(args.ray_num_workers))
    else:
        ray.init(address=args.ray_address)

    CustomTrainingOperator = TrainingOperator.from_creators(
        model_creator=model_creator,
        optimizer_creator=optimizer_creator,
        data_creator=data_creator,
        loss_creator=loss_creator)
    trainer = TorchTrainer(
        training_operator_cls=CustomTrainingOperator,
        use_tqdm=True,
        use_fp16=args.amp,
        config={
            "args": args,
            BATCH_SIZE: args.batch_size
        },
        num_workers=args.ray_num_workers)

    if args.smoke_test:
        args.epochs = 1

    pbar = trange(args.epochs, unit="epoch")
    for i in pbar:
        trainer.train(num_steps=1 if args.smoke_test else None)

        val_stats = trainer.validate(num_steps=1 if args.smoke_test else None)
        pbar.set_postfix(dict(acc=val_stats["val_accuracy"]))

    trainer.shutdown()


if __name__ == "__main__":
    main()
