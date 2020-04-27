# Based on work by Ross Wightman as part of the timm package
# (see LICENSE_THIRDPARTY)
#
# As modified by
# - Maksim Smolin in 2020

# Note: other authors MUST include themselves in the above copyright notice
#       in order to abide by the terms of the Apache license

import time
import logging
from os.path import join

from tqdm import trange

import torchvision
import torch.nn as nn

from timm.data import Dataset, create_loader
from timm.data import resolve_data_config, FastCollateMixup
from timm.models import create_model, convert_splitbn_model
from timm.optim import create_optimizer
from timm.utils import setup_default_logging

import ray
from ray.util.sgd.utils import BATCH_SIZE

from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

import ray.util.sgd.torch.examples.image_models.util as util
from ray.util.sgd.torch.examples.image_models.args import parse_args

class SegOperator(TrainingOperator):
    def setup(self):
        self.model_ema = None

    def train_batch(self, (input, target), batch_info):
        args = self.config["args"]

        if self.use_gpu:
            input = input.cuda(non_blocking=True)
            target = target.cuda(non_blocking=True)

        if not args.prefetcher:
            # todo: support no-gpu training
            if args.mixup > 0.:
                mixup_disabled = False
                if args.mixup_off_epoch:
                    mixup_disabled = info["epoch_idx"] >= args.mixup_off_epoch
                input, target = mixup_batch(
                    input, target,
                    alpha=args.mixup,
                    num_classes=args.num_classes,
                    smoothing=args.smoothing,
                    disable=mixup_disabled)

        with self.timers.record("fwd"):
            output = self.model(input)
            loss = self.criterion(output, target)

        with self.timers.record("grad"):
            self.optimizer.zero_grad()
            if self.use_fp16:
                with amp.scale_loss(loss, self.optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        with self.timers.record("apply"):
            self.optimizer.step()

        torch.cuda.synchronize()

        if model_ema is not None:
            model_ema.update(model)

        return {"train_loss": loss.item(), NUM_SAMPLES: features.size(0)}

    def train_epoch(self, iterator, info):
        args = self.config["args"]

        if self.use_tqdm and self.world_rank == 0:
            desc = ""
            if info is not None and "epoch_idx" in info:
                if "num_epochs" in info:
                    desc = "{}/{}e".format(info["epoch_idx"] + 1,
                                           info["num_epochs"])
                else:
                    desc = "{}e".format(info["epoch_idx"] + 1)
            _progress_bar = tqdm(
                total=info[NUM_STEPS] or len(self.train_loader),
                desc=desc,
                unit="batch",
                leave=False)

        loader = self.train_loader
        if args.prefetcher and args.mixup > 0 and loader.mixup_enabled:
            if args.mixup_off_epoch and epoch >= args.mixup_off_epoch:
                loader.mixup_enabled = False

        batch_time_m = AverageMeter()
        data_time_m = AverageMeter()
        losses_m = AverageMeter()

        self.model.train()

        end = time.time()
        last_idx = len(loader) - 1
        num_updates = epoch * len(loader)
        for batch_idx, batch in enumerate(iterator):
            last_batch = batch_idx == last_idx
            data_time_m.update(time.time() - end)

            batch_info = {
                "batch_idx": batch_idx,
                "global_step": self.global_step
            }
            batch_info.update(info)
            metrics = self.train_batch(batch, batch_info=batch_info)

            batch_time_m.update(time.time() - end)

            if last_batch or batch_idx % args.log_interval == 0:
                lrl = [
                    param_group['lr']
                    for param_group in self.optimizer.param_groups
                ]
                lr = sum(lrl) / len(lrl)

                # fixme: this doesnt work at all
                # reduced_loss = reduce_tensor(loss.data, args.world_size)
                # losses_m.update(metrics["train_loss"], metrics["num_samples"])

                if self.world_rank == 0:
                    total_samples = (
                        metrics["num_samples"] * args.ray_num_workers)

                    logging.info(
                        "Train: {} [{:>4d}/{} ({:>3.0f}%)]  "
                        "Loss: {loss.val:>9.6f} ({loss.avg:>6.4f})  "
                        "Time: {batch_time.val:.3f}s, {rate:>7.2f}/s  "
                        "({batch_time.avg:.3f}s, {rate_avg:>7.2f}/s)  "
                        "LR: {lr:.3e}  "
                        "Data: {data_time.val:.3f} ({data_time.avg:.3f})".format(
                            epoch,
                            batch_idx, len(loader),
                            100. * batch_idx / last_idx,
                            loss=losses_m,
                            batch_time=batch_time_m,
                            rate=total_samples / batch_time_m.val,
                            rate_avg=total_samples / batch_time_m.avg,
                            lr=lr,
                            data_time=data_time_m))

                    # todo: calculate output_dir
                    # if args.save_images and output_dir:
                    #     torchvision.utils.save_image(
                    #         input,
                    #         os.path.join(
                    #             output_dir,
                    #             "train-batch-%d.jpg" % batch_idx),
                    #         padding=0,
                    #         normalize=True)

            if self.use_tqdm and self.world_rank == 0:
                _progress_bar.n = batch_idx + 1
                postfix = {}
                if "train_loss" in metrics:
                    postfix.update(loss=metrics["train_loss"])
                _progress_bar.set_postfix(postfix)

            # todo: we must do this globally
            # if saver is not None and args.recovery_interval and (
            #     last_batch or (batch_idx + 1) % args.recovery_interval == 0):
            #     saver.save_recovery(
            #         model, optimizer, args, epoch,
            #         model_ema=model_ema, use_amp=use_amp, batch_idx=batch_idx)

            # fixme: need metric
            # if self.scheduler is not None:
            #     self.scheduler.step_update(
            #         num_updates=self.global_step, metric=losses_m.avg)

            end = time.time()
            self.global_step += 1


    if hasattr(self.optimizer, 'sync_lookahead'):
        self.optimizer.sync_lookahead()

    # return OrderedDict([('loss', losses_m.avg)])

    # return metric_meters.summary()

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

    ray.init(address=args.ray_address)

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=loss_creator,
        use_tqdm=True,
        use_fp16=args.amp,
        apex_args={"opt_level": "O1"},
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

    print('Done')
    trainer.shutdown()
    print('Shutdown')


if __name__ == "__main__":
    main()
