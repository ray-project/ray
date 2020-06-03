#!/usr/bin/env python

# Based on work by Ross Wightman as part of the timm package
# (see LICENSE_THIRDPARTY)
#
# As modified by
# - Maksim Smolin in 2020

# Note: other authors MUST include themselves in the above copyright notice
#       in order to abide by the terms of the Apache license

import time
import logging
import os
from os.path import join

from tqdm import tqdm, trange

import torchvision
import torch
import torch.nn as nn

from timm.data.distributed_sampler import OrderedDistributedSampler
from torch.utils.data.distributed import DistributedSampler
from torch.utils.data import BatchSampler

from timm.data import Dataset, create_loader
from timm.data import resolve_data_config, FastCollateMixup
from timm.models import create_model, convert_splitbn_model
from timm.optim import create_optimizer
from timm.utils import setup_default_logging, distribute_bn, AverageMeter
from timm.loss import LabelSmoothingCrossEntropy, SoftTargetCrossEntropy
from timm.loss import JsdCrossEntropy
from timm.utils import OrderedDict, reduce_tensor
from timm.scheduler import create_scheduler

import ray
from ray.util.sgd.utils import BATCH_SIZE
import ray.util.sgd.utils as sgd_utils

from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

from ray.util.sgd.torch.constants import NUM_STEPS
from ray.util.sgd.utils import NUM_SAMPLES

import ray.util.sgd.torch.examples.image_models.util as util
from ray.util.sgd.torch.examples.image_models.args import parse_args
import ray.util.sgd.torch.examples.image_models.util as util

def accuracy(output, target, topk=(1,)):
    """
    Computes the accuracy over the k top predictions for the specified values of k
    """
    maxk = max(topk)
    batch_size = target.size(0)

    _, pred = output.topk(maxk, 1, True, True)
    pred = pred.t()

    correct = pred.eq(target.view(1, -1).expand_as(pred))

    return [
        correct[:k].view(-1).float().sum(0) * 100. / batch_size
        for k in topk
    ]

amp = None
try:
    from apex import amp
except ImportError:
    pass

# todo: none of the custom checkpointing actually works. at all
# but actually we should be saving most of the state properly
class ImagenetOperator(TrainingOperator):
    def wandb_log(self, *log_args, **log_kwargs):
        args = self.config["args"]

        if args.wandb and sgd_utils.world_rank() == 0:
            import wandb
            wandb.log(
                step=self.global_step, *log_args, **log_kwargs)

    def setup(self, config):
        args = self.config["args"]

        self.pending_scheduler_val_steps = 0

        # batch_sampler = BatchSampler(
        #     DistributedSampler(self.train_loader.dataset),
        #     self.train_loader.batch_size,
        #     self.train_loader.drop_last)
        # self.train_loader.batch_sampler = batch_sampler

        # # This will add extra duplicate entries to result in equal num
        # # of samples per-process, will slightly alter validation results
        # batch_sampler = BatchSampler(
        #     OrderedDistributedSampler(self.validation_loader.dataset),
        #     self.validation_loader.batch_size,
        #     self.validation_loader.drop_last)
        # self.validation_loader.batch_sampler = batch_sampler
        # # we can't patch sampler directly after creation,
        # # but if you look at the DataLoader constructor sources,
        # # we can just make a batch_sampler out of our new sampler and then
        # # set that

        # # fixme: haha jk we cant do this in 1.4.0

        has_apex = False
        try:
            from apex.parallel import DistributedDataParallel as DDP
            from apex.parallel import convert_syncbn_model
            has_apex = True
        except ImportError as e:
            logging.warning("APEX ImportError: {}.".format(e))
            from torch.nn.parallel import DistributedDataParallel as DDP
            pass

        logging.info(
            "NVIDIA APEX {}. AMP {}.".format(
            "installed" if has_apex else 'not installed',
            "on" if (has_apex and args.amp) else "off"))

        self.model_ema = None
        if args.model_ema:
            # Important to create EMA model after cuda(),
            # DP wrapper, and AMP but before SyncBN and DDP wrapper
            self.model_ema = ModelEma(
                self.model,
                decay=args.model_ema_decay,
                device='cpu' if args.model_ema_force_cpu else '',
                resume=args.resume)

        if args.wandb and sgd_utils.world_rank() == 0:
            import wandb
            wandb.watch(self.model)

        if args.sync_bn:
            # todo:
            # self._models[0] is a hack
            # it forces us to track model state manually since we can't
            # inform the runner about our new model
            #
            # under the hood, self.model uses self._models[0]
            try:
                if has_apex:
                    self._models[0] = convert_syncbn_model(self.model)
                else:
                    self._models[0] = (
                        nn.SyncBatchNorm.convert_sync_batchnorm(self.model))
                logging.info(
                    "Converted model to use Synchronized BatchNorm. "
                    "WARNING: You may have issues if "
                    "using zero initialized BN layers "
                    "(enabled by default for ResNets) "
                    "while sync-bn enabled.")
            except Exception as e:
                logging.error(
                    "Failed to enable Synchronized BatchNorm. "
                    "Install Apex or Torch >= 1.1")

            if has_apex:
                self._models[0] = DDP(self.model, delay_allreduce=True)
            else:
                logging.info(
                    "Using torch DistributedDataParallel. "
                    "Install NVIDIA Apex for Apex DDP.")

                # can use device str in Torch >= 1.1
                self._models[0] = DDP(self.model, device_ids=[0])
            # NOTE: EMA model does not need to be wrapped by DDP

        real_start_epoch = 0
        if args.start_epoch is not None:
            real_start_epoch = args.start_epoch
        if self.scheduler is not None and real_start_epoch > 0:
            self.scheduler.step(real_start_epoch)

        # fixme: we must only run num_epochs - real_start_epoch epochs
        # logging.info("Scheduled epochs: {}".format(num_epochs))

    def train_batch(self, batch, batch_info):
        input, target = batch

        args = self.config["args"]

        if not args.prefetcher:
            # prefetcher already does this for us if enabled
            if self.use_gpu:
                input, target = input.cuda(), target.cuda()

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
            loss = self.train_criterion(output, target)

        with self.timers.record("grad"):
            self.optimizer.zero_grad()
            if self.use_fp16:
                with amp.scale_loss(loss, self.optimizer) as scaled_loss:
                    scaled_loss.backward()
            else:
                loss.backward()

        with self.timers.record("apply"):
            self.optimizer.step()

        if self.use_gpu:
            torch.cuda.synchronize()

        if self.model_ema is not None:
            self.model_ema.update(model)

        return {"train_loss": loss, NUM_SAMPLES: input.size(0)}

    def train_epoch(self, iterator, info):
        if self.pending_scheduler_val_steps != 0:
            raise ValueError(
                "Scheduler has not done enough validation (epoch) steps. "
                "This might be because you are calling train without "
                "calling validate, which is not supported by timm as "
                "it relies on validation metrics for LR scheduling.")

        loader = self.train_loader

        args = self.config["args"]

        # even though we already received the iterator, changing
        # the loader/sampler state is fine since the iterator checks that state
        # on every call to next()
        if args.prefetcher and args.mixup > 0 and loader.mixup_enabled:
            if args.mixup_off_epoch and epoch >= args.mixup_off_epoch:
                loader.mixup_enabled = False

        if self.use_tqdm:
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

        batch_time_m = AverageMeter()
        data_time_m = AverageMeter()
        aux_time_m = AverageMeter()
        losses_m = AverageMeter()

        self.model.train()

        end = time.time()
        last_idx = len(loader) - 1
        num_updates = info["epoch_idx"] * len(loader)
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

            aux_start = time.time()

            # Logging
            lrl = [
                param_group["lr"]
                for param_group in self.optimizer.param_groups
            ]
            lr = sum(lrl) / len(lrl)

            losses_m.update(
                metrics["train_loss"].item(), metrics[NUM_SAMPLES])

            total_samples = (metrics[NUM_SAMPLES] * args.ray_num_workers)

            self.wandb_log({
                # "Epoch #": info["epoch_idx"],
                # "Batch #": batch_idx,
                "Loss": losses_m.val,
                "Average Loss": losses_m.avg,
                # "Total Batches": len(loader),
                # "Batch %": 100. * batch_idx / last_idx,
                "Batch Time": batch_time_m.val,
                "Sample Rate": total_samples / batch_time_m.val,
                "Average Batch Time": batch_time_m.avg,
                "Average Sample Rate": total_samples / batch_time_m.avg,
                "Learning Rate": lr,
                "Data Time": data_time_m.val,
                "Average Data Time": data_time_m.avg,

                "Auxiliary Time": aux_time_m.val,
                "Average Auxiliary Time": aux_time_m.avg,
            })
            if last_batch or batch_idx % args.log_interval == 0:
                _progress_bar.write(
                    "Train: {} [{:>4d}/{} ({:>3.0f}%)]  "
                    "Loss: {loss.val:>9.6f} ({loss.avg:>6.4f})  "
                    "Time: {batch_time.val:.3f}s, {rate:>7.2f}/s  "
                    "({batch_time.avg:.3f}s, {rate_avg:>7.2f}/s)  "
                    "LR: {lr:.3e}  "
                    "Data: {data_time.val:.3f} "
                    "({data_time.avg:.3f}) "
                    "Auxiliary: {aux_time.val:.3f} "
                    "({aux_time.avg:.3f})".format(
                        info["epoch_idx"],
                        batch_idx, len(loader),
                        100. * batch_idx / last_idx,
                        loss=losses_m,
                        batch_time=batch_time_m,
                        rate=total_samples / batch_time_m.val,
                        rate_avg=total_samples / batch_time_m.avg,
                        lr=lr,
                        data_time=data_time_m,
                        aux_time=aux_time_m))

                # todo: calculate output_dir
                # if args.save_images and output_dir:
                #     torchvision.utils.save_image(
                #         input,
                #         os.path.join(
                #             output_dir,
                #             "train-batch-%d.jpg" % batch_idx),
                #         padding=0,
                #         normalize=True)

            if self.use_tqdm:
                _progress_bar.n = batch_idx + 1
                postfix = {}
                if "train_loss" in metrics:
                    postfix.update(loss=metrics["train_loss"].item())
                _progress_bar.set_postfix(postfix)

            # todo: we must do this globally
            # if saver is not None and args.recovery_interval and (
            #     last_batch or (batch_idx + 1) % args.recovery_interval == 0):
            #     saver.save_recovery(
            #         model, optimizer, args, epoch,
            #         model_ema=model_ema, use_amp=use_amp, batch_idx=batch_idx)

            # Finish up batch

            if self.scheduler is not None:
                self.scheduler.step_update(
                    num_updates=self.global_step, metric=losses_m.avg)

            end = time.time()
            aux_time_m.update(end - aux_start)
            self.global_step += 1
        _progress_bar.close()


        if hasattr(self.optimizer, 'sync_lookahead'):
            self.optimizer.sync_lookahead()

        if args.dist_bn in ('broadcast', 'reduce'):
            # todo: logging like this might be breaking with the
            # epoch-wise TQDM
            logging.info("Distributing BatchNorm running means and vars")
            distribute_bn(
                self.model,
                sgd_utils.world_size(),
                args.dist_bn == "reduce")

        self.pending_scheduler_val_steps += 1

        return OrderedDict([("loss", losses_m.avg)])

    def validate_batch(self, batch, batch_info):
        args = self.config["args"]
        input, target = batch

        if not args.prefetcher:
            # prefetcher already does this for us if enabled
            if self.use_gpu:
                input, target = input.cuda(), target.cuda()

        with self.timers.record("eval_fwd"):
            if batch_info["using_ema"]:
                output = self.model_ema(input)
            else:
                output = self.model(input)

            if isinstance(output, (tuple, list)):
                output = output[0]

        # augmentation reduction
        reduce_factor = args.tta
        if reduce_factor > 1:
            output = output.unfold(0, reduce_factor, reduce_factor).mean(dim=2)
            target = target[0:target.size(0):reduce_factor]

        loss = self.val_criterion(output, target)
        prec1, prec5 = accuracy(output, target, topk=(1, 5))

        reduced_loss = reduce_tensor(loss.data, sgd_utils.world_size())
        prec1 = reduce_tensor(prec1, sgd_utils.world_size())
        prec5 = reduce_tensor(prec5, sgd_utils.world_size())

        if self.use_gpu:
            torch.cuda.synchronize()

        return {
            "val_loss": reduced_loss,
            "val_accuracy": prec1,
            "val_accuracy5": prec5,
            NUM_SAMPLES: input.size(0)
        }

    def validate(self, val_iterator, info):
        args = self.config["args"]
        loader = self.validation_loader

        using_ema = info["use_ema"]
        could_use_ema = self.model_ema is not None
        if self.model_ema is not None and not args.model_ema_force_cpu:
            if args.dist_bn in ('broadcast', 'reduce'):
                distribute_bn(
                    self.model_ema,
                    sgd_utils.world_size(),
                    args.dist_bn == 'reduce')

        if using_ema and not could_use_ema:
            # can't use EMA anyway, so we don't do anything
            return {}

        if self.pending_scheduler_val_steps == 0:
            raise ValueError(
                "Scheduler has not done a train step after a "
                "validation step yet. "
                "This might be because you are calling train without "
                "calling validate, which is not supported by timm as "
                "it relies on validation metrics for LR scheduling.")
        if self.pending_scheduler_val_steps > 1:
            raise ValueError(
                "Scheduler has done too many train steps. "
                "This might be because you are calling train without "
                "calling validate, which is not supported by timm as "
                "it relies on validation metrics for LR scheduling.")

        batch_time_m = AverageMeter()
        losses_m = AverageMeter()
        prec1_m = AverageMeter()
        prec5_m = AverageMeter()

        if using_ema:
            self.model_ema.eval()
        else:
            self.model.eval()

        end = time.time()
        last_idx = len(loader) - 1
        with torch.no_grad():
            for batch_idx, batch in enumerate(val_iterator):
                last_batch = batch_idx == last_idx

                batch_info = dict(
                    batch_idx=batch_idx,
                    using_ema=using_ema)
                batch_info.update(info)
                metrics = self.validate_batch(batch, batch_info)

                losses_m.update(
                    metrics["val_loss"].item(),
                    metrics[NUM_SAMPLES])
                prec1_m.update(
                    metrics["val_accuracy"].item(),
                    metrics[NUM_SAMPLES])
                prec5_m.update(
                    metrics["val_accuracy5"].item(),
                    metrics[NUM_SAMPLES])

                batch_time_m.update(time.time() - end)
                end = time.time()

                if last_batch or batch_idx % args.log_interval == 0:
                    log_name = "Test"
                    if using_ema:
                        log_name += " (EMA)"

                    logging.info(
                        "{0}: [{1:>4d}/{2}]  "
                        "Time: {batch_time.val:.3f} ({batch_time.avg:.3f})  "
                        "Loss: {loss.val:>7.4f} ({loss.avg:>6.4f})  "
                        "Prec@1: {top1.val:>7.4f} ({top1.avg:>7.4f})  "
                        "Prec@5: {top5.val:>7.4f} ({top5.avg:>7.4f})".format(
                            log_name, batch_idx, last_idx,
                            batch_time=batch_time_m, loss=losses_m,
                            top1=prec1_m, top5=prec5_m))

        metrics = OrderedDict([
            ("loss", losses_m.avg),
            ("prec1", prec1_m.avg),
            ("prec5", prec5_m.avg)])

        wandb_log_data = {
            "Batch Time": batch_time_m.val,
            "Average Batch Time": batch_time_m.avg,
            "Loss": losses_m.val,
            "Average Loss": losses_m.avg,
            "Top-1 Precision": prec1_m.val,
            "Average Top-1 Precision": prec1_m.avg,
            "Top-5 Precision": prec1_m.val,
            "Average Top-5 Precision": prec1_m.avg,
        }
        if using_ema:
            wandb_log_data = {
                "Validation (EMA) "+k: wandb_log_data[k]
                for k in wandb_log_data
            }
        else:
            wandb_log_data = {
                "Validation "+k: wandb_log_data[k]
                for k in wandb_log_data
            }
        self.wandb_log(wandb_log_data)

        if using_ema or not could_use_ema:
            self.scheduler.step(
                info["epoch_idx"] + 1,
                metrics[args.eval_metric])
            self.pending_scheduler_val_steps -= 1

        return metrics

    # todo: handle saving + loading epoch_idx
    def state_dict(self):
        res = dict(
            model=self.model.state_dict())

        if self.model_ema is not None:
            res["model_ema"] = self.model_ema.state_dict()

        return res

    def load_state_dict(self, state_dict):
        self.model.load_state_dict(state_dict["model"])
        if self.model_ema is not None:
            if "model_ema" in state_dict:
                self.model_ema.load_state_dict(state_dict["model_ema"])
            else:
                raise ValueError(
                    "Found an EMA instance but no model_ema in state_dict.")
        elif "model_ema" in state_dict:
            raise ValueError(
                "Found model_ema in state_dict but no EMA instance.")

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

    logging.info(
        "Model %s created, param count: %d" %
        (args.model, sum([m.numel() for m in model.parameters()])))

    if args.split_bn:
        model = convert_splitbn_model(model, max(args.num_aug_splits, 2))

    if not args.no_gpu:
        model = model.cuda()

    return model


def data_creator(config):
    args = config["args"]

    torch.manual_seed(args.seed + sgd_utils.world_rank())

    train_dir = join(args.data, "train")
    val_dir = join(args.data, "val")

    if args.mock_data:
        util.mock_data(train_dir, val_dir)

    data_config = resolve_data_config(
        vars(args), verbose=True)

    if not os.path.exists(train_dir):
        logging.error(
            "Training folder does not exist at: {}".format(train_dir))
        # throwing makes more sense, but we have to preserve the exit code
        exit(1)
    if not os.path.exists(val_dir):
        val_dir = os.path.join(args.data, 'validation')
        if not os.path.isdir(val_dir):
            logging.error(
                "Validation folder does not exist at: {}".format(val_dir))
            # throwing makes more sense, but we have to preserve the exit code
            exit(1)

    logging.info("Loading datasets.")
    dataset_train = Dataset(train_dir)
    logging.info("Training dataset created.")
    dataset_eval = Dataset(val_dir)
    logging.info("Validation dataset created.")

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
        num_workers=8,
        # we add the samplers ourselves, since they need distributed to be
        # setup
        distributed=False,
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
    logging.info("Training dataloader created.")
    eval_loader = create_loader(
        dataset_eval,
        is_training=False,
        batch_size=args.validation_batch_size_multiplier * config[BATCH_SIZE],
        interpolation=data_config["interpolation"],
        crop_pct=data_config["crop_pct"],
        **common_params)
    logging.info("Validation dataloader created.")

    return train_loader, eval_loader


def optimizer_creator(model, config):
    args = config["args"]
    return create_optimizer(args, model)


def loss_creator(config):
    args = config["args"]

    if args.jsd:
        assert args.num_aug_splits > 1  # JSD only valid with aug splits set
        train_loss_fn = JsdCrossEntropy(
            num_splits=args.num_aug_splits,
            smoothing=args.smoothing)
        validate_loss_fn = nn.CrossEntropyLoss()
    elif args.mixup > 0.:
        # smoothing is handled with mixup label transform
        train_loss_fn = SoftTargetCrossEntropy()
        validate_loss_fn = nn.CrossEntropyLoss()
    elif args.smoothing:
        train_loss_fn = LabelSmoothingCrossEntropy(smoothing=args.smoothing)
        validate_loss_fn = nn.CrossEntropyLoss()
    else:
        train_loss_fn = nn.CrossEntropyLoss()
        validate_loss_fn = train_loss_fn

    return train_loss_fn, validate_loss_fn

def scheduler_creator(optimizer, config):
    args = config["args"]
    return create_scheduler(args, optimizer)

import platform
def main():
    setup_default_logging()

    args, args_text = parse_args()

    if args.wandb:
        import wandb
        from datetime import datetime
        wandb.init(
            project="ray-sgd-imagenet",
            name=str(datetime.now()))
        wandb.config.update(args)

    ray.init(address=args.ray_address, log_to_driver=False)
    # ray.init(address=args.ray_address)

    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=loss_creator,
        scheduler_creator=scheduler_creator,
        training_operator_cls=ImagenetOperator,
        use_tqdm=True,
        use_fp16=args.amp,
        wrap_ddp=False,
        add_dist_sampler=True,  # todo: handle this manually
        apex_args={"opt_level": "O1"},
        config={
            "args": args,
            BATCH_SIZE: args.batch_size
        },
        num_workers=args.ray_num_workers)

    if args.smoke_test:
        args.epochs = 1

    # best_metric = None
    # best_epoch = None
    # saver = None
    # output_dir = ''
    # if args.local_rank == 0:
    #     output_base = args.output if args.output else './output'
    #     exp_name = '-'.join([
    #         datetime.now().strftime("%Y%m%d-%H%M%S"),
    #         args.model,
    #         str(data_config['input_size'][-1])
    #     ])
    #     output_dir = get_outdir(output_base, 'train', exp_name)
    #     decreasing = True if eval_metric == 'loss' else False
    #     saver = CheckpointSaver(checkpoint_dir=output_dir, decreasing=decreasing)
    #     with open(os.path.join(output_dir, 'args.yaml'), 'w') as f:
    #         f.write(args_text)

    for i in range(args.epochs):
    # pbar = trange(args.epochs, unit="epoch")
    # for i in pbar:
        info = dict(
            epoch_idx=i,
            num_epochs=args.epochs)

        trainer.train(
            num_steps=2 if args.smoke_test else None,
            reduce_results=False,
            info=info)
        # print pure model validation logs
        trainer.validate(
            num_steps=2 if args.smoke_test else None,
            info={
                "use_ema": False,
                **info
            })
        # this is kinda hacky
        trainer.validate(
            num_steps=2 if args.smoke_test else None,
            info={
                "use_ema": True,
                **info
            })

        # update_summary(
        #     epoch, train_metrics, eval_metrics, os.path.join(output_dir, 'summary.csv'),
        #     write_header=best_metric is None)

        # if saver is not None:
        #     # save proper checkpoint with eval metric
        #     save_metric = eval_metrics[eval_metric]
        #     best_metric, best_epoch = saver.save_checkpoint(
        #         model, optimizer, args,
        #         epoch=epoch, model_ema=model_ema, metric=save_metric, use_amp=use_amp)

        # pbar.set_postfix(dict(acc=eval_metrics["prec1"]))

    # if best_metric is not None:
    #         logging.info('*** Best metric: {0} (epoch {1})'.format(best_metric, best_epoch))

    trainer.save('./finished_checkpoint')

    if platform.system() != "Darwin":
        # this hangs on Mac OS :shrug:
        trainer.shutdown()


if __name__ == "__main__":
    main()
