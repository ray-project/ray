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

# todo: none of the custom checkpointing actually works. at all
class ImagenetOperator(TrainingOperator):
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
            from apex import convert_syncbn_model
            has_apex = True
        except ImportError:
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

        if args.sync_bn:
            try:
                if has_apex:
                    self.mymodel = convert_syncbn_model(self.model)
                else:
                    self.mymodel = (
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
                self.mymodel = DDP(self.model, delay_allreduce=True)
            else:
                logging.info(
                    "Using torch DistributedDataParallel. "
                    "Install NVIDIA Apex for Apex DDP.")

                # can use device str in Torch >= 1.1
                self.mymodel = DDP(self.model, device_ids=[0])
            # NOTE: EMA model does not need to be wrapped by DDP

        real_start_epoch = 0
        if args.start_epoch is not None:
            real_start_epoch = args.start_epoch
            self.scheduler.step(real_start_epoch)

        logging.info("Scheduled epochs: {}".format(real_start_epoch))

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

            if last_batch or batch_idx % args.log_interval == 0:
                lrl = [
                    param_group["lr"]
                    for param_group in self.optimizer.param_groups
                ]
                lr = sum(lrl) / len(lrl)

                reduced_loss = reduce_tensor(
                    metrics["train_loss"].data, sgd_utils.world_size())
                losses_m.update(reduced_loss.item(), metrics[NUM_SAMPLES])

                total_samples = (
                    metrics[NUM_SAMPLES] * args.ray_num_workers)

                _progress_bar.write(
                    "Train: {} [{:>4d}/{} ({:>3.0f}%)]  "
                    "Loss: {loss.val:>9.6f} ({loss.avg:>6.4f})  "
                    "Time: {batch_time.val:.3f}s, {rate:>7.2f}/s  "
                    "({batch_time.avg:.3f}s, {rate_avg:>7.2f}/s)  "
                    "LR: {lr:.3e}  "
                    "Data: {data_time.val:.3f} "
                    "({data_time.avg:.3f})".format(
                        info["epoch_idx"],
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

            if self.use_tqdm:
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

            if self.scheduler is not None:
                self.scheduler.step_update(
                    num_updates=self.global_step, metric=losses_m.avg)

            end = time.time()
            self.global_step += 1
        _progress_bar.close()


        if hasattr(self.optimizer, 'sync_lookahead'):
            self.optimizer.sync_lookahead()

        if args.dist_bn in ('broadcast', 'reduce'):
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
            "val_accurcay5": prec5,
            NUM_SAMPLES: input.size(0)
        }

    def validate(self, val_iterator, info):
        if self.pending_scheduler_val_steps != 1:
            raise ValueError(
                "Scheduler has done too many train steps. "
                "This might be because you are calling train without "
                "calling validate, which is not supported by timm as "
                "it relies on validation metrics for LR scheduling.")

        log_suffix = info.get("log_suffix", "")

        args = self.config["args"]
        loader = self.eval_loader

        batch_time_m = AverageMeter()
        losses_m = AverageMeter()
        prec1_m = AverageMeter()
        prec5_m = AverageMeter()

        using_ema = False
        # I am not sure if we HAVE to run a validation step on the original
        # model and THEN override it with the metrics from the EMA
        # or if we can just run one validation step with the EMA.
        #
        # Assuming the latter here.
        if self.model_ema is not None and not args.model_ema_force_cpu:
            if args.dist_bn in ('broadcast', 'reduce'):
                distribute_bn(
                    self.model_ema,
                    sgd_utils.world_size(),
                    args.dist_bn == 'reduce')

            using_ema = True

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
                    log_name = "Test" + log_suffix
                    logging.info(
                        "{0}: [{1:>4d}/{2}]  "
                        "Time: {batch_time.val:.3f} ({batch_time.avg:.3f})  "
                        "Loss: {loss.val:>7.4f} ({loss.avg:>6.4f})  "
                        "Prec@1: {top1.val:>7.4f} ({top1.avg:>7.4f})  "
                        "Prec@5: {top5.val:>7.4f} ({top5.avg:>7.4f})".format(
                            log_name, batch_idx, last_idx,
                            batch_time=batch_time_m, loss=losses_m,
                            top1=prec1_m, top5=prec5_m))

        if info["step_lr"]:
            self.scheduler.step(
                info["epoch_idx"] + 1,
                eval_metrics[args.eval_metric])
            self.pending_scheduler_val_steps -= 1

        return OrderedDict([
            ("loss", losses_m.avg),
            ("prec1", prec1_m.avg),
            ("prec5", prec5_m.avg)])

    def state_dict(self):

    def load_state_dict(self, state_dict):

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

    dataset_train = Dataset(train_dir)
    dataset_eval = Dataset(val_dir)

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

    ray.init(address=args.ray_address, log_to_driver=False)

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

    pbar = trange(args.epochs, unit="epoch")
    for i in pbar:
        info = dict(
            epoch_idx=i,
            num_epochs=args.epochs)

        trainer.train(
            num_steps=2 if args.smoke_test else None,
            reduce_results=False,
            info=info)
        eval_metrics = trainer.validate(
            num_steps=2 if args.smoke_test else None,
            info=dict(
                step_lr=True
            ).update(info))

        # update_summary(
        #     epoch, train_metrics, eval_metrics, os.path.join(output_dir, 'summary.csv'),
        #     write_header=best_metric is None)

        # if saver is not None:
        #     # save proper checkpoint with eval metric
        #     save_metric = eval_metrics[eval_metric]
        #     best_metric, best_epoch = saver.save_checkpoint(
        #         model, optimizer, args,
        #         epoch=epoch, model_ema=model_ema, metric=save_metric, use_amp=use_amp)

        pbar.set_postfix(dict(acc=eval_metrics["val_accuracy"]))

    # if best_metric is not None:
    #         logging.info('*** Best metric: {0} (epoch {1})'.format(best_metric, best_epoch))

    if platform.system() != "Darwin":
        # this hangs on Mac OS :shrug:
        trainer.shutdown()


if __name__ == "__main__":
    main()
