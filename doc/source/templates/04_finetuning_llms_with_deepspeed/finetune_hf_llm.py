import argparse
from filelock import FileLock
import functools
import json
import math
import os
from pathlib import Path
import re
import tempfile
import time
import tree
from typing import Tuple

try:
    import deepspeed  # noqa: F401
except ImportError as e:
    raise RuntimeError(
        "Please install deepspeed with `pip install --user deepspeed`."
    ) from e

from accelerate import Accelerator, DeepSpeedPlugin
from accelerate.utils import DummyOptim, DummyScheduler, set_seed
import torch
import torch.nn as nn
import tqdm
from transformers import (
    AutoModelForCausalLM,
    AutoTokenizer,
    get_linear_schedule_with_warmup,
)

from peft import LoraConfig, get_peft_model
import ray
from ray import train
import ray.util.scheduling_strategies
from ray.train.torch import TorchTrainer
from ray.train import Checkpoint

from utils import (
    get_checkpoint_and_refs_dir,
    get_mirror_link,
    download_model,
    get_download_path,
)


OPTIM_BETAS = (0.9, 0.999)
OPTIM_EPS = 1e-8
NUM_WARMUP_STEPS = 10
OPTIM_WEIGHT_DECAY = 0.0
ATTENTION_LAYER_NAME = "self_attn"


def get_expected_lora_num_parameters(
    model, lora_config: LoraConfig, attn_layer_name: str = ATTENTION_LAYER_NAME
):
    """Calculate the expected number of parameters for lora finetuning."""
    sum_params = 0
    num_attention_layers = 0
    modules = model.named_modules()
    loraified_modules = 0
    # We calculate the number of parameters we need for lora finetuning by calculating
    # the sizes of the deecomposed weight matrices according to the paper.
    for full_name, target in modules:
        layer_name = full_name.split(".")[-1]

        if layer_name == attn_layer_name:
            # Detected another attention layer (for example, llama 2 70b should have 80
            # of these)
            num_attention_layers += 1
        elif layer_name in lora_config.modules_to_save:
            # Detect another non-lora module to save, which will also contribute to the
            # number of checkpointed parameters. This will result in one set of
            # trainable parameters "<layer>.original_module.weight" and another one with
            # "<layer>.modules_to_save.default.weight"
            # Therefore, each layer contributes 2 x the number of actual elements in
            # that layer.
            sum_params += 2 * target.weight.numel()
            print(
                "Found non-lora-layer to checkpoint: ",
                layer_name,
                " with num params ",
                target.weight.numel(),
            )
        else:
            for module_name in lora_config.target_modules:
                if layer_name == module_name:
                    loraified_modules += 1
                    if isinstance(target, nn.Linear):
                        # Target is attention weight
                        sum_params += (
                            target.in_features + target.out_features
                        ) * lora_config.r
                    elif isinstance(target, nn.Embedding):
                        # Target is linear weight
                        sum_params += (
                            target.embedding_dim + target.num_embeddings
                        ) * lora_config.r

    print(
        f"Detected {num_attention_layers} attention layers, containing"
        f" {loraified_modules} modules to modify according to LoRA's `target_modules`."
        f" This should yield {sum_params} trainable parameters."
    )

    return sum_params


def get_number_of_params(model: nn.Module):
    sum = 0
    for name, param in model.named_parameters():
        if param.requires_grad:
            sum += param.numel()
    return sum


def collate_fn(batch, tokenizer, block_size, device):
    out_batch = tokenizer(
        list(batch["input"]),
        padding="max_length",
        max_length=block_size,
        truncation=True,
        return_tensors="pt",
    )
    out_batch["labels"] = out_batch["input_ids"].clone()

    out_batch = tree.map_structure(lambda x: x.to(device), out_batch)

    return out_batch


def get_pretrained_path(model_id: str):
    mirror_uri = get_mirror_link(model_id)
    ckpt_path, _ = get_checkpoint_and_refs_dir(
        model_id=model_id, bucket_uri=mirror_uri, s3_sync_args=["--no-sign-request"]
    )
    return ckpt_path


def get_tokenizer(model_name, special_tokens):

    pretrained_path = get_pretrained_path(model_name)
    # Context for legacy=True: https://github.com/huggingface/transformers/issues/25176
    tokenizer = AutoTokenizer.from_pretrained(pretrained_path, legacy=True)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.add_tokens(special_tokens, special_tokens=True)

    return tokenizer


def evaluate(
    *, model, eval_ds, accelerator, bsize, ds_kwargs, as_test: bool = False
) -> Tuple[float, float]:
    model.eval()
    losses = []

    eval_dataloader = eval_ds.iter_torch_batches(batch_size=bsize, **ds_kwargs)
    eval_ds_len = len(list(eval_ds.iter_batches(batch_size=1)))
    for step, batch in tqdm.tqdm(
        enumerate(eval_dataloader), total=eval_ds_len // (bsize + 1)
    ):
        with torch.no_grad():
            outputs = model(**batch)

        loss = outputs.loss
        # The tensors are gathered by concatenating them on the first dimension, so we
        # add a new dimension to the scalar loss to get a tensor of shape (K,) for K
        # workers.
        losses.append(accelerator.gather(loss[None]))

        if as_test:
            break

    # We stack losses so that we have a tensor of shape (T, K) where T is the number of
    # steps and K is the number of workers.
    losses = torch.stack(losses)
    try:
        eval_loss = torch.mean(losses).item()
        perplexity = math.exp(eval_loss)
    except OverflowError:
        perplexity = float("inf")
    return perplexity, eval_loss


def _test_tokenizer(model_name):
    # This function tests that adding special tokens does not
    # result in un-expected tokenization
    # Context: https://github.com/huggingface/transformers/issues/25176
    tokenizer = get_tokenizer(model_name=model_name, special_tokens=["<REPR_END>"])
    testoutput = tokenizer("<REPR_END>inform")["input_ids"]
    expected = tokenizer("inform")["input_ids"]
    assert testoutput[-1] == expected[-1], (
        "The tokenizer is not working as expected with special tokens, "
        f"testoutput={testoutput}, expected={expected}"
    )


def checkpoint_model(
    checkpoint_folder, ckpt_id, model, epoch, last_global_step, **kwargs
):
    """Utility function for checkpointing model + optimizer dictionaries
    The main purpose for this is to be able to resume training from that instant again.
    """
    checkpoint_state_dict = {
        "epoch": epoch,
        "last_global_step": last_global_step,
    }
    # Add extra kwargs too
    checkpoint_state_dict.update(kwargs)

    # In here model will be a DeepspeedEngine object
    model.save_checkpoint(checkpoint_folder, ckpt_id, checkpoint_state_dict)
    status_msg = (
        f"checkpointing: checkpoint_folder={checkpoint_folder}, ckpt_id={ckpt_id}"
    )
    print(status_msg)


def training_function(kwargs: dict):
    print("training_function called")

    # Train has a bug somewhere that causes ACCELERATE_TORCH_DEVICE to not be set
    # properly on multi-gpu nodes
    cuda_visible_device = os.environ["CUDA_VISIBLE_DEVICES"].split(",")
    local_rank = int(os.environ["LOCAL_RANK"])
    device_id = cuda_visible_device[local_rank]
    os.environ["ACCELERATE_TORCH_DEVICE"] = f"cuda:{device_id}"

    config = kwargs["config"]
    args = argparse.Namespace(**kwargs["args"])
    special_tokens = kwargs.get("special_tokens", [])
    model_id = config["model_name"]

    # We need to download the model weights on this machine if they don't exit.
    # We need to acquire a lock to ensure that only one process downloads the model
    bucket_uri = get_mirror_link(model_id)
    download_path = get_download_path(model_id)
    base_path = Path(download_path).parent
    base_path.mkdir(parents=True, exist_ok=True)
    lock_file = str(base_path / f'{model_id.replace("/",  "--")}.lock')
    with FileLock(lock_file):
        download_model(
            model_id=model_id, bucket_uri=bucket_uri, s3_sync_args=["--no-sign-request"]
        )

    # Sample hyper-parameters for learning rate, batch size, seed and a few other HPs
    lr = config["lr"]
    num_epochs = int(config["num_epochs"])
    seed = int(config["seed"])
    batch_size = int(config["batch_size"])
    gradient_accumulation_steps = int(config["gradient_accumulation_steps"])

    # Get deepspeed config to setup the batch size per device
    ds_plugin = config["ds_plugin"]
    ds_plugin.hf_ds_config.config["train_micro_batch_size_per_gpu"] = batch_size

    # Initialize accelerator
    accelerator = Accelerator(
        deepspeed_plugin=ds_plugin,
        gradient_accumulation_steps=gradient_accumulation_steps,
        mixed_precision=args.mx,
    )

    set_seed(seed)

    # train_ds is the local shard for this model
    train_ds = train.get_dataset_shard("train")
    valid_ds = train.get_dataset_shard("valid")

    train_ds_len = len(list(train_ds.iter_batches(batch_size=1)))

    _test_tokenizer(args.model_name)
    tokenizer = get_tokenizer(model_name=args.model_name, special_tokens=special_tokens)
    collate_partial = functools.partial(
        collate_fn,
        tokenizer=tokenizer,
        block_size=config["block_size"],
        device=accelerator.device,
    )

    pretrained_path = get_pretrained_path(model_id)
    print(f"Loading model from {pretrained_path} ...")
    s = time.time()
    model = AutoModelForCausalLM.from_pretrained(
        pretrained_path,
        trust_remote_code=True,
        torch_dtype=torch.bfloat16,
        # `use_cache=True` is incompatible with gradient checkpointing.
        use_cache=False,
        use_flash_attention_2=True,
    )
    print(f"Done loading model in {time.time() - s} seconds.")

    model.resize_token_embeddings(len(tokenizer))

    if config["lora"]:
        # Apply LoRA
        s = time.time()
        lora_config = LoraConfig(**config["lora_config"])

        expected_num_parameters = get_expected_lora_num_parameters(
            lora_config=lora_config, model=model
        )

        print(f"Attempting to apply LoRA config: {lora_config}")

        model.enable_input_require_grads()
        model = get_peft_model(model, lora_config)

        num_parameters = get_number_of_params(model)

        if num_parameters != expected_num_parameters:
            raise ValueError(
                f"Expected {expected_num_parameters} parameters, got {num_parameters} "
                f"parameters. LoRA-ification failed."
            )

        print(
            f"LoRA-ification done in {time.time() - s} seconds. Estimated checkpoint "
            f"size (fp16): {num_parameters * 2 / 1e6} MB"
        )

    print(f"Number of checkpointed parameters: {get_number_of_params(model)}")

    print("Model initialized with pretrained weights. Training starting...")
    if not args.no_grad_ckpt:
        model.gradient_checkpointing_enable()

    optimizer_cls = (
        torch.optim.AdamW
        if accelerator.state.deepspeed_plugin is None
        or "optimizer" not in accelerator.state.deepspeed_plugin.deepspeed_config
        else DummyOptim
    )

    optimizer = optimizer_cls(
        model.parameters(),
        lr=lr,
        betas=OPTIM_BETAS,
        weight_decay=OPTIM_WEIGHT_DECAY,
        eps=OPTIM_EPS,
    )

    # Instantiate scheduler
    # Creates Dummy Scheduler if `scheduler` was specified in the config file or
    # else, creates `args.lr_scheduler_type` Scheduler
    # get train and valid dataset lengths

    num_steps_per_epoch = math.ceil(train_ds_len / args.batch_size_per_device)
    total_training_steps = (
        num_steps_per_epoch * num_epochs // gradient_accumulation_steps
    )

    if (
        accelerator.state.deepspeed_plugin is None
        or "scheduler" not in accelerator.state.deepspeed_plugin.deepspeed_config
    ):
        lr_scheduler = get_linear_schedule_with_warmup(
            optimizer=optimizer,
            num_warmup_steps=NUM_WARMUP_STEPS * args.num_devices,
            num_training_steps=total_training_steps * args.num_devices,
        )
    else:
        lr_scheduler = DummyScheduler(
            optimizer,
            warmup_num_steps=NUM_WARMUP_STEPS * args.num_devices,
            total_num_steps=total_training_steps * args.num_devices,
        )

    # Prepare everything
    # There is no specific order to remember, we just need to unpack the objects in the
    # same order we gave them to the prepare method.
    s = time.time()
    model, optimizer, lr_scheduler = accelerator.prepare(model, optimizer, lr_scheduler)
    print(f"Prepare done in {time.time() - s} seconds.")

    # Now we train the model
    if accelerator.is_main_process:
        print("Starting training ...")
        print("Number of batches on main process", train_ds_len // batch_size)

    for epoch in range(num_epochs):
        fwd_time_sum, bwd_time_sum, optim_step_time_sum = 0, 0, 0
        s_epoch = time.time()
        model.train()
        loss_sum = torch.tensor(0.0).to(accelerator.device)

        train_dataloader = train_ds.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_partial,
        )

        for step, batch in tqdm.tqdm(
            enumerate(train_dataloader), total=train_ds_len // batch_size + 1
        ):

            # We could avoid this line since we set the accelerator with
            # `device_placement=True`.
            with accelerator.accumulate(model):
                s_fwd = time.time()
                outputs = model(**batch)
                loss = outputs.loss
                loss_sum += loss.item()
                e_fwd = time.time()
                fwd_time = e_fwd - s_fwd
                fwd_time_sum += fwd_time
                s_bwd = time.time()
                accelerator.backward(loss)
                e_bwd = time.time()
                bwd_time = e_bwd - s_bwd
                bwd_time_sum += bwd_time

                s_opt_step = time.time()
                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()
                e_opt_step = time.time()
                optim_step_time_sum += e_opt_step - s_opt_step

            if accelerator.is_main_process:
                accelerator.print(
                    f"[epoch {epoch} step {step}] "
                    f"loss: {loss.item()} step-time: {e_opt_step - s_fwd}"
                )

            aggregated_loss = torch.mean(accelerator.gather(loss[None])).item()

            if config["as_test"]:
                break

            # as long as this is not the last step report here
            if step != (train_ds_len // batch_size - 1):
                train.report(
                    {
                        "epoch": epoch,
                        "iteration": step,
                        "train_loss_batch": aggregated_loss,
                        "avg_train_loss_epoch": None,
                        "eval_loss": None,
                        "perplexity": None,
                        "num_iterations": step + 1,
                        "train_time_per_epoch": None,
                        "eval_time_per_epoch": None,
                        "fwd_time": fwd_time,
                        "bwd_time": bwd_time,
                        "avg_fwd_time_per_epoch": None,
                        "avg_bwd_time_per_epoch": None,
                        "learning_rate": lr_scheduler.get_lr()[0],
                    }
                )

        e_epoch = time.time()
        accelerator.print("Train time per epoch: ", e_epoch - s_epoch)

        eval_s_epoch = time.time()
        print("Running evaluation ...")
        perplex, eloss = evaluate(
            model=model,
            eval_ds=valid_ds,
            accelerator=accelerator,
            bsize=config["eval_batch_size"],
            ds_kwargs={"collate_fn": collate_partial},
            as_test=config["as_test"],
        )
        accelerator.print("Eval result loss", eloss)
        accelerator.print("Eval perplex", perplex)

        eval_e_epoch = time.time()
        accelerator.print("Eval time per epoch: ", eval_e_epoch - eval_s_epoch)
        accelerator.print("avg fwd time: ", fwd_time_sum / (step + 1))
        accelerator.print("avg bwd time: ", bwd_time_sum / (step + 1))
        accelerator.print("avg opt step time: ", optim_step_time_sum / (step + 1))

        metrics = {
            "epoch": epoch,
            "iteration": step,
            "train_loss_batch": aggregated_loss,
            "avg_train_loss_epoch": loss_sum.item() / (step + 1),
            "eval_loss": eloss,
            "perplexity": perplex,
            "num_iterations": step + 1,
            "train_time_per_epoch": e_epoch - s_epoch,
            "eval_time_per_epoch": eval_e_epoch - eval_s_epoch,
            "fwd_time": fwd_time,
            "bwd_time": bwd_time,
            "avg_fwd_time_per_epoch": fwd_time_sum / (step + 1),
            "avg_bwd_time_per_epoch": bwd_time_sum / (step + 1),
            "learning_rate": lr_scheduler.get_lr()[0],
        }

        with tempfile.TemporaryDirectory(dir=args.output_dir) as temp_checkpoint_dir:
            accelerator.print(f"Saving the model locally at {temp_checkpoint_dir}")
            accelerator.wait_for_everyone()

            checkpoint_save_start = time.perf_counter()

            if accelerator.is_main_process:
                print("Saving tokenizer and config.")
                tokenizer.save_pretrained(temp_checkpoint_dir)

            accelerator.wait_for_everyone()

            # Checkpointing strategy 1: Distributed checkpointing
            # This checkpointing method makes deepspeed checkpoints on each node
            # and then Ray Train will aggregate them to a central s3 bucket.
            # It should be done on all processes (not just the Rank 0)
            # aggregate_on_rank_0 = False
            # checkpoint_model(
            #     checkpoint_folder=tempdir,
            #     ckpt_id=epoch,
            #     model=model,
            #     epoch=epoch,
            #     last_global_step=step
            # )

            # Checkpointing strategy 2: Aggregate model on the rank 0 worker then upload
            aggregate_on_rank_0 = True
            unwrapped_model = accelerator.unwrap_model(model)
            unwrapped_model.save_pretrained(
                temp_checkpoint_dir,
                is_main_process=accelerator.is_main_process,
                save_function=accelerator.save,
                safe_serialization=True,
                state_dict=accelerator.get_state_dict(model),
            )
            accelerator.wait_for_everyone()
            print("Checkpoint save time: ", time.perf_counter() - checkpoint_save_start)

            checkpoint_upload_start = time.perf_counter()

            # Create the checkpoint object to report to Ray Train and upload to storage.
            # If we aggregated the model on rank 0, we only need to report
            # the checkpoint from the rank 0 worker, since all other checkpoint
            # directories are empty (`save_pretrained` was a noop for other workers).
            if aggregate_on_rank_0:
                checkpoint = (
                    Checkpoint.from_directory(temp_checkpoint_dir)
                    if accelerator.is_main_process
                    else None
                )
            else:
                # Distributed checkpointing should upload shards from each worker.
                checkpoint = Checkpoint.from_directory(temp_checkpoint_dir)

            # Note: After `train.report`, in the case of remote storage,
            # the checkpoint directory will be uploaded to the remote storage.
            train.report(metrics, checkpoint=checkpoint)

            print(
                "Checkpoint upload time: ",
                time.perf_counter() - checkpoint_upload_start,
            )
            print(
                "Total checkpointing time: ",
                time.perf_counter() - checkpoint_save_start,
            )

        if perplex < args.stop_perplexity:
            print(f"Perplexity reached {perplex} < {args.stop_perplexity}. Stopping.")
            break

        if config["as_test"]:
            break


def parse_args():

    parser = argparse.ArgumentParser(description="Simple example of training script.")
    parser.add_argument(
        "--mx",
        type=str,
        default="bf16",
        choices=["no", "fp16", "bf16", "fp8"],
        help="Whether to use mixed precision. Choose"
        "between fp16 and bf16 (bfloat16). Bf16 requires PyTorch >= 1.10."
        "and an Nvidia Ampere GPU.",
    )

    parser.add_argument(
        "--batch-size-per-device",
        "-bs",
        type=int,
        default=16,
        help="Batch size to use per device.",
    )

    parser.add_argument(
        "--stop-perplexity",
        default=0,
        type=float,
        help="Target perplexity to reach after which to stop training. Default is 0. "
        "If 0, training will not stop on perplexity.",
    )

    parser.add_argument(
        "--eval-batch-size-per-device",
        type=int,
        default=64,
        help="Batch size to use per device (For evaluation).",
    )

    parser.add_argument(
        "--num-devices", "-nd", type=int, default=4, help="Number of devices to use."
    )
    parser.add_argument(
        "--grad_accum", type=int, default=1, help="Gradient accumulation steps."
    )
    parser.add_argument("--train_path", type=str, help="Path to training jsonl file")

    parser.add_argument("--test_path", type=str, help="Path to testing jsonl file")

    parser.add_argument(
        "--special_token_path", type=str, help="Path to token json file"
    )
    parser.add_argument(
        "--no-grad-ckpt",
        action="store_true",
        help="If passed, will not use gradient checkpointing.",
    )
    parser.add_argument("--output_dir", type=str, help="Path to output directory.")

    parser.add_argument(
        "--model_name", default="meta-llama/Llama-2-7b-chat-hf", type=str
    )
    parser.add_argument(
        "--num-epochs", type=int, default=1, help="Number of epochs to train for."
    )
    parser.add_argument(
        "--num-checkpoints-to-keep",
        type=int,
        help=(
            "Number of checkpoints to keep, if None, all checkpoints will be kept, "
            "if set to n>=1, the top n checkpoint with min. evaluation perplexity "
            "will be kept."
        ),
        default=None,
    )
    parser.add_argument("--lr", type=float, default=5e-6, help="Learning rate to use.")

    parser.add_argument(
        "--ctx-len",
        type=int,
        default=512,
        help="Maximum context length for the model input sequences.",
    )

    parser.add_argument(
        "--as-test",
        action="store_true",
        help="If passed, will run the script in test mode.",
    )

    parser.add_argument(
        "--ds-config",
        type=str,
        default="./deepspeed_configs/zero_3_llama_2_7b.json",
        help="Deepspeed config json to use.",
    )

    parser.add_argument(
        "--lora",
        action="store_true",
        default=False,
        help="If passed, will enable parameter efficient fine-tuning with LoRA ("
        "https://arxiv.org/pdf/2106.09685.pdf).",
    )

    args = parser.parse_args()

    return args


def main():

    args = parse_args()

    if not args.output_dir:
        raise ValueError("--output_dir must be specified")

    # update the config with args so that we have access to them.
    config = vars(args)
    config.update(
        **{
            "lr": args.lr,
            "num_epochs": args.num_epochs,
            "seed": 42,
            "batch_size": args.batch_size_per_device,
            "gradient_accumulation_steps": args.grad_accum,
            "model_name": args.model_name,
            "block_size": args.ctx_len,
            "eval_batch_size": args.eval_batch_size_per_device,
        }
    )

    # Add LoRA config if needed
    if args.lora:
        with open("./lora_configs/lora.json", "r") as json_file:
            lora_config = json.load(json_file)
        config["lora_config"] = lora_config

    # Add deepspeed plugin to the config
    ds_plugin = DeepSpeedPlugin(hf_ds_config=config.get("ds_config"))
    config.update(ds_plugin=ds_plugin)

    ray.init(
        runtime_env={
            "env_vars": {"HF_HOME": "/mnt/local_storage/.cache/huggingface"},
            "working_dir": ".",
        }
    )

    # Read data
    train_ds = ray.data.read_json(args.train_path)
    if args.test_path is not None:
        valid_ds = ray.data.read_json(args.test_path)
    else:
        valid_ds = None

    # json file
    with open(args.special_token_path, "r") as json_file:
        special_tokens = json.load(json_file)["tokens"]

    assert (
        "ANYSCALE_ARTIFACT_STORAGE" in os.environ
    ), "ANYSCALE_ARTIFACT_STORAGE env var must be set!"
    artifact_storage = os.environ["ANYSCALE_ARTIFACT_STORAGE"]
    user_name = re.sub(r"\s+", "__", os.environ.get("ANYSCALE_USERNAME", "user"))
    storage_path = (
        f"{artifact_storage}/{user_name}/ft_llms_with_deepspeed/{args.model_name}"
    )

    trial_name = f"{args.model_name}".split("/")[-1]
    if args.lora:
        trial_name += "-lora"

    trainer = TorchTrainer(
        training_function,
        train_loop_config={
            "config": config,
            "args": vars(args),
            "special_tokens": special_tokens,
        },
        run_config=train.RunConfig(
            storage_path=storage_path,
            checkpoint_config=train.CheckpointConfig(
                num_to_keep=args.num_checkpoints_to_keep,
                checkpoint_score_attribute="perplexity",
                checkpoint_score_order="min",
            ),
        ),
        scaling_config=train.ScalingConfig(
            num_workers=args.num_devices,
            use_gpu=True,
            resources_per_worker={"GPU": 1},
        ),
        datasets={"train": train_ds, "valid": valid_ds},
        dataset_config=ray.train.DataConfig(datasets_to_split=["train", "valid"]),
    )

    result: train.Result = trainer.fit()
    # `best_checkpoints` are sorted in increasing score order.
    # (Ex: in this case, negative perplexity, since we set `checkpoint_score_order=min`)
    best_checkpoint, best_checkpoint_metrics = result.best_checkpoints[-1]

    print("Results are stored at:")
    print(result.path)
    print("Best checkpoint is stored at:")
    print(best_checkpoint)
    print(f"With perplexity: {best_checkpoint_metrics['perplexity']}")


if __name__ == "__main__":
    main()
