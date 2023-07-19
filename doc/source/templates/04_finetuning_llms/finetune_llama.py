import evaluate
import torch
import argparse
import json
import math
import os
import functools
import time 
import tree
import pandas as pd
from pathlib import Path
import torch

from accelerate import Accelerator, DeepSpeedPlugin
from accelerate.utils import DummyOptim, DummyScheduler, set_seed
from transformers import (
    AutoModelForCausalLM,
    get_linear_schedule_with_warmup,
)


import ray
from ray import air
from ray.air import session
from ray.train.torch import TorchTrainer
import ray.util.scheduling_strategies

EVAL_BATCH_SIZE = 32
MODEL = "tiiuae/falcon-7b"
# MODEL = "tiiuae/falcon-40b"
BLOCK_SIZE = 512
OVERLAP_LENGTH = 128
OPTIM_BETAS = (0.9, 0.999)
OPTIM_EPS = 1e-8
OPTIM_WEIGHT_DECAY = 0.

def collate_fn(batch, tokenizer, device):
    print('-'*80)
    print(type(batch["input"]))
    print('-'*80)
    out_batch = tokenizer(
        list(batch["input"]), 
        padding="longest", 
        max_length=BLOCK_SIZE, 
        truncation=True, 
        return_tensors="pt"    
    )
    out_batch["labels"] = out_batch["input_ids"].clone()
    
    out_batch = tree.map_structure(lambda x: x.to(device), out_batch)
    
    return out_batch


def get_tokenizer(model_name, special_tokens):
    
    from transformers.models.llama.tokenization_llama import LlamaTokenizer

    tokenizer = LlamaTokenizer.from_pretrained(model_name)
    tokenizer.pad_token = tokenizer.eos_token
    tokenizer.add_tokens(special_tokens, special_tokens=True)
    
    return tokenizer
    
    
def get_ds(path):
    # jsonl file
    with open(path, 'r') as json_file:
        items = [json.loads(x) for x in json_file]

    dataset = {"input": []}
    for item in items:
        assert set(item.keys()) == {"input"}
        dataset["input"].append(item["input"])
    
    df = pd.DataFrame.from_dict(dataset)
    return ray.data.from_pandas(df)

def evaluate(args, model, eval_dataloader, accelerator, eval_dataset):
    model.eval()
    losses = []
    for step, batch in enumerate(eval_dataloader):
        with torch.no_grad():
            outputs = model(**batch)

        loss = outputs.loss
        losses.append(accelerator.gather(loss.repeat(EVAL_BATCH_SIZE)))

    losses = torch.cat(losses)
    losses = losses[: len(eval_dataset)]
    try:
        eval_loss = torch.mean(losses)
        perplexity = math.exp(eval_loss)
    except OverflowError:
        perplexity = float("inf")
    return perplexity, eval_loss

def training_function(kwargs: dict):
    os.environ["RANK"] = str(session.get_world_rank())
    os.environ["WORLD_SIZE"] = str(session.get_world_size())
    os.environ["LOCAL_RANK"] = str(session.get_local_rank())
    os.environ["OMP_NUM_THREADS"] = str(
        session.get_trial_resources().bundles[-1].get("CPU", 1)
    )

    print("training_function called")

    config = kwargs["config"]
    args = argparse.Namespace(**kwargs["args"])
    special_tokens = kwargs.get("special_tokens", [])

    if not args.output_dir:
        raise ValueError("--output_dir must be specified")
    
    if not Path(args.output_dir).exists():
        Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    # Sample hyper-parameters for learning rate, batch size, seed and a few other HPs
    lr = config["lr"]
    num_epochs = int(config["num_epochs"])
    seed = int(config["seed"])
    batch_size = int(config["batch_size"])
    gradient_accumulation_steps = int(config["gradient_accumulation_steps"])

    deepspeed_config = {
        "fp16": {
            "enabled": False
        },
        "bf16": {
            "enabled": True
        },
        "scheduler": {
            "type": "WarmupLR",
            "params": {
                "warmup_min_lr": "auto",
                "warmup_max_lr": "auto",
                "warmup_num_steps": "auto"
            }
        },
        "optimizer": {
            "type": "AdamW",
            "params": {
            "lr": 1e-5,
            "betas": [0.99, 0.999],
            "eps": 1e-8,
            "weight_decay": 1e-6
            }
        },
        "zero_optimization": {
            "stage": 3,
            "offload_optimizer": {
                "device": "cpu",
                "pin_memory": False,
            },
            # "offload_param": {
            #     "device": "cpu",
            #     "pin_memory": True,
            # },
            "overlap_comm": True,
            "contiguous_gradients": True,
            "reduce_bucket_size": "auto",
            "stage3_prefetch_bucket_size": "auto",
            "stage3_param_persistence_threshold": "auto",
            "gather_16bit_weights_on_model_save": True,
            "round_robin_gradients": True,
        },
        "gradient_accumulation_steps": "auto",
        "gradient_clipping": "auto",
        "steps_per_print": 10,
        "train_batch_size": "auto",
        "train_micro_batch_size_per_gpu": batch_size,
        "wall_clock_breakdown": False,
    }
    ds_plugin = DeepSpeedPlugin(hf_ds_config=deepspeed_config)

    # Initialize accelerator
    accelerator = Accelerator(
        deepspeed_plugin=ds_plugin,
        gradient_accumulation_steps=gradient_accumulation_steps,
        mixed_precision=args.mx,
    )
    
    set_seed(seed)
    
    # train_ds is the local shard for this model
    train_ds = session.get_dataset_shard("train")
    valid_ds = session.get_dataset_shard("valid")
    
    train_ds_len = len(list(train_ds.iter_batches(batch_size=1)))
    valid_ds_len = len(list(valid_ds.iter_batches(batch_size=1)))
    
    tokenizer = get_tokenizer(model_name=args.model_name, special_tokens=special_tokens)
    collate_partial = functools.partial(collate_fn, tokenizer=tokenizer, device=accelerator.device)
        

    if accelerator.is_main_process:
        print("Saving tokenizer and config.")
        tokenizer.save_pretrained(args.output_dir)

    print("Loading model ...")
    s = time.time()
    model = AutoModelForCausalLM.from_pretrained(
        MODEL,
        trust_remote_code=True, 
        use_cache=False, 
        torch_dtype=torch.bfloat16
    )
    print(f"Done loading model in {time.time() - s} seconds.")
    model.resize_token_embeddings(len(tokenizer))    
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
        eps=OPTIM_EPS
    )

    # Instantiate scheduler
    # Creates Dummy Scheduler if `scheduler` was spcified in the config file else creates `args.lr_scheduler_type` Scheduler
    # get train and valid dataset lengths

    if (
        accelerator.state.deepspeed_plugin is None
        or "scheduler" not in accelerator.state.deepspeed_plugin.deepspeed_config
    ):
        print("linear scheduler")
        lr_scheduler = get_linear_schedule_with_warmup(
            optimizer=optimizer,
            num_warmup_steps=100,
            num_training_steps=(train_ds_len * num_epochs) // gradient_accumulation_steps,
        )
    else:
        print("dummy scheduler")
        lr_scheduler = DummyScheduler(
            optimizer, total_num_steps=(train_ds_len * num_epochs) // gradient_accumulation_steps, warmup_num_steps=100
        )

    # Prepare everything
    # There is no specific order to remember, we just need to unpack the objects in the same order we gave them to the
    # prepare method.
    s = time.time()
    model, optimizer, lr_scheduler = accelerator.prepare(model, optimizer, lr_scheduler)
    
    print(f"Prepare done in {time.time() - s} seconds.")

    # Now we train the model
    if accelerator.is_main_process:
        print("Starting training ...")
        print("number of batches on main process", train_ds_len // batch_size)

    avg_fwd_time, avg_bwd_time, avg_opt_step_time = 0, 0, 0
    for epoch in range(num_epochs):
        s_epoch = time.time()
        model.train()
        loss_sum = torch.tensor(0.0).to(accelerator.device)
        
        train_dataloader = train_ds.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_partial,
        )
    
        for step, batch in enumerate(train_dataloader):
            # We could avoid this line since we set the accelerator with `device_placement=True`.
            # batch = tree.map_structure(lambda x: x.to(accelerator.device), batch)
            with accelerator.accumulate(model):
                s_fwd = time.time()
                outputs = model(**batch)
                loss = outputs.loss
                loss_sum += loss
                e_fwd = time.time()
                avg_fwd_time += e_fwd - s_fwd
                s_bwd = time.time()
                accelerator.backward(loss)
                e_bwd = time.time()
                avg_bwd_time += e_bwd - s_bwd

                # if accelerator.sync_gradients:
                #     accelerator.clip_grad_norm_(model.parameters(), 1.0)

                s_opt_step = time.time()
                optimizer.step()
                lr_scheduler.step()
                optimizer.zero_grad()
                e_opt_step = time.time()
                avg_opt_step_time += e_opt_step - s_opt_step
                
            if accelerator.is_main_process:
                accelerator.print(f"[epoch {epoch} step {step}] loss: {loss.item()} steptime: {e_opt_step - s_fwd}")

        e_epoch = time.time()
        accelerator.print("Train time per epoch: ", e_epoch - s_epoch)

        eval_s_epoch = time.time()
        
        valid_dataloader = valid_ds.iter_torch_batches(
            batch_size=batch_size,
            collate_fn=collate_partial,
        )
        
        perplex, eloss = evaluate(args, model, valid_dataloader, accelerator, valid_ds)
        accelerator.print("Eval result loss", eloss)
        accelerator.print("Eval perplex", perplex)

        eval_e_epoch = time.time()
        accelerator.print("Eval time per epoch: ", eval_e_epoch - eval_s_epoch)
        accelerator.print("avg fwd time: ", avg_fwd_time / (step + 1))
        accelerator.print("avg bwd time: ", avg_bwd_time / (step + 1))
        accelerator.print("avg opt step time: ", avg_opt_step_time / (step + 1))

        session.report(
            {
                "epoch": epoch,
                "train_loss": loss_sum.item() / train_ds_len,
                "eval_loss": eloss.item() / train_ds_len,
                "perplexity": perplex / valid_ds_len,
                "number of iterations": step + 1,
                "Train time per epoch": e_epoch - s_epoch,
                "Eval time per epoch": eval_e_epoch - eval_s_epoch,
                "avg fwd time": avg_fwd_time / (step + 1),
                "avg bwd time": avg_bwd_time / (step + 1),
            },
        )
        
        if args.output_dir is not None:
            accelerator.print(f"Saving bin version of model in {args.output_dir}")
            accelerator.wait_for_everyone()
            with accelerator.main_process_first():
                if not os.path.exists(os.path.join(args.output_dir, str(epoch))):
                    os.makedirs(os.path.join(args.output_dir, str(epoch)))
            unwrapped_model = accelerator.unwrap_model(model)
            unwrapped_model.save_pretrained(
                os.path.join(args.output_dir, str(epoch)),
                is_main_process=accelerator.is_main_process,
                save_function=accelerator.save,
                safe_serialization=False,
                state_dict=accelerator.get_state_dict(model),
            )

def main():
    global MODEL
    global EVAL_BATCH_SIZE
    
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
    parser.add_argument("--bs", type=int, default=64, help="Batch size to use.")
    parser.add_argument("--grad_accum", type=int, default=1, help="Gradient accumulation steps.")
    parser.add_argument("--train_path", type=str, help="Path to training jsonl file")
    parser.add_argument("--test_path", type=str, help="Path to testing jsonl file")
    parser.add_argument("--special_token_path", type=str, help="Path to token json file")    
    parser.add_argument("--no-grad-ckpt", action="store_true", help="If passed, will not use gradient checkpointing.")
    parser.add_argument("--output_dir", type=str, help="Path to output directory.")
    parser.add_argument("--model_name", default="tiiuae/falcon-7b", type=str)
    parser.add_argument("--num-epochs", type=int, default=10, help="Number of epochs to train for.")
    parser.add_argument("--lr", type=float, default=5e-6, help="Learning rate to use.")
    args = parser.parse_args()
    config = {
        "lr": args.lr, 
        "num_epochs": args.num_epochs, 
        "seed": 42, 
        "batch_size": args.bs, 
        "gradient_accumulation_steps": args.grad_accum,
        "model_name": MODEL,
        "block_size": BLOCK_SIZE,
        "overlap": OVERLAP_LENGTH,
        "eval_batch_size": EVAL_BATCH_SIZE,
    }
    MODEL = args.model_name
    EVAL_BATCH_SIZE = args.bs
    os.environ["RAY_memory_monitor_refresh_ms"] = "0"
    os.environ["TUNE_RESULT_DIR"] = args.output_dir
    ray.init(
        runtime_env={
            "pip": [
                "einops",
                "torch>=2.0",
                "evaluate",
                "datasets==2.13.1",
                "accelerate>=0.20.2"
            ],
            "env_vars": {
                "RAY_memory_monitor_refresh_ms": "0",
                "HF_HOME": "/mnt/local_storage/.cache/huggingface",
            }
        }
    )
    
    from ray.train.torch import TorchConfig
    
    train_ds = get_ds(args.train_path)
    if args.test_path is not None:
        valid_ds = get_ds(args.test_path)
    else:
        valid_ds = None
    
    
    # json file
    with open(args.special_token_path, 'r') as json_file:
        special_tokens = json.load(json_file)["tokens"]
    
    trainer = TorchTrainer(
        training_function,
        train_loop_config={"config": config, "args": vars(args), "special_tokens": special_tokens},
        # accelerate_config={}, # set to None to grab the default config
        scaling_config=air.ScalingConfig(
            num_workers=32, 
            use_gpu=True,  
            resources_per_worker={"GPU": 1},
        ),
        datasets={
            "train": train_ds,
            "valid": valid_ds,
        }
    )

    results = trainer.fit()


if __name__ == "__main__":
    main() 
