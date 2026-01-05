# Train a GPT-2 model with Ray Train `JaxTrainer`

**Time to complete**: 15 min

This template shows you how to distribute a JAX/Flax training loop with [Ray Train](https://docs.ray.io/en/latest/train/train.html) through the `JaxTrainer`. You’ll train a small GPT-2-style Transformer from scratch on the [OpenWebText](https://openwebtext2.readthedocs.io/en/latest/) dataset.

Ray Train lets you keep your training code in a normal Python function, then runs that function on a set of Ray workers. `JaxTrainer` handles the orchestration (starting workers, setting up the distributed context, and collecting metrics/checkpoints) so you can focus on the model and the input pipeline.

This tutorial is inspired by Andrey Karpathy’s [nanoGPT](https://github.com/karpathy/nanoGPT/tree/master) and Google’s [Train a GPT-2 model with JAX on TPU for free](https://developers.googleblog.com/en/train-gpt2-model-with-jax-on-tpu/).

In this tutorial, you:

1. Start with a basic JAX/Flax GPT-2-style model and training step.
2. Wrap the loop in a `train_loop_per_worker` function and scale it out using Ray Train `JaxTrainer` with GPUs or TPUs!
3. Stream data and report metrics (and optionally checkpoints) through Ray Train.



## Step 1: Install dependencies and prepare the dataset


First, install the required Python packages.

If you’re running on GPUs, install `jax[cuda]`. If you’re running on Google TPUs, install `jax[tpu]`. For platform-specific requirements, see the [JAX installation guide](https://docs.jax.dev/en/latest/installation.html).

This notebook uses `jax[cuda]` in the examples for simplicity.



```python
# Run below if you plan to use GPUs.
%%bash
pip install pandas, numpy, jax[cuda], flax, tiktoken, datasets, transformers, orbax, optax


# Run below if you plan to use Google TPUs.
# %%bash
# pip install pandas, numpy, jax[tpu], flax, tiktoken, datasets, transformers, orbax, optax
```

Next, prepare the data that you’ll feed into the training loop.

This notebook uses [OpenWebText](https://openwebtext2.readthedocs.io/en/latest/), an open reproduction of OpenAI’s (private) WebText. The goal of this step is to tokenize the dataset once and write it to disk as two files:

- `train.bin`: training tokens.
- `val.bin`: validation tokens.

You can use Karpathy’s nanoGPT prep script ([`prepare.py`](https://github.com/karpathy/nanoGPT/blob/master/data/openwebtext/prepare.py)) or download preprocessed data from Kaggle ([OpenWebText GPT-2](https://www.kaggle.com/datasets/windmaple/openwebtext-gpt2)).

For simplicity, the following code adapts the nanoGPT approach and writes the output to the shared storage path used in an Anyscale workspace (`/mnt/cluster_storage`). If you already have `train.bin` and `val.bin`, you can skip this step.



```python
import os
from tqdm import tqdm
import numpy as np
import tiktoken
from datasets import load_dataset # huggingface datasets

# number of workers in .map() call
# good number to use is ~order number of cpu cores // 2
num_proc = 8
storage_path = "/mnt/cluster_storage/openwebtext"

# number of workers in load_dataset() call
# best number might be different from num_proc above as it also depends on NW speed.
# it is better than 1 usually though
num_proc_load_dataset = num_proc

enc = tiktoken.get_encoding("gpt2")

dataset = load_dataset("openwebtext", num_proc=num_proc_load_dataset)

# owt by default only contains the 'train' split, so create a test split
split_dataset = dataset["train"].train_test_split(test_size=0.0005, seed=2357, shuffle=True)
split_dataset['val'] = split_dataset.pop('test') # rename the test split to val
# we now want to tokenize the dataset. first define the encoding function (gpt2 bpe)
def process(example):
    ids = enc.encode_ordinary(example['text']) # encode_ordinary ignores any special tokens
    ids.append(enc.eot_token) # add the end of text token, e.g. 50256 for gpt2 bpe
    # note: I think eot should be prepended not appended... hmm. it's called "eot" though...
    out = {'ids': ids, 'len': len(ids)}
    return out

# tokenize the dataset
tokenized = split_dataset.map(
    process,
    remove_columns=['text'],
    desc="tokenizing the splits",
    num_proc=num_proc,
)

# concatenate all the ids in each dataset into one large file we can use for training
for split, dset in tokenized.items():
    arr_len = np.sum(dset['len'], dtype=np.uint64)
    filename = os.path.join(storage_path, f'{split}.bin')
    dtype = np.uint16 # (can do since enc.max_token_value == 50256 is < 2**16)
    arr = np.memmap(filename, dtype=dtype, mode='w+', shape=(arr_len,))
    total_batches = 1024

    idx = 0
    for batch_idx in tqdm[int](range(total_batches), desc=f'writing {filename}'):
        # Batch together samples for faster write
        batch = dset.shard(num_shards=total_batches, index=batch_idx, contiguous=True).with_format('numpy')
        arr_batch = np.concatenate(batch['ids'])
        # Write into mmap
        arr[idx : idx + len(arr_batch)] = arr_batch
        idx += len(arr_batch)
    arr.flush()

# train.bin is ~18GB, val.bin ~8.8MB
# train has ~9B tokens 
# val has ~4M tokens
```

After running the script, you should have two files in shared storage:

1. Training dataset: `/mnt/cluster_storage/openwebtext/train.bin`
2. Validation dataset: `/mnt/cluster_storage/openwebtext/val.bin`

You’ll load these files with Ray Data later, then stream batches to the training workers.


## Step 2: Define a JAX/Flax GPT-2-style model


Now define the model and the core training step with JAX and Flax.

The JAX ecosystem is modular: JAX provides the array programming and compilation primitives, and libraries such as [Flax](https://github.com/google/flax) (neural network building blocks), [Optax](https://github.com/google-deepmind/optax) (optimizers and losses), and [Orbax](https://github.com/google/orbax) (checkpointing) provide higher-level components. You’ll use all three in this tutorial.

In this section, nothing is Ray-specific yet—you’re building a normal single-process JAX/Flax training step that you’ll scale out with `JaxTrainer` in the next section.



```python
import os
import time
import numpy as np
from dataclasses import dataclass

import jax
import jax.numpy as jnp
from jax.experimental import mesh_utils
from jax.sharding import Mesh, PartitionSpec as P, NamedSharding

import flax.nnx as nnx
import optax
import orbax.checkpoint as orbax

import tiktoken

import ray
from ray import train
import ray.data
import ray.train
from ray.train import Checkpoint

```

First, define a small training configuration object. `JaxTrainer` will pass values from `train_loop_config` into your per-worker function, so keeping config in one place makes it easy to scale and to tune.



```python
@dataclass(frozen=True)
class TrainingConfig:
    # Model config (GPT-2 base model configuration).
    tokenizer = tiktoken.get_encoding("gpt2")  # We use gpt2 tokenizer for this tutorial.
    vocab_size = tokenizer.n_vocab

    num_transformer_blocks: int = 12
    seqlen: int = 1024
    embed_dim: int = 768

    num_heads: int = 12
    dropout_rate: float = 0.1

    dtype = jnp.bfloat16 # change to jnp.float32 for older GPUs
    param_dtype = jnp.float32

    @property
    def feed_forward_dim(self) -> int:
        return 4 * self.embed_dim

    # Optimizer config.
    init_learning_rate: float = 5e-4
    weight_decay: float = 1e-1

    # Training loop config.
    global_batch_size: int = 32
    max_steps: int = 10_000
    log_every_n_steps: int = 10
    val_every_n_steps: int = 100
    checkpoint_every_n_steps: int = 100

    # Data/config paths.
    openwebtext_root: str = "/mnt/cluster_storage/openwebtext"
```

This tutorial provides a GPT-2-style Transformer model implemented with [Flax NNX](https://flax.readthedocs.io/en/v0.8.3/experimental/nnx/index.html). The model code is standard JAX/Flax—Ray Train doesn’t require any special model wrappers.



```python
# --- Model Definitions (Safe to keep global) ---
# Keep dtype settings in one place so the model code doesn't rely on undefined globals.
dtype = TrainingConfig.dtype
param_dtype = TrainingConfig.param_dtype


def causal_attention_mask(seq_len):
    return jnp.tril(jnp.ones((seq_len, seq_len)))

class TransformerBlock(nnx.Module):
    def __init__(self, embed_dim: int, num_heads: int, ff_dim: int, dropout_rate: float, rngs: nnx.Rngs):
        self.layer_norm1 = nnx.LayerNorm(epsilon=1e-6,
                                         num_features=embed_dim,
                                         scale_init=nnx.with_partitioning(nnx.initializers.ones_init(), ('model',)),
                                         bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ('model',)),
                                         dtype=dtype,
                                         param_dtype=param_dtype,
                                         rngs=rngs)
        self.mha = nnx.MultiHeadAttention(num_heads=num_heads,
                                          in_features=embed_dim,
                                          kernel_init=nnx.with_partitioning(nnx.initializers.xavier_uniform(), (None, 'model')),
                                          bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ('model',)),
                                          dtype=dtype,
                                          param_dtype=param_dtype,
                                          rngs=rngs)
        self.dropout1 = nnx.Dropout(rate=dropout_rate)
        self.layer_norm2 = nnx.LayerNorm(epsilon=1e-6,
                                         num_features=embed_dim,
                                         scale_init=nnx.with_partitioning(nnx.initializers.ones_init(), ('model',)),
                                         bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ('model',)),
                                         dtype=dtype,
                                         param_dtype=param_dtype,
                                         rngs=rngs)
        self.linear1 = nnx.Linear(in_features=embed_dim,
                                  out_features=ff_dim,
                                  kernel_init=nnx.with_partitioning(nnx.initializers.xavier_uniform(), (None, 'model')),
                                  bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ('model',)),
                                  dtype=dtype,
                                  param_dtype=param_dtype,
                                  rngs=rngs)
        self.linear2 = nnx.Linear(in_features=ff_dim,
                                  out_features=embed_dim,
                                  kernel_init=nnx.with_partitioning(nnx.initializers.xavier_uniform(), (None, 'model')),
                                  bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ('model',)),
                                  dtype=dtype,
                                  param_dtype=param_dtype,
                                  rngs=rngs)
        self.dropout2 = nnx.Dropout(rate=dropout_rate)

    def __call__(self, inputs, training: bool = False):
        input_shape = inputs.shape
        bs, seq_len, emb_sz = input_shape
        attention_output = self.mha(
            inputs_q=self.layer_norm1(inputs),
            mask=causal_attention_mask(seq_len),
            decode=False,
        )
        x = inputs + self.dropout1(attention_output, deterministic=not training)
        mlp_output = self.linear1(self.layer_norm2(x))
        mlp_output = nnx.gelu(mlp_output)
        mlp_output = self.linear2(mlp_output)
        mlp_output = self.dropout2(mlp_output, deterministic=not training)
        return x + mlp_output

class TokenAndPositionEmbedding(nnx.Module):
    def __init__(self, seqlen: int, vocab_size: int, embed_dim: int, rngs: nnx.Rngs):
        self.token_emb = nnx.Embed(num_embeddings=vocab_size, features=embed_dim, dtype=dtype, param_dtype=param_dtype, rngs=rngs)
        self.pos_emb = nnx.Embed(num_embeddings=seqlen, features=embed_dim, dtype=dtype, param_dtype=param_dtype, rngs=rngs)

    def __call__(self, x):
        positions = jnp.arange(0, x.shape[1])[None, :]
        position_embedding = self.pos_emb(positions)
        token_embedding = self.token_emb(x)
        return self.token_emb, token_embedding+position_embedding

class GPT2(nnx.Module):
    def __init__(
        self,
        seqlen: int,
        vocab_size: int,
        embed_dim: int,
        num_heads: int,
        dropout_rate: float,
        feed_forward_dim: int,
        num_transformer_blocks: int,
        rngs: nnx.Rngs,
    ):
        self.embedding_layer = TokenAndPositionEmbedding(seqlen, vocab_size, embed_dim, rngs=rngs)
        self.dropout = nnx.Dropout(rate=dropout_rate)
        self.transformer_blocks = nnx.List([
            TransformerBlock(embed_dim, num_heads, feed_forward_dim, dropout_rate, rngs=rngs)
            for _ in range(num_transformer_blocks)
        ])
        self.layer_norm = nnx.LayerNorm(
            epsilon=1e-6,
            num_features=embed_dim,
            scale_init=nnx.with_partitioning(nnx.initializers.ones_init(), ("model",)),
            bias_init=nnx.with_partitioning(nnx.initializers.zeros_init(), ("model",)),
            dtype=dtype,
            param_dtype=param_dtype,
            rngs=rngs,
        )

    def __call__(self, inputs, training: bool = False):
        token_embedding, x = self.embedding_layer(inputs)
        x = self.dropout(x, deterministic=not training)
        for transformer_block in self.transformer_blocks:
            x = transformer_block(x, training=training)
        x = self.layer_norm(x)
        outputs = token_embedding.attend(x)
        return outputs


def create_model(*, rngs: nnx.Rngs, config: TrainingConfig):
    return GPT2(
        seqlen=config.seqlen,
        vocab_size=config.vocab_size,
        embed_dim=config.embed_dim,
        num_heads=config.num_heads,
        dropout_rate=config.dropout_rate,
        feed_forward_dim=config.feed_forward_dim,
        num_transformer_blocks=config.num_transformer_blocks,
        rngs=rngs,
    )
```

Next, define the `loss_fn` and `train_step` functions.

`train_step()` computes the loss, takes gradients, and updates model parameters through the optimizer. The training loop will call this function repeatedly.

For performance, this notebook JIT-compiles both functions with `@nnx.jit`.



```python

@nnx.jit
def loss_fn(model, batch):
    logits = model(batch[0])
    loss = optax.softmax_cross_entropy_with_integer_labels(
        logits=logits, labels=batch[1]
    ).mean()
    return loss, logits

@nnx.jit
def train_step(model, optimizer, metrics, batch):
    grad_fn = nnx.value_and_grad(loss_fn, has_aux=True)
    (loss, logits), grads = grad_fn(model, batch)
    metrics.update(loss=loss, logits=logits, labels=batch[1])
    optimizer.update(model, grads)
    return loss

```

## Step 3: Scale training to multiple workers with Ray Train `JaxTrainer`

Next, wrap the JAX training logic in a `train_loop_per_worker` function and launch it with Ray Train `JaxTrainer`.

Each Ray Train worker runs the same Python function with a different world rank, and Ray sets device visibility per worker (for example, one GPU per worker). Inside this function, you can:

- Read the distributed context (`world_rank`, `world_size`).
- Get the per-worker dataset shard (`train.get_dataset_shard(...)`) to stream batches.
- Report metrics (and optionally checkpoints) back to the trainer with `ray.train.report(...)`.



```python
def train_loop_per_worker(config_dict: dict) -> None:

    config = TrainingConfig(**config_dict)

    world_rank = ray.train.get_context().get_world_rank()
    world_size = ray.train.get_context().get_world_size()
    print(f"Worker rank {world_rank}/{world_size} sees devices: {jax.devices()}")


    # Create a mesh per worker process. 
    device_mesh = mesh_utils.create_device_mesh((jax.process_count(), 1))
    mesh = Mesh(device_mesh, axis_names=("data", "model"))
    data_sharding = NamedSharding(mesh, P("data", None))
    jax.set_mesh(mesh)


    # Initialize the model locally.
    model = create_model(rngs=nnx.Rngs(0), config=config)


    # We use Ray data to load the training and validation datasets.
    train_it = ray.train.get_dataset_shard("train")
    val_it = ray.train.get_dataset_shard("val")
    if train_it is None or val_it is None:
        raise RuntimeError("No Ray Train datasets provided. Pass datasets={...} to JaxTrainer.")
    
    local_batch_size = config.global_batch_size // jax.process_count()
    global_input_shape = (config.global_batch_size, config.seqlen)
    
    train_batches = iter(train_it.iter_batches(
        batch_size=local_batch_size,
        batch_format="numpy",
        prefetch_batches=2,
        drop_last=True,
    ))
    val_batches = iter(val_it.iter_batches(
        batch_size=local_batch_size,
        batch_format="numpy",
        prefetch_batches=2,
        drop_last=True,
    ))

    def make_global_batch(local_x: np.ndarray, local_y: np.ndarray, global_shape: tuple):
        # jax.make_array_from_process_local_data automatically handles the transfer 
        # from host memory (numpy) to device memory.
        global_x = jax.make_array_from_process_local_data(data_sharding, local_x, global_shape)
        global_y = jax.make_array_from_process_local_data(data_sharding, local_y, global_shape)
        return global_x, global_y

    # Initialize the optimizer.
    schedule = optax.cosine_decay_schedule(
        init_value=config.init_learning_rate,
        decay_steps=config.max_steps,
    )
    optax_chain = optax.chain(optax.adamw(learning_rate=schedule, weight_decay=config.weight_decay))
    optimizer = nnx.Optimizer(model, optax_chain, wrt=nnx.Param)

    checkpointer = orbax.PyTreeCheckpointer()
    start_time = time.time()

    train_metrics = nnx.metrics.Average('loss')
    val_metrics = nnx.metrics.Average('val_loss')

    for step in range(config.max_steps):
        try:
            local_batch = next(train_batches)
        except StopIteration:
            train_batches = iter(train_it.iter_batches(
                batch_size=local_batch_size,
                batch_format="numpy",
                prefetch_batches=2,
                drop_last=True,
            ))
            local_batch = next(train_batches)
    
        global_x, global_y = make_global_batch(local_batch["x"], local_batch["y"], global_input_shape)

        loss = train_step(model, optimizer, train_metrics, (global_x, global_y))

        if (step + 1) % config.log_every_n_steps == 0:
            elapsed = time.time() - start_time
            # Report metrics through Ray Train.
            ray.train.report({"step": step + 1, "train_loss": float(loss), "elapsed_s": elapsed})
            start_time = time.time()
        
        if (step + 1) % config.val_every_n_steps == 0:
            try:
                local_validation_batch = next(val_batches)
            except StopIteration:
                val_batches = iter(val_it.iter_batches(
                    batch_size=local_batch_size,
                    batch_format="numpy",
                    prefetch_batches=2,
                    drop_last=True,
                ))
                local_validation_batch = next(val_batches)

            global_val_input, global_val_target = make_global_batch(
                local_validation_batch["x"], 
                local_validation_batch["y"], 
                global_input_shape
            )
            
            loss, logits = loss_fn(model, (global_val_input, global_val_target))
            val_metrics.update(val_loss=loss, logits=logits)
            val_loss = float(val_metrics.compute())
            metrics = {"step": step + 1, "val_loss": float(val_loss)}
            

            if (step + 1) % config.checkpoint_every_n_steps == 0:
                
                # Orbax checkpointing is a barrier.
                train_state = nnx.to_pure_dict(nnx.state(model))
                checkpoint_path = os.path.join("/mnt/cluster_storage/checkpoint/jax_gpt2_ray_data", str(step + 1))
                checkpointer.save(checkpoint_path, train_state)
                
                # Save a checkpoint and report validation metrics through Ray Train.
                # The controller persists the checkpoint to the RunConfig storage path.
                checkpoint = Checkpoint.from_directory(checkpoint_path) 
                if world_rank == 0:
                    train.report(metrics, checkpoint=checkpoint)
                else:
                    train.report(metrics, checkpoint=None)

```

## Step 4: Launch distributed training with `JaxTrainer` and Ray Data

To run `train_loop_per_worker` on a Ray cluster, you construct a `JaxTrainer` with:

- `train_loop_per_worker`: the training function you defined earlier. Each Ray Train worker runs this function.
- `train_loop_config`: a hyperparameter dictionary passed into the function.
- `scaling_config`: the number of workers and compute resources (GPUs or TPUs) for the training run.
- `datasets`: Ray Data datasets that Ray Train will shard and stream to the workers.
- `run_config`: runtime configuration including where to write outputs such as checkpoints.

The following cells create Ray Data datasets from the `.bin` token files and then launch the trainer.



```python
def make_bin_xy_dataset(
    bin_path: str,
    seqlen: int,
    *,
    # How many sequences to generate per epoch-like pass.
    # You can make this very large and then .repeat() downstream.
    num_sequences: int,
    seed: int = 0,
    dtype=np.uint16,
    concurrency: int = 64,
):
    """
    Build a Ray Dataset of (x,y) sequences sampled randomly from a .bin token file.

    Produces rows:
      - x: int32[seqlen]
      - y: int32[seqlen]
    """
    if not os.path.exists(bin_path):
        raise FileNotFoundError(bin_path)

    # Open memmap on driver just to get length.
    data = np.memmap(bin_path, dtype=dtype, mode="r")
    n = int(len(data))
    if n <= seqlen + 1:
        raise ValueError(f"{bin_path} too small: len={n}, seqlen={seqlen}")

    rng = np.random.default_rng(seed)
    # Each start index uses [i : i+seqlen+1]
    starts = rng.integers(0, n - (seqlen + 1), size=num_sequences, dtype=np.int64)

    # Create a dataset of start indices
    ds = ray.data.from_items([{"i": int(i)} for i in starts])

    def read_xy(batch):
        # Open memmap inside worker process
        mm = np.memmap(bin_path, dtype=dtype, mode="r")
        idx = batch["i"].astype(np.int64, copy=False)

        # Allocate fixed arrays
        bs = idx.shape[0]
        x = np.empty((bs, seqlen), dtype=np.int32)
        y = np.empty((bs, seqlen), dtype=np.int32)

        # Slice per row (still Python loop, but runs in parallel across Ray workers)
        for j, start in enumerate(idx):
            window = mm[start : start + seqlen + 1].astype(np.int32, copy=False)
            x[j] = window[:-1]
            y[j] = window[1:]

        return {"x": x, "y": y}

    # Batch reading for efficiency
    ds = ds.map_batches(
        read_xy,
        batch_format="numpy",
        batch_size=32,
        compute=ray.data.TaskPoolStrategy(size=concurrency),
        zero_copy_batch=True,
    )

    return ds

train_ds = make_bin_xy_dataset(
        "/mnt/cluster_storage/openwebtext/train.bin",
        seqlen=1024,
        num_sequences=5_000_000,
        seed=2357,
    )
val_ds = make_bin_xy_dataset(
    "/mnt/cluster_storage/openwebtext/val.bin",
    seqlen=1024,
    num_sequences=5_000_000,
    seed=2357,
)
```


```python
from ray.train import RunConfig, ScalingConfig
from ray.train.v2.jax import JaxTrainer

storage_path = "/mnt/cluster_storage"

trainer = JaxTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "global_batch_size": 32,
    },
    scaling_config=ScalingConfig(
        use_gpu=True,
        num_workers=2,  # Change this to match on your GPU cluster setting.
        resources_per_worker={"GPU": 1},
    ),
    run_config=RunConfig(
        name="jax_gpt2",
        storage_path=storage_path,
        worker_runtime_env={"env_vars": {"LD_LIBRARY_PATH": ""}},
    ),
    datasets={"train": train_ds, "val": val_ds},
)

result = trainer.fit()
print(result)


```

### Run on TPUs

`JaxTrainer` can also run on TPUs.

To switch from GPUs to TPUs, update the `ScalingConfig` to request TPU resources and set the TPU topology/accelerator type. The following example shows a TPU V6E configuration.

For more information about TPU clusters with Ray on Kubernetes, see the [KubeRay TPU guide](https://docs.ray.io/en/master/cluster/kubernetes/user-guides/tpu.html#kuberay-tpu).



```python
from ray.train import RunConfig, ScalingConfig
from ray.train.v2.jax import JaxTrainer

storage_path = "/mnt/cluster_storage"

trainer = JaxTrainer(
    train_loop_per_worker=train_loop_per_worker,
    train_loop_config={
        "global_batch_size": 32,
    },
    scaling_config=ScalingConfig(
            use_tpu=True,
            num_workers=4,
            topology="2x2x4",
            accelerator_type="TPU-V6E",
            resources_per_worker={"TPU": 4},
            placement_strategy="SPREAD",
    ),
    run_config=RunConfig(
        name="jax_gpt2_tpu",
        storage_path=storage_path,
        worker_runtime_env={"env_vars": {"LD_LIBRARY_PATH": ""}},
    ),
    datasets={"train": train_ds, "val": val_ds},
)

result = trainer.fit()
print(result)

```

## Summary

In this notebook, you:

- Built a GPT-2-style model with JAX and Flax NNX.
- Wrapped the training step in a `train_loop_per_worker` and scaled it out using Ray Train `JaxTrainer` with GPUs or TPUs.
- Streamed data to distributed workers and reported metrics (and optionally checkpoints) through Ray Train.

