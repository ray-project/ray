import ray
import torch
import pytorch_lightning as pl
import torch.nn.functional as F
from torch.utils.data import DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForCausalLM
from datasets import load_dataset, load_metric
import numpy as np
import pandas as pd
from ray.data.preprocessors import Chain
import evaluate

MODEL_NAME = "databricks/dolly-v2-3b"

current_dataset = load_dataset("tiny_shakespeare")

from ray.data.preprocessors import BatchMapper


def split_text(batch: pd.DataFrame) -> pd.DataFrame:
    text = list(batch["text"])
    flat_text = "".join(text)
    split_text = [
        x.strip()
        for x in flat_text.split("\n")
        if x.strip() and not x.strip()[-1] == ":"
    ]
    return pd.DataFrame(split_text, columns=["text"])


def tokenize(batch: pd.DataFrame) -> dict:
    tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME, padding_side="left")
    tokenizer.pad_token = tokenizer.eos_token
    ret = tokenizer(
        list(batch["text"]),
        truncation=True,
        max_length=256,
        padding="max_length",
        return_tensors="np",
    )
    ret["labels"] = ret["input_ids"].copy()
    return dict(ret)


splitter = BatchMapper(split_text, batch_format="pandas")
tokenizer = BatchMapper(tokenize, batch_format="pandas")
preprocessor = Chain(splitter, tokenizer)

ray_datasets = ray.data.from_huggingface(current_dataset)


class DollyV2Model(pl.LightningModule):
    def __init__(self, lr=2e-5, eps=1e-8):
        super().__init__()
        self.lr = lr
        self.eps = eps
        self.model = AutoModelForCausalLM.from_pretrained(MODEL_NAME, torch_dtype=torch.bfloat16)

        self.metric = evaluate.load("accuracy")
        self.predictions = []
        self.references = []

    def forward(self, batch):
        input_ids, attention_mask = batch["input_ids"], batch["attention_mask"]
        outputs = self.model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        return logits

    def training_step(self, batch, batch_idx):
        labels = batch["labels"]
        logits = self.forward(batch)
        vocab_size = logits.shape[-1]
        print("dimension", logits.shape, labels.shape)
        loss = F.cross_entropy(logits.view(-1, vocab_size), labels.view(-1))
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx):
        labels = batch["labels"]
        logits = self.forward(batch)
        print("dimension", logits.shape, labels.shape)
        preds = torch.argmax(logits, dim=-1)
        self.predictions.append(preds.view(-1))
        self.references.append(labels.view(-1))

    def on_validation_epoch_end(self):
        predictions = torch.concat(self.predictions).view(-1)
        references = torch.concat(self.references).view(-1)

        result = self.metric.compute(
            predictions=predictions, references=references
        )

        self.log_dict(result, sync_dist=True)
        self.predictions.clear()
        self.references.clear()

    def configure_optimizers(self):
        return torch.optim.AdamW(self.trainer.model.parameters(), lr=self.lr, eps=self.eps)


# ## 3. Configure your LightningTrainer
# 
# Define a LightningTrainer with necessary configurations, including hyper-parameters, checkpointing and compute resources settings. 
# 
# You may find the API of {class}`LightningConfigBuilder <ray.train.lightning.LightningConfigBuilder>` and the discussion {ref}`here <lightning-config-builder-intro>` useful.
# 
# 

# In[12]:


from ray.train.lightning import LightningTrainer, LightningConfigBuilder
from ray.air.config import RunConfig, ScalingConfig, CheckpointConfig
from pytorch_lightning.callbacks import TQDMProgressBar

from torch.distributed.fsdp.wrap import size_based_auto_wrap_policy

# Define the configs for LightningTrainer
lightning_config = (
    LightningConfigBuilder()
    .module(cls=DollyV2Model, lr=1e-5, eps=1e-8)
    .trainer(max_epochs=3, accelerator="gpu")
    .checkpointing(save_on_train_epoch_end=False, save_top_k = 0)
    .strategy(name="fsdp", auto_wrap_policy=size_based_auto_wrap_policy)
    .build()
)


# :::{note}
# Note that the `lightning_config` is created on the head node and will be passed to the worker nodes later. Be aware that the environment variables and hardware settings may differ between the head node and worker nodes.
# :::
# 
# :::{note}
# {meth}`LightningConfigBuilder.checkpointing() <ray.train.lightning.LightningConfigBuilder.checkpointing>` creates a [ModelCheckpoint]{<https://lightning.ai/docs/pytorch/stable/api/lightning.pytorch.callbacks.ModelCheckpoint.html#lightning.pytorch.callbacks.ModelCheckpoint>} callback. This callback defines the checkpoint frequency and saves checkpoint files in Lightning style. If you want to save AIR checkpoints for Batch Prediction, please also provide an AIR CheckpointConfig.
# :::

# In[13]:


# Save AIR checkpoints according to the performance on validation set
run_config = RunConfig(
    name="ptl-sent-classification",
    checkpoint_config=CheckpointConfig(
        num_to_keep=2,
        checkpoint_score_attribute="matthews_correlation",
        checkpoint_score_order="max",
    ),
)

# Scale the DDP training workload across 4 GPUs
# You can change this config based on your compute resources.
scaling_config = ScalingConfig(
    num_workers=4, use_gpu=True, resources_per_worker={"CPU": 1, "GPU": 1}
)


trainer = LightningTrainer(
    lightning_config=lightning_config,
    run_config=run_config,
    scaling_config=scaling_config,
    datasets={"train": ray_datasets["train"], "val": ray_datasets["validation"]},
    datasets_iter_config={"batch_size": 4},
    preprocessor=preprocessor,
)
result = trainer.fit()


# :::{note}
# Note that we are using Ray Dataset for data ingestion for faster preprocessing here, but you can also continue to use the native `PyTorch DataLoader` or `LightningDataModule`. See {ref}`this example <lightning_mnist_example>`. 
# 
# :::

# In[16]:


result
