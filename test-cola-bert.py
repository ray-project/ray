#!/usr/bin/env python
# coding: utf-8

# (lightning_advanced_example)=
# 
# # Finetune a BERT Text Classifier with LightningTrainer
# 
# :::{note}
# 
# This is an advanced example for {class}`LightningTrainer <ray.train.lightning.LightningTrainer>`, which demonstrates how to use LightningTrainer with {ref}`Ray Dataset <datasets>` and {ref}`Batch Predictor <air-predictors>`. 
# 
# If you just want to quickly convert your existing PyTorch Lightning scripts into Ray AIR, you can refer to this starter example:
# {ref}`Train a Pytorch Lightning Image Classifier <lightning_mnist_example>`.
# 
# :::
# 
# In this demo, we will introduce how to finetune a text classifier on [CoLA(The Corpus of Linguistic Acceptability)](https://nyu-mll.github.io/CoLA/) datasets with pretrained BERT. 
# In particular, we will:
# - Create Ray Datasets from the original CoLA dataset.
# - Define a preprocessor to tokenize the sentences.
# - Finetune a BERT model using LightningTrainer.
# - Construct a BatchPredictor with the checkpoint and preprocessor.
# - Do batch prediction on multiple GPUs, and evaluate the results.

# In[5]:


SMOKE_TEST = False


# Uncomment and run the following line in order to install all the necessary dependencies:

# In[6]:


# !pip install numpy transformers datasets pytorch_lightning


# Let's start by importing the needed libraries:

# In[7]:


import ray
import torch
import pytorch_lightning as pl
import torch.nn.functional as F
from torch.utils.data import DataLoader, random_split
from transformers import AutoTokenizer, AutoModelForSequenceClassification
from datasets import load_dataset, load_metric
import numpy as np


# ## 1. Preprocess CoLA Dataset
# 
# CoLA is a binary sentence classification task with 10.6K training examples. First, we download the dataset and metrics using the HuggingFace API, and create Ray Datasets for each split accordingly.

# In[8]:


dataset = load_dataset("glue", "cola")
metric = load_metric("glue", "cola")


# In[9]:


ray_datasets = ray.data.from_huggingface(dataset)


# Next, define a preprocessor that tokenizes the input sentences and pads the ID sequence to length 128 using the bert-base-uncased tokenizer. The preprocessor transforms all datasets that we provide to the LightningTrainer later.

# In[10]:


from ray.data.preprocessors import BatchMapper

tokenizer = AutoTokenizer.from_pretrained("bert-base-cased")
import transformers

def tokenize_sentence(batch):
    encoded_sent = tokenizer(
        batch["sentence"].tolist(),
        max_length=128,
        truncation=True,
        padding="max_length",
        return_tensors="pt",
    )
    batch["input_ids"] = encoded_sent["input_ids"].numpy()
    batch["attention_mask"] = encoded_sent["attention_mask"].numpy()
    batch["label"] = np.array(batch["label"])
    batch.pop("sentence")
    return batch


preprocessor = BatchMapper(tokenize_sentence, batch_format="numpy")


# ## 2. Define a PyTorch Lightning Model
# 
# You don't have to make any change of your `LightningModule` definition. Just copy and paste your code here:

# In[11]:


class SentimentModel(pl.LightningModule):
    def __init__(self, lr=2e-5, eps=1e-8):
        super().__init__()
        self.lr = lr
        self.eps = eps
        self.num_classes = 2
        transformers.utils.logging.set_verbosity_error()
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "bert-base-cased", num_labels=self.num_classes
        )
        self.metric = load_metric("glue", "cola")
        self.predictions = []
        self.references = []

    def forward(self, batch):
        input_ids, attention_mask = batch["input_ids"], batch["attention_mask"]
        outputs = self.model(input_ids, attention_mask=attention_mask)
        logits = outputs.logits
        return logits

    def training_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        loss = F.cross_entropy(logits.view(-1, self.num_classes), labels)
        self.log("train_loss", loss)
        return loss

    def validation_step(self, batch, batch_idx):
        labels = batch["label"]
        logits = self.forward(batch)
        preds = torch.argmax(logits, dim=1)
        self.predictions.append(preds)
        self.references.append(labels)

    def on_validation_epoch_end(self):
        predictions = torch.concat(self.predictions).view(-1)
        references = torch.concat(self.references).view(-1)
        matthews_correlation = self.metric.compute(
            predictions=predictions, references=references
        )

        # self.metric.compute() returns a dictionary:
        # e.g. {"matthews_correlation": 0.53}
        self.log_dict(matthews_correlation, sync_dist=True)
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

# Define the configs for LightningTrainer
lightning_config = (
    LightningConfigBuilder()
    .module(cls=SentimentModel, lr=1e-5, eps=1e-8)
    .trainer(max_epochs=3, accelerator="gpu")
    .checkpointing(save_on_train_epoch_end=False)
    .strategy(name="fsdp")
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


# In[14]:


if SMOKE_TEST:
    lightning_config = (
        LightningConfigBuilder()
        .module(cls=SentimentModel, lr=1e-5, eps=1e-8)
        .trainer(max_epochs=2, accelerator="gpu")
        .checkpointing(save_on_train_epoch_end=False)
        .strategy(name="fsdp")
        .build()
    )

    for split, ds in ray_datasets.items():
        ray_datasets[split] = ds.random_sample(0.1)


# ## 4. Fine-tune the model with LightningTrainer
# 
# Train the model with the configuration we specified above. 
# 
# To feed data into LightningTrainer, we need to configure the following arguments:
# 
# - datasets: A dictionary of the input Ray datasets, with special keys "train" and "val".
# - datasets_iter_config: The argument list of {meth}`iter_torch_batches() <ray.data.Dataset.iter_torch_batches>`. It defines the way we iterate dataset shards for each worker.
# - preprocessor: The preprocessor that will be applied to the input dataset.
# 
# Now, call `trainer.fit()` to initiate the training process.

# In[15]:


trainer = LightningTrainer(
    lightning_config=lightning_config,
    run_config=run_config,
    scaling_config=scaling_config,
    datasets={"train": ray_datasets["train"], "val": ray_datasets["validation"]},
    datasets_iter_config={"batch_size": 16},
    preprocessor=preprocessor,
)
result = trainer.fit()


# :::{note}
# Note that we are using Ray Dataset for data ingestion for faster preprocessing here, but you can also continue to use the native `PyTorch DataLoader` or `LightningDataModule`. See {ref}`this example <lightning_mnist_example>`. 
# 
# :::

# In[16]:


result
