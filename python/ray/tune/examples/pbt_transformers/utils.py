"""Utilities to load and cache data."""

import os
from sklearn.model_selection import train_test_split
from filelock import FileLock
import torch
from torch.utils.data import TensorDataset
from transformers import AutoModelForSequenceClassification, AutoTokenizer, Trainer
from transformers import glue_convert_examples_to_features as convert_examples_to_features
from transformers.data.processors import glue_processors


def download_data(model_name, task_name, data_dir="./data"):
    # Download RTE training data
    print("Downloading dataset.")
    import urllib
    import zipfile
    if task_name == "rte":
        url = "https://firebasestorage.googleapis.com/v0/b/mtl-sentence-representations.appspot.com/o/data%2FRTE.zip?alt=media&token=5efa7e85-a0bb-4f19-8ea2-9e1840f077fb"
    else:
        raise ValueError("Unknown task: {}".format(task_name))
    data_file = os.path.join(data_dir, "{}.zip".format(task_name))
    if not os.path.exists(data_file):
        urllib.request.urlretrieve(url, data_file)
        with zipfile.ZipFile(data_file) as zip_ref:
            zip_ref.extractall(data_dir)
        print("Downloaded data for task {} to {}".format(task_name, data_dir))


def featurize_and_save(examples, file, task, model_name):
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    features = convert_examples_to_features(
        examples,
        tokenizer,
        max_length=128,
        task=task
    )
    print("Saving features into cached file {}".format(file))
    torch.save(features, file)


def get_cache_file(data_dir, mode):
    return os.path.join(data_dir, "cached_{}".format(mode))


def load_and_cache(task, model_name, data_dir):
    print("Downloading and caching Tokenizer")
    # Triggers tokenizer download to cache
    AutoTokenizer.from_pretrained(model_name)
    print("Downloading and caching pre-trained model")
    # Triggers model download to cache
    AutoModelForSequenceClassification.from_pretrained(
        model_name,
    )
    download_data(model_name, task, data_dir)

    print("Loading and featurizing examples")
    task_data_dir = os.path.join(data_dir, task.upper())
    processor = glue_processors[task]()
    train_cache_file = get_cache_file(task_data_dir, "train")
    val_cache_file = get_cache_file(task_data_dir, "val")
    test_cache_file = get_cache_file(task_data_dir, "test")
    if not os.path.exists(train_cache_file):
        examples = processor.get_train_examples(task_data_dir)
        featurize_and_save(examples, train_cache_file, task, model_name)
    if not os.path.exists(val_cache_file):
        # Load in dev set and split into val/test
        examples = processor.get_dev_examples(task_data_dir)
        val_examples, test_examples = train_test_split(examples, test_size=0.5)
        featurize_and_save(val_examples, val_cache_file, task, model_name)
        featurize_and_save(test_examples, test_cache_file, task, model_name)


def get_datasets(data_dir, task, mode):
    task_data_dir = os.path.join(data_dir, task.upper())
    cache_file = get_cache_file(task_data_dir, mode)
    print("Loading features from cached file {}".format(cache_file))
    with FileLock("/tmp/load_and_cache_examples.lock"):
        assert os.path.exists(cache_file)
        features = torch.load(cache_file)
    return features
