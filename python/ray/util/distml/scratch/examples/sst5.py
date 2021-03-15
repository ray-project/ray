import transformers
import os
import numpy as np
import numpy.random as npr
import pandas as pd
from tqdm import tqdm
from typing import Tuple
from concurrent.futures import ProcessPoolExecutor

TEXT_COL, LABEL_COL = 'text', 'truth'
MAX_LENGTH = 256
DATASET_DIR = "./sst"

__all__ = ["make_sst_dataloader"]


def _one_hot(x, k, dtype=np.float32):
    """Create a one-hot encoding of x of size k."""
    return np.array(x[:, None] == np.arange(k), dtype)


class Dataloader:
    def __init__(self, data, target, batch_size=128):
        '''
        data: shape(width, height, channel, num)
        target: shape(num, num_classes)
        '''
        self.data = data
        self.target = target
        self.batch_size = batch_size
        num_data = self.target.shape[0]
        num_complete_batches, leftover = divmod(num_data, batch_size)
        self.num_batches = num_complete_batches + bool(leftover)

    def synth_batches(self):
        num_imgs = self.target.shape[0]
        rng = npr.RandomState(np.random.randint(10))
        perm = rng.permutation(num_imgs)
        for i in range(self.num_batches):
            batch_idx = perm[i * self.batch_size:(i + 1) * self.batch_size]
            img_batch = self.data[batch_idx]
            label_batch = self.target[batch_idx]
            yield img_batch, label_batch

    def __iter__(self):
        return self.synth_batches()


def read_sst5(data_dir, colnames=[LABEL_COL, TEXT_COL]):
    def read_csv(mode="train"):
        df = pd.read_csv(os.path.join(data_dir, f"sst_{mode}.txt"), sep='\t', header=None, names=colnames)
        df[LABEL_COL] = df[LABEL_COL].str.replace('__label__', '')
        df[LABEL_COL] = df[LABEL_COL].astype(int)   # Categorical data type for truth labels
        df[LABEL_COL] = df[LABEL_COL] - 1  # Zero-index labels for PyTorch
        return df
    return read_csv("train"), read_csv("dev"), read_csv("test")


class TextProcessor:
    def __init__(self, tokenizer, label2id: dict, clf_token, pad_token, max_length):
        self.tokenizer = tokenizer
        self.label2id = label2id
        self.max_length = max_length
        self.clf_token = clf_token
        self.pad_token = pad_token

    def encode(self, input):
        return list(self.tokenizer.convert_tokens_to_ids(o) for o in input)

    def token2id(self, item: Tuple[str, str]):
        "Convert text (item[0]) to sequence of IDs and label (item[1]) to integer"
        assert len(item) == 2   # Need a row of text AND labels
        label, text = item[0], item[1]
        assert isinstance(text, str)   # Need position 1 of input to be of type(str)
        inputs = self.tokenizer.tokenize(text)
        # Trim or pad dataset
        if len(inputs) >= self.max_length:
            inputs = inputs[:self.max_length - 1]
            ids = [self.clf_token] + self.encode(inputs)
        else:
            pad = [self.pad_token] * (self.max_length - len(inputs) - 1)
            ids = [self.clf_token] + self.encode(inputs) + pad

        return np.array(ids, dtype='int64'), self.label2id[label]

    def process_row(self, row):
        "Calls the token2id method of the text processor for passing items to executor"
        return self.token2id((row[1][LABEL_COL], row[1][TEXT_COL]))

    def process_data(self, df: pd.DataFrame):
        "Process rows in pd.DataFrame using n_cpus and return data and labels"

        tqdm.pandas()
        with ProcessPoolExecutor(max_workers=4) as executor:
            result = list(
                tqdm(executor.map(self.process_row, df.iterrows(), chunksize=8192),
                     desc=f"Processing {len(df)} examples on 4 cores",
                     total=len(df)))

        data = np.array([r[0] for r in result])
        labels = np.array([r[1] for r in result])
        labels = _one_hot(labels, 5)

        return data, labels


def make_sst_dataloader(batch_size=128):
    train_data, val_data, test_data = read_sst5(DATASET_DIR)

    # print(train_data.keys()) # Index(['truth', 'text'], dtype='object')
    # print(train_dataset["text"].tolist())

    labels = list(set(train_data[LABEL_COL].tolist()))
    label2int = {label: i for i, label in enumerate(labels)}

    tokenizer = transformers.AutoTokenizer.from_pretrained("bert-base-uncased")
    clf_token = tokenizer.vocab['[CLS]']  # classifier token
    pad_token = tokenizer.vocab['[PAD]']  # pad token
    proc = TextProcessor(tokenizer, label2int, clf_token, pad_token, max_length=MAX_LENGTH)

    train_input, train_target = proc.process_data(train_data)
    val_input, val_target = proc.process_data(val_data)
    testinput, test_target = proc.process_data(test_data)

    print(f"vocab size is {tokenizer.vocab_size}")

    train_dataloader = Dataloader(train_input, train_target, batch_size=batch_size)
    val_dataloader = Dataloader(val_input, val_target, batch_size=batch_size)
    test_dataloader = Dataloader(testinput, test_target, batch_size=batch_size)

    return train_dataloader, val_dataloader, test_dataloader


if __name__ == "__main__":
    train_dataloader, _, _ = make_sst_dataloader()

    for input, target in train_dataloader:
        print(input.shape, target.shape)

    # train_data, dev_data, test_data = read_sst5(DATASET_DIR)

    # print(train_data.keys()) # Index(['truth', 'text'], dtype='object')
    # # print(train_dataset["text"].tolist())


    # TextProcessor()

    # train_loader = Dataloader(data_train, labels_train, batch_size=batch_size)
    # test_loader = Dataloader(data_test, labels_test, batch_size=batch_size)


    # labels = list(set(datasets["train"][LABEL_COL].tolist()))
    # label2int = {label: i for i, label in enumerate(labels)}
    # tokenizer = BertTokenizer.from_pretrained('bert-base-cased', do_lower_case=False)
    # clf_token = tokenizer.vocab['[CLS]']  # classifier token
    # pad_token = tokenizer.vocab['[PAD]']  # pad token
    # proc = TextProcessor(tokenizer, label2int, clf_token, pad_token, max_length=MAX_LENGTH)

    # train_dl = proc.create_dataloader(datasets["train"], batch_size=32)
    # valid_dl = proc.create_dataloader(datasets["dev"], batch_size=32)
    # test_dl = proc.create_dataloader(datasets["test"], batch_size=32)

    # print(len(train_dl), len(valid_dl), len(test_dl))
