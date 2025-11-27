import json
from pathlib import Path

import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F
from ray.train.torch import get_device


def pad_array(arr, dtype=np.int32):
    max_len = max(len(row) for row in arr)
    padded_arr = np.zeros((arr.shape[0], max_len), dtype=dtype)
    for i, row in enumerate(arr):
        padded_arr[i][: len(row)] = row
    return padded_arr


def collate_fn(batch, device=None):
    dtypes = {"embedding": torch.float32, "label": torch.int64}
    tensor_batch = {}
    
    # If no device is provided, try to get it from Ray Train context
    if device is None:
        try:
            device = get_device()
        except RuntimeError:
            # When not in Ray Train context, use CPU for testing/serving
            device = "cpu"
    
    for key in dtypes.keys():
        if key in batch:
            tensor_batch[key] = torch.as_tensor(
                batch[key],
                dtype=dtypes[key],
                device=device,
            )
    return tensor_batch


class ClassificationModel(torch.nn.Module):
    def __init__(self, embedding_dim, hidden_dim, dropout_p, num_classes):
        super().__init__()
        # Hyperparameters
        self.embedding_dim = embedding_dim
        self.hidden_dim = hidden_dim
        self.dropout_p = dropout_p
        self.num_classes = num_classes

        # Define layers
        self.fc1 = nn.Linear(embedding_dim, hidden_dim)
        self.batch_norm = nn.BatchNorm1d(hidden_dim)
        self.relu = nn.ReLU()
        self.dropout = nn.Dropout(dropout_p)
        self.fc2 = nn.Linear(hidden_dim, num_classes)

    def forward(self, batch):
        z = self.fc1(batch["embedding"])
        z = self.batch_norm(z)
        z = self.relu(z)
        z = self.dropout(z)
        z = self.fc2(z)
        return z

    @torch.inference_mode()
    def predict(self, batch):
        z = self(batch)
        y_pred = torch.argmax(z, dim=1).cpu().numpy()
        return y_pred

    @torch.inference_mode()
    def predict_probabilities(self, batch):
        z = self(batch)
        y_probs = F.softmax(z, dim=1).cpu().numpy()
        return y_probs

    def save(self, dp):
        Path(dp).mkdir(parents=True, exist_ok=True)
        with open(Path(dp, "args.json"), "w") as fp:
            json.dump(
                {
                    "embedding_dim": self.embedding_dim,
                    "hidden_dim": self.hidden_dim,
                    "dropout_p": self.dropout_p,
                    "num_classes": self.num_classes,
                },
                fp,
                indent=4,
            )
        torch.save(self.state_dict(), Path(dp, "model.pt"))

    @classmethod
    def load(cls, args_fp, state_dict_fp, device="cpu"):
        with open(args_fp, "r") as fp:
            model = cls(**json.load(fp))
        model.load_state_dict(torch.load(state_dict_fp, map_location=device))
        return model
