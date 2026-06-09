"""Tokenized causal-LM dataloaders shared across LLM framework adapters.

Provides a single ``build_text_dataloader`` entrypoint used by the deepspeed,
torchtitan, and (later) megatron adapters so token accounting and batching are
consistent across frameworks. Supports:

  - "synthetic": random token ids, no network. Used for smoke tests and for
    isolating framework/compute throughput from data-ingest effects.
  - HF dataset ids (e.g. "wikitext", "ag_news") or "c4": streamed and tokenized
    with the model's tokenizer.

Every batch is a dict with ``input_ids`` and ``attention_mask`` of shape
[batch_size, seq_len], matching what HF ``AutoModelForCausalLM`` expects.
"""

import logging
from typing import Any, Dict, Iterator, Optional

import torch
from torch.utils.data import DataLoader, Dataset, IterableDataset

logger = logging.getLogger(__name__)


# Registered HF dataset ids + the split/config needed to stream them.
_HF_DATASETS: Dict[str, Dict[str, Any]] = {
    "c4": {"path": "allenai/c4", "name": "en", "split": "train", "streaming": True},
    "wikitext": {
        "path": "wikitext",
        "name": "wikitext-103-raw-v1",
        "split": "train",
        "streaming": True,
    },
    "ag_news": {"path": "ag_news", "split": "train", "streaming": False},
}


class SyntheticTokenDataset(IterableDataset):
    """Emits random token-id sequences of fixed length.

    Deterministic per (seed, worker) so runs are reproducible, infinite so the
    training loop is bounded by num_steps rather than dataset size.
    """

    def __init__(self, seq_len: int, vocab_size: int, seed: int):
        self._seq_len = seq_len
        self._vocab_size = vocab_size
        self._seed = seed

    def __iter__(self) -> Iterator[Dict[str, torch.Tensor]]:
        generator = torch.Generator()
        generator.manual_seed(self._seed)
        while True:
            input_ids = torch.randint(
                0, self._vocab_size, (self._seq_len,), generator=generator
            )
            yield {
                "input_ids": input_ids,
                "attention_mask": torch.ones(self._seq_len, dtype=torch.long),
            }


class TokenizedTextDataset(Dataset):
    """Materialized, pre-tokenized map-style dataset for HF text corpora."""

    def __init__(self, encodings: Dict[str, torch.Tensor]):
        self._encodings = encodings
        self._length = encodings["input_ids"].shape[0]

    def __len__(self) -> int:
        return self._length

    def __getitem__(self, idx: int) -> Dict[str, torch.Tensor]:
        return {k: v[idx] for k, v in self._encodings.items()}


def _build_synthetic_loader(
    seq_len: int, batch_size: int, seed: int, vocab_size: int
) -> DataLoader:
    dataset = SyntheticTokenDataset(seq_len, vocab_size, seed)
    return DataLoader(dataset, batch_size=batch_size)


def _build_hf_loader(
    dataset_name: str,
    tokenizer: Any,
    seq_len: int,
    batch_size: int,
    limit_rows: int,
    shuffle: bool,
) -> DataLoader:
    from datasets import DownloadConfig, load_dataset

    if dataset_name not in _HF_DATASETS:
        raise ValueError(
            f"Unknown dataset '{dataset_name}'. Known: "
            f"{sorted(_HF_DATASETS) + ['synthetic']}"
        )
    spec = _HF_DATASETS[dataset_name]
    # Cap the number of raw rows we tokenize. For streaming datasets a 1% slice
    # syntax isn't available, so we take() from the stream instead.
    n = limit_rows if limit_rows > 0 else 2000

    load_kwargs = {
        k: v for k, v in spec.items() if k in ("path", "name", "split", "streaming")
    }
    load_kwargs["download_config"] = DownloadConfig(disable_tqdm=True)
    dataset = load_dataset(**load_kwargs)

    if spec.get("streaming"):
        texts = [row["text"] for row in dataset.take(n)]
    else:
        dataset = dataset.select(range(min(n, len(dataset))))
        texts = dataset["text"]

    encodings = tokenizer(
        texts,
        padding="max_length",
        max_length=seq_len,
        truncation=True,
        return_tensors="pt",
    )
    encodings = {"input_ids": encodings["input_ids"], "attention_mask": encodings["attention_mask"]}
    return DataLoader(
        TokenizedTextDataset(encodings), batch_size=batch_size, shuffle=shuffle
    )


def build_text_dataloader(
    dataset_name: str,
    dataset_path: str,
    tokenizer: Optional[Any],
    seq_len: int,
    batch_size: int,
    seed: int = 42,
    limit_rows: int = -1,
    shuffle: bool = True,
    synthetic_vocab_size: int = 32000,
) -> DataLoader:
    """Build a causal-LM dataloader.

    When ``dataset_name == "synthetic"`` the tokenizer is unused and random
    token ids are generated, so this path needs neither network nor a real
    tokenizer — ideal for CPU smoke tests of the harness itself.
    """
    if dataset_name == "synthetic":
        vocab_size = (
            tokenizer.vocab_size
            if tokenizer is not None and hasattr(tokenizer, "vocab_size")
            else synthetic_vocab_size
        )
        return _build_synthetic_loader(seq_len, batch_size, seed, vocab_size)

    if tokenizer is None:
        raise ValueError(f"A tokenizer is required for dataset '{dataset_name}'.")
    return _build_hf_loader(
        dataset_name, tokenizer, seq_len, batch_size, limit_rows, shuffle
    )
