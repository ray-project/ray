"""
This module defines a dataset framework for sampling benchmark requests.
"""

from abc import ABC, abstractmethod
from pathlib import Path
from typing import Dict, List, Optional

from datasets import load_dataset, load_from_disk


class BenchmarkDataset(ABC):
    DEFAULT_RANDOM_SEED = 0

    def __init__(
        self,
        dataset_path: Optional[str] = None,
        random_seed: int = DEFAULT_RANDOM_SEED,
    ) -> None:
        """
        Abstract base class for benchmark datasets.

        All benchmark datasets should inherit from this class and implement
        the required abstract methods.

        Args:
            dataset_path: The path to the dataset on disk.
            random_seed: The seed for the random number generator.
        """
        self._dataset_path = dataset_path
        self._random_seed = random_seed

    @abstractmethod
    def load_data(self) -> None:
        """
        Load data from the dataset source into memory.

        Raises:
            NotImplementedError: If the method is not implemented in subclasses.
        """
        raise NotImplementedError("load_data must be implemented in subclasses.")

    @abstractmethod
    def sample(self, num_requests: int) -> List[Dict]:
        """
        Sample prompts from the loaded dataset.

        Args:
            num_requests: The number of prompts to sample from the dataset.

        Returns:
            A list of sampled request dictionaries.

        Raises:
            NotImplementedError: If the method is not implemented in subclasses.
        """
        raise NotImplementedError("sample must be implemented in subclasses.")


class ShareGPTDataset(BenchmarkDataset):
    """Implements the ShareGPT dataset. The first human message of each conversation is used to build a prompt."""

    def __init__(
        self,
        dataset_path: str,
        seed: int,
        hf_dataset_id: str = "Crystalcareai/Code-feedback-sharegpt-renamed",
        hf_split: str = "train",
        truncate_prompt: Optional[int] = None,
    ) -> None:
        """
        Initializes the ShareGPTDataset.

        Args:
            dataset_path: The path to the dataset on disk.
            seed: The seed for the random number generator.
            hf_dataset_id: The Hugging Face dataset ID to download if the dataset is not found on disk.
            hf_split: The Hugging Face split to load from the dataset.
            truncate_prompt: Maximum prompt length so that the prompt fits in the model's context window.
        """
        super().__init__(dataset_path, seed)
        self._seed = seed

        self._hf_dataset_id = hf_dataset_id
        self._hf_split = hf_split
        self._truncate_prompt = truncate_prompt

        self._data: list[Dict] | None = None

    def load_data(self) -> None:
        """Load data from the dataset path into memory."""
        if self._data is None:
            self._data = self._load_dataset_data()

    def sample(self, num_requests: int) -> List[Dict]:
        """Sample prompts from the loaded dataset."""
        if self._data is None:
            self.load_data()

        prompts = []
        for item in self._data:
            if len(prompts) >= num_requests:
                break

            prompt_data = self._extract_prompt(item)
            if prompt_data is not None:
                prompts.append(prompt_data)

        if not prompts:
            raise ValueError("ShareGPT dataset yielded no usable prompts")
        return prompts

    def _load_dataset(self):
        """Load dataset from disk or Hugging Face."""
        path = Path(self._dataset_path)
        print(f"Attempting to load dataset from {path}")
        print(f"Dataset exists on disk: {path.exists()}")

        try:
            if path.exists():
                dataset = load_from_disk(str(path))
            else:
                print(
                    f"Dataset not found on disk, downloading from Hugging Face: {self._hf_dataset_id}"
                )

                path.parent.mkdir(parents=True, exist_ok=True)
                dataset = load_dataset(self._hf_dataset_id, split=self._hf_split)
                dataset.save_to_disk(str(path))
            return dataset

        except Exception as e:
            raise RuntimeError(f"Error loading ShareGPT dataset: {e}")

    def _load_dataset_data(self) -> List[Dict]:
        """Load and process dataset data into a list of dictionaries."""
        ds = self._load_dataset().shuffle(seed=self._seed)
        data = []

        for i, row in enumerate(ds):
            data.append(row)

        print(f"Loaded {len(data)} samples from dataset")
        return data

    def _extract_prompt(self, item: Dict) -> Dict | None:
        """
        Extracts the first human message of a conversation or None.

        The ShareGPT schema uses {"role": "human", "value": ...} for user
        turns.
        """
        messages = item.get("messages") or item.get("conversations") or []
        prompt = next(
            (
                str(msg.get("value", "")).strip()
                for msg in messages
                if msg.get("role") in {"human", "user"}
            ),
            None,
        )

        # Only return a valid prompt if it's not empty
        if prompt and prompt.strip():
            if self._truncate_prompt:
                prompt = prompt[: self._truncate_prompt]
            return {"prompt": prompt}

        return None
