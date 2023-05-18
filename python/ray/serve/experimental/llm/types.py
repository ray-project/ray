from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

class Batch(ABC):
    @abstractmethod
    def filter(self, request_ids: List[int]) -> "Batch":
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def concatenate(cls, batches: List["Batch"]) -> "Batch":
        raise NotImplementedError

    @abstractmethod
    def __len__(self):
        raise NotImplementedError


class FinishReason(Enum):
    LENGTH = 1
    EOS_TOKEN = 2
    STOP_SEQUENCE = 3


@dataclass
class GeneratedText:
    text: str
    generated_tokens: int
    finish_reason: FinishReason
    seed: Optional[int]


@dataclass
class Generation:
    request_id: int
    token_id: int
    token_logprob: float
    token_text: str
    token_is_special: bool
    generated_text: Optional[GeneratedText]


@dataclass
class GenerationRequest:
    id: int
    input_text: str


@dataclass
class SamplingParams:
    temperature: float
    top_k: int
    top_p: float
