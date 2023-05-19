# coding=utf-8
# Code adapted from "Text Generation Inference"
# available at https://github.com/huggingface/text-generation-inference
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import torch

from enum import Enum
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import List, Optional

from transformers import PreTrainedTokenizerBase


class Batch(ABC):
    @classmethod
    @abstractmethod
    def from_requests(
        cls,
        requests: List["GenerationRequest"],
        tokenizer: PreTrainedTokenizerBase,
        device: torch.device,
    ) -> "Batch":
        raise NotImplementedError

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

    @property
    @abstractmethod
    def id(self):
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
    stopped: bool
    generated_text: Optional[GeneratedText]


@dataclass
class SamplingParams:
    temperature: float
    top_k: int
    top_p: float
    typical_p: float
    do_sample: bool
    seed: int
    repetition_penalty: float
    watermark: bool

    max_new_tokens: int
    stop_sequences: List[str]
    ignore_eos_token: bool


@dataclass
class GenerationRequest:
    id: int
    input_text: str
    max_length: int
    input_length: int
    sampling_params: SamplingParams
