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
import re
import torch

from functools import lru_cache
from transformers import (
    TemperatureLogitsWarper,
    TopKLogitsWarper,
    TopPLogitsWarper,
    TypicalLogitsWarper,
    RepetitionPenaltyLogitsProcessor,
    PreTrainedTokenizerBase,
)
from typing import List, Tuple, Optional

from ray.serve.experimental.llm.watermark import WatermarkLogitsProcessor
from ray.serve.experimental.llm.types import SamplingParams, FinishReason


class Sampling:
    def __init__(self, seed: int, device: str = "cpu"):
        self.generator = torch.Generator(device)
        self.generator.manual_seed(seed)
        self.seed = seed

    def __call__(self, logits):
        probs = torch.nn.functional.softmax(logits, -1)
        # Avoid GPU<->CPU sync done by torch multinomial
        # See: https://github.com/pytorch/pytorch/blob/925a3788ec5c06db62ca732a0e9425a26a00916f/aten/src/ATen/native/Distributions.cpp#L631-L637
        q = torch.empty_like(probs).exponential_(1, generator=self.generator)
        return probs.div_(q).argmax()


class Greedy:
    def __call__(self, logits):
        return logits.argmax()


class StaticWarper:
    def __init__(
        self,
        temperature=1.0,
        top_k=None,
        top_p=None,
        typical_p=None,
    ):
        self.warpers = []

        if temperature is not None and temperature != 1.0:
            temperature = float(temperature)
            self.warpers.append(TemperatureLogitsWarper(temperature))
        if top_k is not None and top_k != 0:
            self.warpers.append(TopKLogitsWarper(top_k=top_k))
        if top_p is not None and top_p < 1.0:
            self.warpers.append(TopPLogitsWarper(top_p=top_p))
        if typical_p is not None and typical_p < 1.0:
            self.warpers.append(TypicalLogitsWarper(mass=typical_p))

        self.cuda_graph = None
        self.static_scores = None
        self.static_warped_scores = None
        self.static_next_logprob = None

    def __call__(self, scores):
        if self.cuda_graph is None:
            self.static_scores = scores
            self.cuda_graph = torch.cuda.CUDAGraph()

            with torch.cuda.graph(self.cuda_graph):
                for warper in self.warpers:
                    self.static_warped_scores = warper(None, self.static_scores)

                # Compute logprobs
                self.static_next_logprob = torch.log_softmax(
                    self.static_warped_scores, -1
                )

        self.static_scores.copy_(scores)
        self.cuda_graph.replay()

        return self.static_warped_scores, self.static_next_logprob


@lru_cache(10)
def static_warper(
    temperature: Optional[float],
    top_k: Optional[int],
    top_p: Optional[float],
    typical_p: Optional[float],
) -> StaticWarper:
    return StaticWarper(
        temperature=temperature, top_k=top_k, top_p=top_p, typical_p=typical_p
    )


class NextTokenChooser:
    def __init__(
        self,
        watermark=False,
        temperature=1.0,
        repetition_penalty=1.0,
        top_k=None,
        top_p=None,
        typical_p=None,
        do_sample=False,
        seed=0,
        device="cpu",
    ):
        self.watermark_processor = (
            WatermarkLogitsProcessor(device=device) if watermark else None
        )
        self.repetition_processor = (
            RepetitionPenaltyLogitsProcessor(penalty=repetition_penalty)
            if repetition_penalty
            else None
        )

        has_warpers = (
            (temperature is not None and temperature != 1.0)
            or (top_k is not None and top_k != 0)
            or (top_p is not None and top_p < 1.0)
            or (typical_p is not None and typical_p < 1.0)
        )
        if has_warpers:
            self.static_warper = static_warper(
                temperature=temperature, top_k=top_k, top_p=top_p, typical_p=typical_p
            )
        else:
            self.static_warper = None

        sampling = do_sample or has_warpers
        self.choice = Sampling(seed, device) if sampling else Greedy()

    def __call__(self, input_ids, scores):
        if self.watermark_processor:
            scores = self.watermark_processor(input_ids, scores)
        if self.repetition_processor:
            # FIXME: scores.clone() is used to avoid following error:
            # RuntimeError: Output 0 of SliceBackward0 is a view and is being modified
            # inplace. This view is the output of a function that returns multiple views.
            # Such functions do not allow the output views to be modified inplace.
            # You should replace the inplace operation by an out-of-place one.
            # scores = self.repetition_processor(input_ids, scores.clone())
            pass

        if self.static_warper is None:
            next_logprob = torch.log_softmax(scores, -1)
        else:
            scores, next_logprob = self.static_warper(scores)

        next_id = self.choice(scores[-1]).view(1, 1)

        return next_id, next_logprob

    @classmethod
    def from_params(
        cls,
        params: SamplingParams,
        device: torch.device,
    ) -> "NextTokenChooser":
        return NextTokenChooser(
            watermark=params.watermark,
            temperature=params.temperature,
            repetition_penalty=params.repetition_penalty,
            top_k=params.top_k,
            top_p=params.top_p,
            typical_p=params.typical_p,
            do_sample=params.do_sample,
            seed=params.seed,
            device=device,
        )


class StopSequenceCriteria:
    def __init__(self, stop_sequence: str):
        stop_sequence = re.escape(stop_sequence)
        self.regex = re.compile(f".*{stop_sequence}$")

    def __call__(self, output: str) -> bool:
        if self.regex.findall(output):
            return True
        return False


class StoppingCriteria:
    def __init__(
        self,
        eos_token_id: int,
        stop_sequence_criterias: List[StopSequenceCriteria],
        max_new_tokens: int = 20,
        ignore_eos_token: bool = False,
    ):
        self.eos_token_id = eos_token_id
        self.stop_sequence_criterias = stop_sequence_criterias
        self.max_new_tokens = max_new_tokens
        self.current_tokens = 0
        self.current_output = ""
        self.ignore_eos_token = ignore_eos_token

    def __call__(self, last_token: int, last_output: str) -> Tuple[bool, Optional[str]]:
        self.current_tokens += 1
        if self.current_tokens >= self.max_new_tokens:
            return True, FinishReason.LENGTH

        if not self.ignore_eos_token and last_token == self.eos_token_id:
            return True, FinishReason.EOS_TOKEN

        self.current_output += last_output
        for stop_sequence_criteria in self.stop_sequence_criterias:
            if stop_sequence_criteria(self.current_output):
                return True, FinishReason.STOP_SEQUENCE

        return False, None

    @classmethod
    def from_params(
        cls,
        params: SamplingParams,
        tokenizer: PreTrainedTokenizerBase,
    ) -> "StoppingCriteria":
        stop_sequence_criterias = [
            StopSequenceCriteria(sequence) for sequence in params.stop_sequences
        ]
        return StoppingCriteria(
            tokenizer.eos_token_id,
            stop_sequence_criterias,
            params.max_new_tokens,
            params.ignore_eos_token,
        )


def tensor_memory(tensor):
    if tensor.is_cuda:  # only for tensors stored in GPU
        return tensor.element_size() * tensor.nelement()
    else:
        return 0
