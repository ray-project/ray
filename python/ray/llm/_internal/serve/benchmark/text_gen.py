"""Text generation and conversation management for the benchmark."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING, Optional

import numpy as np

from ray.llm._internal.serve.benchmark.models import WorkloadSpec

if TYPE_CHECKING:
    from transformers import PreTrainedTokenizerBase

logger = logging.getLogger(__name__)


class Conversation:
    """A single multi-turn conversation with a unique session ID."""

    def __init__(
        self,
        session_id: str,
        system_prompt: str,
        user_messages: list[str],
        num_turns: int,
    ):
        self.session_id = session_id
        self.system_prompt = system_prompt
        self.user_messages = user_messages
        self.num_turns = num_turns
        self._assistant_responses: list[str] = []

    def get_turn_messages(self, turn_idx: int) -> list[dict[str, str]]:
        """Build the messages list for turn `turn_idx` (0-indexed)."""
        messages: list[dict[str, str]] = []
        if self.system_prompt:
            messages.append({"role": "system", "content": self.system_prompt})

        for i in range(turn_idx + 1):
            messages.append({"role": "user", "content": self.user_messages[i]})
            if i < turn_idx:
                if i < len(self._assistant_responses):
                    messages.append(
                        {"role": "assistant", "content": self._assistant_responses[i]}
                    )
                else:
                    messages.append({"role": "assistant", "content": "(placeholder)"})
        return messages

    def inject_assistant_response(self, turn_idx: int, content: str) -> None:
        """Record the server's response for turn `turn_idx`."""
        if turn_idx == len(self._assistant_responses):
            self._assistant_responses.append(content)
        elif turn_idx < len(self._assistant_responses):
            self._assistant_responses[turn_idx] = content
        else:
            raise ValueError(
                f"Cannot inject response for turn {turn_idx}: "
                f"only {len(self._assistant_responses)} responses recorded."
            )


class TextGenerator:
    """Generates random text with exact token counts using a tokenizer."""

    def __init__(self, tokenizer: "PreTrainedTokenizerBase"):
        self._tokenizer = tokenizer
        self._vocab_size = tokenizer.vocab_size
        logger.info(
            "TextGenerator using tokenizer (vocab_size=%d) for exact token counts.",
            self._vocab_size,
        )

    def generate(self, num_tokens: int) -> str:
        if num_tokens <= 0:
            return ""
        return self._generate_exact(num_tokens)

    def generate_token_ids(self, num_tokens: int) -> list[int]:
        if num_tokens <= 0:
            return []
        return np.random.randint(0, self._vocab_size, size=num_tokens).tolist()

    def _generate_exact(self, target_tokens: int) -> str:
        tokenizer = self._tokenizer
        token_ids = np.random.randint(
            0, self._vocab_size, size=target_tokens + 20
        ).tolist()

        text = tokenizer.decode(token_ids, skip_special_tokens=True)
        actual_ids = tokenizer.encode(text, add_special_tokens=False)
        actual_len = len(actual_ids)

        if actual_len == target_tokens:
            return text

        if actual_len > target_tokens:
            trimmed_ids = actual_ids[:target_tokens]
            text = tokenizer.decode(trimmed_ids, skip_special_tokens=True)
            final_len = len(tokenizer.encode(text, add_special_tokens=False))
            if final_len != target_tokens:
                text = self._binary_search_trim(actual_ids, target_tokens)
            return text

        deficit = target_tokens - actual_len
        extra_ids = np.random.randint(0, self._vocab_size, size=deficit + 20).tolist()
        extra_text = tokenizer.decode(extra_ids, skip_special_tokens=True)
        combined = text + " " + extra_text
        combined_ids = tokenizer.encode(combined, add_special_tokens=False)

        if len(combined_ids) >= target_tokens:
            trimmed = combined_ids[:target_tokens]
            text = tokenizer.decode(trimmed, skip_special_tokens=True)
            final_len = len(tokenizer.encode(text, add_special_tokens=False))
            if final_len != target_tokens:
                text = self._binary_search_trim(combined_ids, target_tokens)
            return text

        while len(tokenizer.encode(combined, add_special_tokens=False)) < target_tokens:
            combined += " hello"
        combined_ids = tokenizer.encode(combined, add_special_tokens=False)
        return self._binary_search_trim(combined_ids, target_tokens)

    def _binary_search_trim(self, token_ids: list[int], target: int) -> str:
        tokenizer = self._tokenizer
        lo, hi = target, len(token_ids)
        best_text = tokenizer.decode(token_ids[:target], skip_special_tokens=True)

        while lo <= hi:
            mid = (lo + hi) // 2
            text = tokenizer.decode(token_ids[:mid], skip_special_tokens=True)
            actual = len(tokenizer.encode(text, add_special_tokens=False))
            if actual == target:
                return text
            elif actual < target:
                lo = mid + 1
            else:
                hi = mid - 1
                best_text = text

        for n in range(target, len(token_ids) + 1):
            text = tokenizer.decode(token_ids[:n], skip_special_tokens=True)
            if len(tokenizer.encode(text, add_special_tokens=False)) == target:
                return text
        return best_text


def conversation_factory(
    session_idx: int,
    spec: WorkloadSpec,
    shared_system_text: str,
    text_gen: Optional[TextGenerator],
) -> Conversation:
    """Create a single conversation on-demand (lazy generation)."""
    session_id = f"session-{session_idx:06d}"

    if spec.unique_s > 0 and text_gen is not None:
        unique_text = text_gen.generate(spec.unique_s)
        system_prompt = shared_system_text + " " + unique_text
    else:
        system_prompt = shared_system_text

    user_messages = (
        [text_gen.generate(spec.user_tokens) for _ in range(spec.num_turns)]
        if text_gen is not None
        else ["" for _ in range(spec.num_turns)]
    )

    return Conversation(
        session_id=session_id,
        system_prompt=system_prompt,
        user_messages=user_messages,
        num_turns=spec.num_turns,
    )
