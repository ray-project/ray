"""Tests for benchmark engine components: TextGenerator, Conversation, conversation_factory, BenchmarkState."""

import asyncio
import sys

import numpy as np
import pytest
from transformers import AutoTokenizer

from ray.llm._internal.serve.benchmark.multiturn_bench import (
    BenchmarkState,
    Conversation,
    TextGenerator,
    TurnMetric,
    WorkloadSpec,
    conversation_factory,
)

# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture(scope="module")
def tokenizer():
    return AutoTokenizer.from_pretrained("gpt2")


@pytest.fixture(scope="module")
def text_gen(tokenizer):
    return TextGenerator(tokenizer)


def _make_spec(**overrides) -> WorkloadSpec:
    """Create a resolved WorkloadSpec with sensible defaults."""
    defaults = dict(
        isl=1000,
        hit_rate=0.7,
        num_turns=3,
        osl=50,
        shared_system_prompt_ratio=0.5,
        concurrency=4,
        num_sessions=10,
    )
    defaults.update(overrides)
    return WorkloadSpec(**defaults).resolve()


# ============================================================================
# TextGenerator
# ============================================================================


class TestTextGenerator:
    def test_generate_exact_token_count(self, text_gen, tokenizer):
        """Generated text should tokenize to exactly the requested count."""
        np.random.seed(42)
        for target in [1, 10, 50, 200]:
            text = text_gen.generate(target)
            actual = len(tokenizer.encode(text, add_special_tokens=False))
            assert actual == target, f"Expected {target} tokens, got {actual}"

    def test_generate_zero_tokens(self, text_gen):
        assert text_gen.generate(0) == ""

    def test_generate_negative_tokens(self, text_gen):
        assert text_gen.generate(-5) == ""

    def test_generate_token_ids_length(self, text_gen):
        """generate_token_ids should return exactly N IDs."""
        np.random.seed(42)
        ids = text_gen.generate_token_ids(100)
        assert len(ids) == 100

    def test_generate_token_ids_in_vocab_range(self, text_gen, tokenizer):
        np.random.seed(42)
        ids = text_gen.generate_token_ids(500)
        assert all(0 <= i < tokenizer.vocab_size for i in ids)

    def test_generate_token_ids_zero(self, text_gen):
        assert text_gen.generate_token_ids(0) == []

    def test_generate_token_ids_negative(self, text_gen):
        assert text_gen.generate_token_ids(-1) == []


# ============================================================================
# Conversation
# ============================================================================


class TestConversation:
    def test_turn_zero_messages(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="You are helpful.",
            user_messages=["Hello", "Follow up"],
            num_turns=2,
        )
        msgs = conv.get_turn_messages(0)
        assert len(msgs) == 2
        assert msgs[0] == {"role": "system", "content": "You are helpful."}
        assert msgs[1] == {"role": "user", "content": "Hello"}

    def test_turn_one_with_injected_response(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="sys",
            user_messages=["u0", "u1"],
            num_turns=2,
        )
        conv.inject_assistant_response(0, "a0")
        msgs = conv.get_turn_messages(1)
        assert len(msgs) == 4
        assert msgs[0] == {"role": "system", "content": "sys"}
        assert msgs[1] == {"role": "user", "content": "u0"}
        assert msgs[2] == {"role": "assistant", "content": "a0"}
        assert msgs[3] == {"role": "user", "content": "u1"}

    def test_turn_one_without_response_uses_placeholder(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="sys",
            user_messages=["u0", "u1"],
            num_turns=2,
        )
        msgs = conv.get_turn_messages(1)
        assert msgs[2] == {"role": "assistant", "content": "(placeholder)"}

    def test_no_system_prompt(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="",
            user_messages=["u0"],
            num_turns=1,
        )
        msgs = conv.get_turn_messages(0)
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    def test_inject_sequential(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="sys",
            user_messages=["u0", "u1", "u2"],
            num_turns=3,
        )
        conv.inject_assistant_response(0, "a0")
        conv.inject_assistant_response(1, "a1")
        msgs = conv.get_turn_messages(2)
        roles = [m["role"] for m in msgs]
        assert roles == ["system", "user", "assistant", "user", "assistant", "user"]

    def test_inject_overwrite(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="sys",
            user_messages=["u0"],
            num_turns=1,
        )
        conv.inject_assistant_response(0, "first")
        conv.inject_assistant_response(0, "second")
        assert conv._assistant_responses[0] == "second"

    def test_inject_out_of_order_raises(self):
        conv = Conversation(
            session_id="s0",
            system_prompt="sys",
            user_messages=["u0", "u1", "u2"],
            num_turns=3,
        )
        with pytest.raises(ValueError, match="Cannot inject"):
            conv.inject_assistant_response(2, "skipped turn 0 and 1")


# ============================================================================
# conversation_factory
# ============================================================================


class TestConversationFactory:
    def test_creates_valid_conversation(self, text_gen):
        np.random.seed(42)
        spec = _make_spec()
        shared_text = text_gen.generate(spec.shared_s) if spec.shared_s > 0 else ""
        conv = conversation_factory(0, spec, shared_text, text_gen)

        assert conv.session_id == "session-000000"
        assert conv.num_turns == spec.num_turns
        assert len(conv.user_messages) == spec.num_turns

    def test_user_messages_have_correct_token_count(self, text_gen, tokenizer):
        np.random.seed(42)
        spec = _make_spec()
        shared_text = text_gen.generate(spec.shared_s) if spec.shared_s > 0 else ""
        conv = conversation_factory(0, spec, shared_text, text_gen)

        for msg in conv.user_messages:
            token_count = len(tokenizer.encode(msg, add_special_tokens=False))
            assert (
                token_count == spec.user_tokens
            ), f"Expected {spec.user_tokens} tokens, got {token_count}"

    def test_cross_session_shared_prefix(self, text_gen):
        """Two sessions with cross_sharing > 0 should share the same prefix."""
        np.random.seed(42)
        spec = _make_spec(shared_system_prompt_ratio=0.8)
        shared_text = text_gen.generate(spec.shared_s) if spec.shared_s > 0 else ""

        conv0 = conversation_factory(0, spec, shared_text, text_gen)
        conv1 = conversation_factory(1, spec, shared_text, text_gen)

        assert conv0.system_prompt.startswith(shared_text)
        assert conv1.system_prompt.startswith(shared_text)

    def test_zero_cross_sharing_no_shared_prefix(self, text_gen):
        np.random.seed(42)
        spec = _make_spec(shared_system_prompt_ratio=0.0, isl=2000, hit_rate=0.6)
        assert spec.shared_s == 0

    def test_session_id_format(self, text_gen):
        np.random.seed(42)
        spec = _make_spec()
        shared_text = text_gen.generate(spec.shared_s) if spec.shared_s > 0 else ""
        conv = conversation_factory(42, spec, shared_text, text_gen)
        assert conv.session_id == "session-000042"


# ============================================================================
# BenchmarkState
# ============================================================================


class TestBenchmarkState:
    def _run(self, coro):
        return asyncio.get_event_loop().run_until_complete(coro)

    @pytest.fixture(autouse=True)
    def _setup_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        yield
        loop.close()

    def test_get_next_session_increments(self):
        spec = _make_spec(num_sessions=5)
        state = BenchmarkState(spec)
        ids = [self._run(state.get_next_session()) for _ in range(5)]
        assert ids == [0, 1, 2, 3, 4]

    def test_get_next_session_exhausted(self):
        spec = _make_spec(num_sessions=2)
        state = BenchmarkState(spec)
        self._run(state.get_next_session())
        self._run(state.get_next_session())
        assert self._run(state.get_next_session()) is None

    def test_get_next_session_unlimited(self):
        spec = _make_spec(
            num_sessions=None, duration_s=60.0, request_rate=1.0, concurrency=None
        )
        state = BenchmarkState(spec)
        ids = [self._run(state.get_next_session()) for _ in range(100)]
        assert ids == list(range(100))

    def test_record_metric_during_warmup(self):
        spec = _make_spec()
        state = BenchmarkState(spec)
        metric = TurnMetric(
            session_id="s0",
            turn=0,
            ttft_ms=1.0,
            fc_ms=1.0,
            tpot_ms=1.0,
            latency_ms=10.0,
            input_tokens=100,
            output_tokens=50,
            start_time_ms=0.0,
        )
        self._run(state.record_metric(metric))
        assert len(state.warmup_metrics) == 1
        assert len(state.measured_metrics) == 0

    def test_record_metric_after_warmup(self):
        spec = _make_spec()
        state = BenchmarkState(spec)
        self._run(state.mark_warmup_complete())

        metric = TurnMetric(
            session_id="s0",
            turn=0,
            ttft_ms=1.0,
            fc_ms=1.0,
            tpot_ms=1.0,
            latency_ms=10.0,
            input_tokens=100,
            output_tokens=50,
            start_time_ms=0.0,
        )
        self._run(state.record_metric(metric))
        assert len(state.warmup_metrics) == 0
        assert len(state.measured_metrics) == 1

    def test_inflight_tracking(self):
        spec = _make_spec()
        state = BenchmarkState(spec)

        self._run(state.track_inflight_start())
        self._run(state.track_inflight_start())
        assert state.inflight == 2
        assert state.max_inflight == 2

        self._run(state.track_inflight_end())
        assert state.inflight == 1
        assert state.max_inflight == 2

    def test_mark_session_complete(self):
        spec = _make_spec()
        state = BenchmarkState(spec)
        state.active_turns["session-000000"] = 0
        state.conversations[0] = Conversation(
            session_id="session-000000",
            system_prompt="sys",
            user_messages=["u0"],
            num_turns=1,
        )

        self._run(state.mark_session_complete(0, "session-000000"))
        assert state.completed_sessions == 1
        assert "session-000000" not in state.active_turns
        assert 0 not in state.conversations

    def test_warmup_entropy_check_single_turn_spec(self):
        """With num_turns=1, max_entropy=0 so warmup completes immediately."""
        spec = _make_spec(num_turns=1, hit_rate=0.3)
        state = BenchmarkState(spec)
        state.active_turns["s0"] = 0
        result = self._run(state.check_and_complete_warmup())
        assert result is True
        assert state.warmup_complete is True

    def test_warmup_entropy_not_reached(self):
        """All sessions on same turn -> entropy=0 -> warmup not complete."""
        spec = _make_spec(num_turns=5)
        state = BenchmarkState(spec)
        for i in range(10):
            state.active_turns[f"s{i}"] = 0
        result = self._run(state.check_and_complete_warmup())
        assert result is False
        assert state.warmup_complete is False

    def test_warmup_entropy_reached(self):
        """Spread sessions across all turns -> high entropy -> warmup complete."""
        spec = _make_spec(num_turns=4)
        state = BenchmarkState(spec)
        for i in range(20):
            state.active_turns[f"s{i}"] = i % 4
        result = self._run(state.check_and_complete_warmup())
        assert result is True
        assert state.warmup_complete is True

    def test_has_remaining_sessions_with_limit(self):
        spec = _make_spec(num_sessions=3)
        state = BenchmarkState(spec)
        assert self._run(state.has_remaining_sessions()) is True
        self._run(state.get_next_session())
        self._run(state.get_next_session())
        assert self._run(state.has_remaining_sessions()) is True
        self._run(state.get_next_session())
        assert self._run(state.has_remaining_sessions()) is False

    def test_has_remaining_sessions_unlimited(self):
        spec = _make_spec(
            num_sessions=None, duration_s=60.0, request_rate=1.0, concurrency=None
        )
        state = BenchmarkState(spec)
        for _ in range(100):
            self._run(state.get_next_session())
        assert self._run(state.has_remaining_sessions()) is True

    def test_get_inflight(self):
        spec = _make_spec()
        state = BenchmarkState(spec)
        assert self._run(state.get_inflight()) == 0
        self._run(state.track_inflight_start())
        self._run(state.track_inflight_start())
        assert self._run(state.get_inflight()) == 2
        self._run(state.track_inflight_end())
        assert self._run(state.get_inflight()) == 1

    def test_get_stats(self):
        spec = _make_spec()
        state = BenchmarkState(spec)
        stats = self._run(state.get_stats())
        assert "warmup_metrics" in stats
        assert "measured_metrics" in stats
        assert "completed_sessions" in stats
        assert "max_inflight" in stats
        assert "warmup_complete" in stats
        assert stats["warmup_complete"] is False


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
