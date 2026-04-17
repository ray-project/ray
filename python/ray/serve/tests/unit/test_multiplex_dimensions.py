import pytest

from ray.serve._private.common import parse_multiplex_ids_from_headers
from ray.serve.context import _RequestContext


class TestParseMultiplexIdsFromHeaders:
    def _parse(self, headers):
        # Callers pass (bytes, bytes) pairs (ASGI scope format).
        return parse_multiplex_ids_from_headers(
            [(k.encode(), v.encode()) for k, v in headers]
        )

    def test_generalized_pattern_multiple_dimensions(self):
        assert self._parse(
            [
                ("serve_multiplexed_model_id", "lora_v2"),
                ("serve_multiplexed_session_id", "user_123"),
            ]
        ) == {"model": "lora_v2", "session": "user_123"}

    def test_dash_form_normalized_to_underscore(self):
        """Fronting proxies (nginx, AWS API GW) sometimes turn `_` → `-` in
        header names. Normalization collapses both to the same key."""
        assert self._parse(
            [
                ("Serve-Multiplexed-Model-Id", "m1"),
                ("X-Session-Id", "s1"),
            ]
        ) == {"model": "m1", "session": "s1"}

    def test_x_session_id_shortcut(self):
        assert self._parse([("x-session-id", "user_abc")]) == {"session": "user_abc"}

    def test_explicit_session_dim_wins_over_x_session_id(self):
        """If both the generalized `serve_multiplexed_session_id` and the
        shortcut `x-session-id` are present, the explicit dimension wins."""
        assert self._parse(
            [
                ("serve_multiplexed_session_id", "explicit"),
                ("x-session-id", "fallback"),
            ]
        ) == {"session": "explicit"}


class TestBatchSplitByMultiplexIds:
    class _FakeRequest:
        def __init__(self, multiplex_ids):
            self.request_context = _RequestContext(multiplex_ids=multiplex_ids)

    def _split(self, batch):
        from ray.serve.batching import _BatchQueue

        bq = _BatchQueue.__new__(_BatchQueue)
        return bq._split_batch_by_multiplex_ids(batch)

    def test_identical_multiplex_ids(self):
        batch = [
            self._FakeRequest({"model": "m1", "session": "s1"}),
            self._FakeRequest({"model": "m1", "session": "s1"}),
            self._FakeRequest({"model": "m1", "session": "s2"}),
            self._FakeRequest({"model": "m2", "session": "s1"}),
        ]
        assert len(self._split(batch)) == 3

    def test_different_dimension_subsets(self):
        req_a = self._FakeRequest({"model": "lora1", "session": "2"})
        req_b = self._FakeRequest({"model": "lora1"})
        req_c = self._FakeRequest({"session": "3"})

        sub_batches = self._split([req_a, req_b, req_c])
        assert len(sub_batches) == 3
        by_ids = {
            frozenset(sb[0].request_context.multiplex_ids.items()): sb
            for sb in sub_batches
        }
        assert by_ids[frozenset({("model", "lora1"), ("session", "2")})] == [req_a]
        assert by_ids[frozenset({("model", "lora1")})] == [req_b]
        assert by_ids[frozenset({("session", "3")})] == [req_c]


if __name__ == "__main__":
    import sys

    sys.exit(pytest.main([__file__, "-v"]))
