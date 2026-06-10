import sys
from types import SimpleNamespace

import pytest
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.sampling_params import RequestOutputKind, SamplingParams

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_tracking import (
    enable_token_tracking,
)
from ray.llm._internal.serve.routing_policies.kv_aware.utils import (
    is_kv_aware_routing,
)
from ray.llm.tests.serve.mocks.mock_vllm_engine import MockAsyncLLM
from ray.serve.llm.request_router import KVAwareRouter

REPLICA_UNIQUE_ID = "test-replica-uid"
WORKER_ID = get_worker_id(REPLICA_UNIQUE_ID)
# A pre-tokenized prompt, as vLLM's serving layer always passes to generate.
PROMPT = {"prompt_token_ids": [1, 2, 3]}


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(address="auto")


def request_output(token_counts, prompt_len=5, finished=False):
    """A real vLLM RequestOutput: one CompletionOutput per entry in token_counts."""
    return RequestOutput(
        request_id="r",
        prompt=None,
        prompt_token_ids=list(range(prompt_len)),
        prompt_logprobs=None,
        outputs=[
            CompletionOutput(
                index=i,
                text="",
                token_ids=list(range(n)),
                cumulative_logprob=None,
                logprobs=None,
            )
            for i, n in enumerate(token_counts)
        ],
        finished=finished,
    )


def delta_steps(num_tokens, prompt_len=5):
    """A DELTA-kind stream: one new token per step, last step finished."""
    return [
        request_output([1], prompt_len=prompt_len, finished=i == num_tokens - 1)
        for i in range(num_tokens)
    ]


@ray.remote(num_cpus=0)
class RecordingKVRouterActor(KVRouterActor):
    """KVRouterActor that records the lifecycle events it receives for tests."""

    def __init__(self, block_size):
        self._block_size = block_size
        self._event_log = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        await super().on_lifecycle_events(events)

    def get_event_log(self):
        return self._event_log


@ray.remote(num_cpus=0)
class RaisingActor:
    """A KV-router stand-in whose event ingest always raises, to prove the
    engine token stream is never disrupted."""

    async def on_lifecycle_events(self, events):
        raise RuntimeError("actor down")


@pytest.fixture
def build_token_tracking_engine(monkeypatch):
    def _build(script, actor, **engine_kwargs):
        def get_deployment_actor(name):
            assert name == KV_ROUTER_ACTOR_NAME
            return actor

        monkeypatch.setattr(serve, "get_deployment_actor", get_deployment_actor)
        monkeypatch.setattr(
            serve,
            "get_replica_context",
            lambda: SimpleNamespace(
                replica_id=SimpleNamespace(unique_id=REPLICA_UNIQUE_ID)
            ),
        )
        return enable_token_tracking(MockAsyncLLM)(script, **engine_kwargs)

    return _build


async def consume(stream, limit=None):
    """Drain ``stream``, optionally closing it early after ``limit`` outputs."""
    outputs = []
    async for output in stream:
        outputs.append(output)
        if limit is not None and len(outputs) == limit:
            await stream.aclose()
    return outputs


async def drain(engine):
    """Wait for the engine forwarder's queued lifecycle batches to land."""
    await engine._lifecycle_forwarder.flush()


def decode_counts(events):
    return [args[1] for name, args in events if name == "on_decode_progress"]


@pytest.mark.asyncio
async def test_basic_lifecycle(build_token_tracking_engine):
    """A streamed request reports add -> prefill -> exact decode counts -> done."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(3, prompt_len=10), actor)

    prompt = {"prompt_token_ids": list(range(10))}
    outputs = await consume(
        engine.generate(
            prompt, SamplingParams(output_kind=RequestOutputKind.DELTA), "req-1"
        )
    )
    await drain(engine)

    assert ray.get(actor.get_event_log.remote()) == [
        ("on_request_added", ("req-1", WORKER_ID, list(range(10)))),
        ("on_prefill_complete", ("req-1",)),
        ("on_decode_progress", ("req-1", 1)),
        ("on_decode_progress", ("req-1", 2)),
        ("on_decode_progress", ("req-1", 3)),
        ("on_request_completed", ("req-1",)),
    ]
    assert outputs == engine.script


@pytest.mark.asyncio
async def test_in_order_reports(build_token_tracking_engine):
    """Back-to-back reports reach the actor in submission order."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(200), actor)

    await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert events[0][0] == "on_request_added"
    assert events[1][0] == "on_prefill_complete"
    assert events[-1] == ("on_request_completed", ("r",))
    assert decode_counts(events) == list(range(1, 201))


@pytest.mark.asyncio
async def test_streaming_accumulates_decode_progress(build_token_tracking_engine):
    """A DELTA (streaming) request sums each step's new tokens into a running
    total reported as decode progress."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    # Steps carry only new tokens: 1, then 2, then 1 -> cumulative 1, 3, 4.
    script = [request_output([n]) for n in (1, 2, 1)]
    engine = build_token_tracking_engine(script, actor)

    await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)

    assert decode_counts(ray.get(actor.get_event_log.remote())) == [1, 3, 4]


@pytest.mark.asyncio
async def test_non_streaming_reports_full_output_once(build_token_tracking_engine):
    """A FINAL_ONLY (non-streaming) request arrives as one finished chunk, with a
    CompletionOutput per candidate, so progress is reported once at the summed
    token count across candidates."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    # FINAL_ONLY n=3: a single finished chunk carrying every candidate's output.
    script = [request_output([2, 3, 4], finished=True)]
    engine = build_token_tracking_engine(script, actor)

    await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.FINAL_ONLY), "r"
        )
    )
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert decode_counts(events) == [9]  # 2 + 3 + 4 summed across candidates
    assert [name for name, _ in events] == [
        "on_request_added",
        "on_prefill_complete",
        "on_decode_progress",
        "on_request_completed",
    ]


@pytest.mark.asyncio
async def test_empty_steps_ignored(build_token_tracking_engine):
    """Token-less outputs (e.g. a finish-only chunk) emit no progress hooks."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    script = [
        request_output([1]),
        request_output([0]),  # structural chunk: no new tokens
        request_output([1], finished=True),
    ]
    engine = build_token_tracking_engine(script, actor)

    await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert decode_counts(events) == [1, 2]
    assert [e for e in events if e[0] == "on_prefill_complete"] == [
        ("on_prefill_complete", ("r",))
    ]


@pytest.mark.asyncio
@pytest.mark.parametrize("early_drop", [False, True])
async def test_completed_exactly_once(early_drop, build_token_tracking_engine):
    """Completion fires exactly once on normal end and on early stream close."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(3), actor)

    stream = engine.generate(
        PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
    )
    await consume(stream, limit=1 if early_drop else None)
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert [e for e in events if e[0] == "on_request_completed"] == [
        ("on_request_completed", ("r",))
    ]


@pytest.mark.asyncio
async def test_engine_error_still_completes(build_token_tracking_engine):
    """A mid-stream engine error propagates but still frees the request."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(3), actor, error_after=1)

    with pytest.raises(RuntimeError, match="engine failure"):
        await consume(
            engine.generate(
                PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
            )
        )
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert events[-1] == ("on_request_completed", ("r",))


@pytest.mark.asyncio
async def test_zero_token_request(build_token_tracking_engine):
    """An output-less request (e.g. validation abort) is added and freed only."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine([request_output([0], finished=True)], actor)

    prompt = {"prompt_token_ids": [1, 2, 3]}
    await consume(
        engine.generate(
            prompt, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)

    assert ray.get(actor.get_event_log.remote()) == [
        ("on_request_added", ("r", WORKER_ID, [1, 2, 3])),
        ("on_request_completed", ("r",)),
    ]


@pytest.mark.asyncio
async def test_actor_failure_isolation(build_token_tracking_engine):
    """A failing actor never disrupts the engine's output stream."""
    engine = build_token_tracking_engine(delta_steps(2), RaisingActor.remote())

    outputs = await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)  # the failed batches are dropped without raising

    assert len(outputs) == 2


def test_decorator_returns_subclass():
    """The decorator returns an isinstance-compatible subclass."""
    assert issubclass(enable_token_tracking(MockAsyncLLM), MockAsyncLLM)


@pytest.mark.asyncio
async def test_passthrough_without_actor(monkeypatch):
    """Outside a replica (no actor resolvable) the engine is a pure pass-through."""

    def _raise(name):
        raise RuntimeError("no actor")

    monkeypatch.setattr(serve, "get_deployment_actor", _raise)
    engine = enable_token_tracking(MockAsyncLLM)(delta_steps(2))

    outputs = await consume(
        engine.generate(
            PROMPT, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )

    assert len(outputs) == 2
    assert engine._lifecycle_forwarder is None  # resolution failed; retried next call


@pytest.mark.parametrize(
    "request_router_config, expected",
    [
        ({"request_router_class": KVAwareRouter}, True),
        ({}, False),  # default (non-KV) router
        (None, False),  # no router configured
    ],
)
def test_is_kv_aware_routing(request_router_config, expected):
    """The engine wraps with token tracking only on KVAwareRouter deployments,
    so non-KV deployments never retry a missing actor lookup per request."""
    assert is_kv_aware_routing(request_router_config) is expected


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
