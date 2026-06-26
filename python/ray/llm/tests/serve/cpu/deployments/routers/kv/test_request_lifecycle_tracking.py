import sys
from dataclasses import asdict
from types import SimpleNamespace

import pytest
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.sampling_params import RequestOutputKind, SamplingParams

import ray
import ray.cloudpickle
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

# Cluster workers can't import this pytest module, so ship the actor classes
# defined here (and their MockSelectionService) to actors by value, not by name.
ray.cloudpickle.register_pickle_by_value(sys.modules[__name__])

REPLICA_UNIQUE_ID = "test-replica-uid"
WORKER_ID = get_worker_id(REPLICA_UNIQUE_ID)
# A pre-tokenized prompt, as vLLM's serving layer always passes to generate.
PROMPT = {"prompt_token_ids": [1, 2, 3]}
# SamplingParams.max_tokens the engine reports as the request's expected output.
MAX_TOKENS = 20


def sampling(kind=RequestOutputKind.DELTA, max_tokens=MAX_TOKENS):
    return SamplingParams(output_kind=kind, max_tokens=max_tokens)


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


class MockSelectionService:
    """Records the selection-service reservation calls the actor's lifecycle
    hooks make, standing in for the Dynamo selection service. ``add_output_block``
    is synchronous as in the real binding; the rest are async."""

    def __init__(self):
        self.calls = []
        self.reservations = []

    async def create_reservation(self, request):
        self.reservations.append(dict(request))
        self.calls.append(
            (
                "create_reservation",
                request["reservation_id"],
                request["worker_id"],
                len(request["token_ids"]),
                request.get("expected_output_tokens"),
            )
        )

    async def prefill_complete(self, reservation_id):
        self.calls.append(("prefill_complete", reservation_id))

    def add_output_block(self, reservation_id, *, decay_fraction=None):
        self.calls.append(("add_output_block", reservation_id, decay_fraction))

    async def free_reservation(self, reservation_id):
        self.calls.append(("free_reservation", reservation_id))


@ray.remote(num_cpus=0)
class RecordingKVRouterActor(KVRouterActor):
    """In-process KVRouterActor with the event plane + Serve LongPoll stripped,
    recording the events it applies and the reservation calls it books into the
    (fake) selection service."""

    def __init__(self, block_size):
        self._block_size = block_size
        self._replica_id_by_worker = {}
        self._requests = {}
        self._effective_prefill_tokens_by_request = {}
        self._pending_tasks = set()
        self._svc = MockSelectionService()
        self._event_log = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        await super().on_lifecycle_events(events)

    def get_event_log(self):
        return self._event_log

    def get_dynamo_calls(self):
        return self._svc.calls

    async def get_request_lifecycle(self, request_id):
        """Return a snapshot of an in-flight request's state, or ``None``."""
        state = self._requests.get(request_id)
        return None if state is None else asdict(state)

    async def get_active_request_ids(self):
        """Return ids of the in-flight requests."""
        return list(self._requests)

    async def get_worker_active_load(self, worker_id):
        """Return the number of in-flight requests attributed to ``worker_id``."""
        return sum(1 for s in self._requests.values() if s.worker_id == worker_id)


@ray.remote(num_cpus=0)
class RaisingActor:
    """A KV-router stand-in whose event ingest always raises, to prove the
    engine token stream is never disrupted."""

    async def on_lifecycle_events(self, events):
        raise RuntimeError("actor down")


class LocalKVRouterActor(KVRouterActor):
    """In-process KVRouterActor with the event plane + Serve LongPoll stripped."""

    def __init__(self, block_size):
        self._block_size = block_size
        self._replica_id_by_worker = {}
        self._requests = {}
        self._effective_prefill_tokens_by_request = {}
        self._pending_tasks = set()
        self._svc = MockSelectionService()

    async def get_request_lifecycle(self, request_id):
        """Return a snapshot of an in-flight request's state, or ``None``."""
        state = self._requests.get(request_id)
        return None if state is None else asdict(state)

    async def get_active_request_ids(self):
        """Return ids of the in-flight requests."""
        return list(self._requests)

    async def get_worker_active_load(self, worker_id):
        """Return the number of in-flight requests attributed to ``worker_id``."""
        return sum(1 for s in self._requests.values() if s.worker_id == worker_id)


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


def op_names(calls):
    return [c[0] for c in calls]


@pytest.mark.asyncio
async def test_basic_lifecycle(build_token_tracking_engine):
    """A streamed request reports add -> prefill -> exact decode counts -> done."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(3, prompt_len=10), actor)

    prompt = {"prompt_token_ids": list(range(10))}
    outputs = await consume(engine.generate(prompt, sampling(), "req-1"))
    await drain(engine)

    assert ray.get(actor.get_event_log.remote()) == [
        ("on_request_added", ("req-1", WORKER_ID, list(range(10)), MAX_TOKENS)),
        ("on_prefill_complete", ("req-1",)),
        ("on_decode_progress", ("req-1", 1)),
        ("on_decode_progress", ("req-1", 2)),
        ("on_decode_progress", ("req-1", 3)),
        ("on_request_completed", ("req-1",)),
    ]
    assert outputs == engine.script


@pytest.mark.asyncio
async def test_lifecycle_uses_serve_request_id(build_token_tracking_engine):
    """Lifecycle events use the same Serve request id used by routing, even if
    vLLM's engine-level id is different."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(1, prompt_len=10), actor)

    prompt = {"prompt_token_ids": list(range(10))}
    serve.context._serve_request_context.set(
        serve.context._RequestContext(request_id="serve-route-id")
    )
    try:
        await consume(
            engine.generate(
                prompt,
                sampling(),
                "chatcmpl-serve-route-id",
            )
        )
    finally:
        serve.context._serve_request_context.set(serve.context._RequestContext())
    await drain(engine)

    assert ray.get(actor.get_event_log.remote()) == [
        (
            "on_request_added",
            ("serve-route-id", WORKER_ID, list(range(10)), MAX_TOKENS),
        ),
        ("on_prefill_complete", ("serve-route-id",)),
        ("on_decode_progress", ("serve-route-id", 1)),
        ("on_request_completed", ("serve-route-id",)),
    ]


@pytest.mark.asyncio
async def test_in_order_reports(build_token_tracking_engine):
    """Back-to-back reports reach the actor in submission order."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(200), actor)

    await consume(engine.generate(PROMPT, sampling(max_tokens=200), "r"))
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

    await consume(engine.generate(PROMPT, sampling(), "r"))
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
        engine.generate(PROMPT, sampling(kind=RequestOutputKind.FINAL_ONLY), "r")
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

    await consume(engine.generate(PROMPT, sampling(), "r"))
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

    stream = engine.generate(PROMPT, sampling(), "r")
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
        await consume(engine.generate(PROMPT, sampling(), "r"))
    await drain(engine)

    events = ray.get(actor.get_event_log.remote())
    assert events[-1] == ("on_request_completed", ("r",))


@pytest.mark.asyncio
async def test_zero_token_request(build_token_tracking_engine):
    """An output-less request (e.g. validation abort) is added and freed only."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine([request_output([0], finished=True)], actor)

    prompt = {"prompt_token_ids": [1, 2, 3]}
    await consume(engine.generate(prompt, sampling(), "r"))
    await drain(engine)

    assert ray.get(actor.get_event_log.remote()) == [
        ("on_request_added", ("r", WORKER_ID, [1, 2, 3], MAX_TOKENS)),
        ("on_request_completed", ("r",)),
    ]


@pytest.mark.asyncio
async def test_actor_failure_isolation(build_token_tracking_engine):
    """A failing actor never disrupts the engine's output stream."""
    engine = build_token_tracking_engine(delta_steps(2), RaisingActor.remote())

    outputs = await consume(engine.generate(PROMPT, sampling(), "r"))
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

    outputs = await consume(engine.generate(PROMPT, sampling(), "r"))

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


@pytest.mark.asyncio
async def test_block_boundary_crossings():
    """Each ceil((prompt+output)/block_size) increase advances total_blocks."""
    actor = LocalKVRouterActor(block_size=16)
    await actor.on_request_added("r", 1, list(range(10)))
    assert (await actor.get_request_lifecycle("r"))["total_blocks"] == 1  # ceil(10/16)

    await actor.on_decode_progress("r", 6)  # 10+6=16 -> still 1 block
    assert (await actor.get_request_lifecycle("r"))["total_blocks"] == 1

    await actor.on_decode_progress("r", 7)  # 17 -> crosses into block 2
    assert (await actor.get_request_lifecycle("r"))["total_blocks"] == 2

    await actor.on_decode_progress("r", 39)  # 49 -> ceil=4, crosses two more at once
    snapshot = await actor.get_request_lifecycle("r")
    assert snapshot["total_blocks"] == 4
    assert snapshot["output_tokens"] == 39


@pytest.mark.asyncio
async def test_active_load_tracking():
    """Active load is per-worker; completion evicts the request entirely."""
    actor = LocalKVRouterActor(block_size=16)
    await actor.on_request_added("a", 1, list(range(8)))
    await actor.on_request_added("b", 1, [])
    await actor.on_request_added("c", 2, [])
    assert await actor.get_worker_active_load(1) == 2
    assert await actor.get_worker_active_load(2) == 1

    await actor.on_prefill_complete("a")
    await actor.on_decode_progress("a", 5)
    assert await actor.get_worker_active_load(1) == 2  # still active while decoding
    assert await actor.get_request_lifecycle("a") == {
        "worker_id": 1,
        "prompt_tokens": 8,
        "expected_output_tokens": None,
        "prefill_completed": True,
        "output_tokens": 5,
        "total_blocks": 1,
    }

    # Completion evicts (bounding memory to in-flight requests).
    await actor.on_request_completed("a")
    assert await actor.get_worker_active_load(1) == 1
    assert set(await actor.get_active_request_ids()) == {"b", "c"}
    assert await actor.get_request_lifecycle("a") is None

    # Hooks for an unknown request id are ignored.
    await actor.on_prefill_complete("missing")
    await actor.on_decode_progress("missing", 3)
    await actor.on_request_completed("missing")
    assert await actor.get_request_lifecycle("missing") is None


@pytest.mark.asyncio
async def test_books_effective_prefill_hint():
    """The route-time effective prefill hint is booked into Dynamo load state."""
    actor = LocalKVRouterActor(block_size=16)
    actor._effective_prefill_tokens_by_request["r"] = 7

    await actor.on_request_added("r", WORKER_ID, list(range(16)))

    assert actor._svc.reservations[0]["effective_prefill_tokens"] == 7


@pytest.mark.asyncio
async def test_tracks_streamed_request_state(build_token_tracking_engine):
    """End-to-end: exact token counts land as actor block state over ``.remote``."""
    actor = RecordingKVRouterActor.remote(block_size=8)
    # prompt 12, block_size 8: baseline ceil(12/8)=2; boundaries at cumulative
    # output 5 and 13 -> 9 generated tokens cross only the first.
    engine = build_token_tracking_engine(delta_steps(9, prompt_len=12), actor)

    prompt = {"prompt_token_ids": list(range(12))}
    stream = engine.generate(prompt, sampling(), "req-e2e")
    for _ in range(9):
        await stream.__anext__()
    await drain(engine)

    # All outputs consumed but the stream is still open: the request is
    # tracked with its exact final counts.
    assert await actor.get_request_lifecycle.remote("req-e2e") == {
        "worker_id": WORKER_ID,
        "prompt_tokens": 12,
        "expected_output_tokens": MAX_TOKENS,
        "prefill_completed": True,
        "output_tokens": 9,
        "total_blocks": 3,
    }
    assert await actor.get_worker_active_load.remote(WORKER_ID) == 1

    # Stream end fires completion, which evicts the request.
    with pytest.raises(StopAsyncIteration):
        await stream.__anext__()
    await drain(engine)
    assert await actor.get_request_lifecycle.remote("req-e2e") is None
    assert await actor.get_active_request_ids.remote() == []
    assert await actor.get_worker_active_load.remote(WORKER_ID) == 0


@pytest.mark.asyncio
async def test_lifecycle_books_selection_service_load(build_token_tracking_engine):
    """A streamed request books create_reservation -> prefill_complete -> one
    add_output_block per crossed decode block -> free_reservation, in order."""
    actor = RecordingKVRouterActor.remote(block_size=8)
    # prompt 12 (2 blocks); cumulative output crosses into block 3 at 5 tokens
    # and block 4 at 13 tokens -> exactly two output blocks over 20 tokens.
    engine = build_token_tracking_engine(delta_steps(20, prompt_len=12), actor)

    prompt = {"prompt_token_ids": list(range(12))}
    await consume(engine.generate(prompt, sampling(), "req-1"))
    await drain(engine)

    calls = ray.get(actor.get_dynamo_calls.remote())
    assert op_names(calls) == (
        ["create_reservation", "prefill_complete"]
        + ["add_output_block"] * 2
        + ["free_reservation"]
    )
    assert calls[0] == ("create_reservation", "req-1", WORKER_ID, 12, MAX_TOKENS)
    assert calls[-1] == ("free_reservation", "req-1")


@pytest.mark.asyncio
async def test_decode_blocks_book_add_output_block():
    """Each crossed decode block books one add_output_block in the service."""
    actor = LocalKVRouterActor(block_size=16)
    await actor.on_request_added("r", WORKER_ID, list(range(10)))  # 1 prompt block
    await actor.on_decode_progress("r", 6)  # 16 -> still 1 block
    await actor.on_decode_progress("r", 7)  # 17 -> crosses into block 2
    await actor.on_decode_progress("r", 39)  # 49 -> ceil=4, crosses two more

    assert (
        op_names(actor._svc.calls) == ["create_reservation"] + ["add_output_block"] * 3
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "expected_output_tokens, expected_decay",
    [
        pytest.param(40, pytest.approx(1.0 - 8 / 40), id="with-estimate"),
        pytest.param(None, None, id="no-estimate"),
    ],
)
async def test_expected_output_tokens_sets_decay_fraction(
    expected_output_tokens, expected_decay
):
    """With an output-length estimate each booked decode block decays by the
    remaining fraction; without one the block carries no decay."""
    actor = LocalKVRouterActor(block_size=8)
    await actor.on_request_added(
        "r", WORKER_ID, list(range(8)), expected_output_tokens=expected_output_tokens
    )
    await actor.on_decode_progress("r", 8)  # total 16 -> crosses into block 2

    block_calls = [c for c in actor._svc.calls if c[0] == "add_output_block"]
    assert block_calls == [("add_output_block", "r", expected_decay)]


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
