import sys
from types import SimpleNamespace

import pytest
from vllm.config import CacheConfig
from vllm.outputs import CompletionOutput, RequestOutput
from vllm.sampling_params import RequestOutputKind, SamplingParams

import ray
from ray import serve
from ray.llm._internal.serve.routing_policies.kv_aware.kv_aware_actor import (
    KV_ROUTER_ACTOR_NAME,
    KVRouterActor,
    get_worker_id,
)
from ray.llm._internal.serve.routing_policies.kv_aware.kv_event_plane import (
    derive_kv_event_block_size,
)
from ray.llm._internal.serve.routing_policies.kv_aware.token_tracking import (
    enable_token_tracking,
)

REPLICA_UNIQUE_ID = "test-replica-uid"
WORKER_ID = get_worker_id(REPLICA_UNIQUE_ID)


@pytest.fixture(scope="module", autouse=True)
def ray_cluster():
    if not ray.is_initialized():
        ray.init(address="auto")


def request_output(token_counts, prompt_len=4, finished=False):
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


def delta_steps(num_tokens, prompt_len=4):
    """A DELTA-kind stream: one new token per step, last step finished."""
    return [
        request_output([1], prompt_len=prompt_len, finished=i == num_tokens - 1)
        for i in range(num_tokens)
    ]


class MockAsyncLLM:
    """Mocks AsyncLLM: ``generate`` yields a scripted output stream.

    This is the one test seam on CPU -- the GPU integration test covers the
    real engine; everything downstream of ``generate`` runs the production
    path here.
    """

    def __init__(self, script, error_after=None):
        self.script = script
        self.error_after = error_after

    async def generate(self, prompt, sampling_params, request_id, **kwargs):
        for i, output in enumerate(self.script):
            if self.error_after is not None and i == self.error_after:
                raise RuntimeError("engine failure")
            yield output


class FakeKvRouter:
    """Records the Dynamo ``KvRouter`` lifecycle calls the actor makes.

    Stands in for the real pyo3 ``KvRouter`` so CPU tests can assert the actor
    books load into the right Dynamo hooks; ``overlap`` seeds a per-worker KV
    cache hit (in blocks) for the admission-time ``get_overlap_scores`` query.
    """

    def __init__(self, overlap=None):
        self.calls = []
        self._overlap = overlap or {}

    async def get_overlap_scores(self, token_ids):
        return {
            "workers": [
                {"worker_id": w, "device_blocks": b} for w, b in self._overlap.items()
            ]
        }

    async def add_request(
        self,
        request_id,
        token_ids,
        worker_id,
        dp_rank=0,
        cached_tokens=0,
        expected_output_tokens=None,
        **kwargs,
    ):
        self.calls.append(
            (
                "add_request",
                request_id,
                worker_id,
                len(token_ids),
                cached_tokens,
                expected_output_tokens,
            )
        )

    async def mark_prefill_complete(self, request_id):
        self.calls.append(("mark_prefill_complete", request_id))

    async def add_output_block(self, request_id, decay_fraction=None):
        self.calls.append(("add_output_block", request_id, decay_fraction))

    async def free(self, request_id):
        self.calls.append(("free", request_id))


class LifecycleActor(KVRouterActor.__ray_actor_class__):
    """In-process KVRouterActor with the event plane + Serve LongPoll stripped
    and a FakeKvRouter recording the Dynamo calls its lifecycle hooks make."""

    def __init__(self, block_size, overlap=None):
        self._block_size = block_size
        self._replica_id_by_worker = {}
        self._dyn_worker_id_to_replica_id = {}
        self._requests = {}
        self._kv_router = FakeKvRouter(overlap=overlap)

    def _start_replica_tracking(self):
        pass


@ray.remote(num_cpus=0)
class RecordingKVRouterActor(LifecycleActor):
    """``LifecycleActor`` as a Ray actor that also logs the reported events."""

    def __init__(self, block_size, overlap=None):
        super().__init__(block_size, overlap=overlap)
        self._event_log = []

    async def on_lifecycle_events(self, events):
        self._event_log.extend(events)
        await super().on_lifecycle_events(events)

    def get_event_log(self):
        return self._event_log

    def get_dynamo_calls(self):
        return self._kv_router.calls


@ray.remote(num_cpus=0)
class RaisingActor:
    """A KV-router stand-in whose event ingest always raises, to prove the
    engine stream is never disrupted."""

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
    """Wait for the engine reporter's queued lifecycle batches to land."""
    await engine._kv_reporter.flush()


def op_names(calls):
    return [c[0] for c in calls]


@pytest.mark.asyncio
async def test_lifecycle_books_dynamo_load(build_token_tracking_engine):
    """A streamed request books add_request -> mark_prefill_complete ->
    one add_output_block per crossed decode block -> free, in that order."""
    actor = RecordingKVRouterActor.remote(block_size=8)
    # prompt 12 (2 blocks); cumulative output crosses into block 3 at 5 tokens
    # and block 4 at 13 tokens -> exactly two output blocks over 20 tokens.
    engine = build_token_tracking_engine(delta_steps(20, prompt_len=12), actor)

    prompt = {"prompt_token_ids": list(range(12))}
    await consume(
        engine.generate(
            prompt, SamplingParams(output_kind=RequestOutputKind.DELTA), "req-1"
        )
    )
    await drain(engine)

    calls = ray.get(actor.get_dynamo_calls.remote())
    assert op_names(calls) == (
        ["add_request", "mark_prefill_complete"] + ["add_output_block"] * 2 + ["free"]
    )
    # No KV overlap configured -> the whole prompt counts as prefill work.
    assert calls[0] == ("add_request", "req-1", WORKER_ID, 12, 0, None)
    assert calls[-1] == ("free", "req-1")


@pytest.mark.asyncio
async def test_decode_block_crossings_drive_add_output_block():
    """Each ceil((prompt+output)/block_size) increase books one output block."""
    actor = LifecycleActor(block_size=16)
    await actor.on_request_added("r", WORKER_ID, list(range(10)))  # ceil(10/16)=1
    await actor.on_decode_progress("r", 6)  # 16 -> still 1 block
    await actor.on_decode_progress("r", 7)  # 17 -> crosses into block 2
    await actor.on_decode_progress("r", 39)  # 49 -> ceil=4, crosses two more

    snapshot = await actor.get_request_lifecycle("r")
    assert snapshot["output_blocks"] == 3
    assert snapshot["total_blocks"] == 4
    # Three crossings -> three add_output_block calls, after one add_request.
    assert (
        op_names(actor._kv_router.calls) == ["add_request"] + ["add_output_block"] * 3
    )


@pytest.mark.asyncio
async def test_add_request_subtracts_cached_overlap():
    """Admission books only the prefill work past the worker's cached prefix."""
    # The chosen worker already caches one 16-token block of the prompt.
    actor = LifecycleActor(block_size=16, overlap={WORKER_ID: 1})
    await actor.on_request_added("r", WORKER_ID, list(range(32)))

    name, rid, worker_id, n_tokens, cached_tokens, expected = actor._kv_router.calls[0]
    assert (name, rid, worker_id, n_tokens) == ("add_request", "r", WORKER_ID, 32)
    assert cached_tokens == 16  # one cached block * block_size


@pytest.mark.asyncio
async def test_expected_output_tokens_set_decay_fraction():
    """With an output-length estimate, each decode block decays by remaining
    fraction; without one the block carries no decay."""
    actor = LifecycleActor(block_size=8)
    await actor.on_request_added(
        "r", WORKER_ID, list(range(8)), expected_output_tokens=40
    )
    await actor.on_decode_progress("r", 8)  # total 16 -> crosses into block 2

    block_calls = [c for c in actor._kv_router.calls if c[0] == "add_output_block"]
    assert block_calls == [("add_output_block", "r", pytest.approx(1.0 - 8 / 40))]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "kind", [RequestOutputKind.DELTA, RequestOutputKind.CUMULATIVE]
)
async def test_token_count_normalization(kind, build_token_tracking_engine):
    """DELTA and CUMULATIVE streams book identical decode progress."""
    if kind is RequestOutputKind.DELTA:
        # New tokens per step: 1, 2, 1 -> cumulative 1, 3, 4.
        script = [request_output([n], prompt_len=4) for n in (1, 2, 1)]
    else:
        # Full output so far per step: 1, 3, 4.
        script = [request_output([n], prompt_len=4) for n in (1, 3, 4)]
    actor = RecordingKVRouterActor.remote(block_size=2)
    engine = build_token_tracking_engine(script, actor)

    prompt = {"prompt_token_ids": list(range(4))}
    await consume(engine.generate(prompt, SamplingParams(output_kind=kind), "r"))
    await drain(engine)

    # prompt 4 (2 blocks) at block_size 2; cumulative output 1,3,4 -> totals
    # 5,7,8 -> blocks 3,4,4: two crossings regardless of stream kind.
    calls = ray.get(actor.get_dynamo_calls.remote())
    assert op_names(calls) == [
        "add_request",
        "mark_prefill_complete",
        "add_output_block",
        "add_output_block",
        "free",
    ]


@pytest.mark.asyncio
async def test_in_order_reports(build_token_tracking_engine):
    """Back-to-back reports reach the actor in submission order.

    Plain fire-and-forget calls to an async actor would run out of order, which
    would book a completion before the admission and resurrect a freed request.
    """
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(200, prompt_len=8), actor)

    await consume(
        engine.generate("p", SamplingParams(output_kind=RequestOutputKind.DELTA), "r")
    )
    await drain(engine)

    names = op_names(ray.get(actor.get_dynamo_calls.remote()))
    assert names[0] == "add_request"
    assert names[1] == "mark_prefill_complete"
    assert names[-1] == "free"


@pytest.mark.asyncio
@pytest.mark.parametrize("early_drop", [False, True])
async def test_freed_exactly_once(early_drop, build_token_tracking_engine):
    """free fires exactly once on normal end and on early stream close."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(delta_steps(3, prompt_len=8), actor)

    stream = engine.generate(
        "p", SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
    )
    await consume(stream, limit=1 if early_drop else None)
    await drain(engine)

    calls = ray.get(actor.get_dynamo_calls.remote())
    assert op_names(calls).count("free") == 1


@pytest.mark.asyncio
async def test_engine_error_still_frees(build_token_tracking_engine):
    """A mid-stream engine error propagates but still frees the request."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(
        delta_steps(3, prompt_len=8), actor, error_after=1
    )

    with pytest.raises(RuntimeError, match="engine failure"):
        await consume(
            engine.generate(
                "p", SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
            )
        )
    await drain(engine)

    assert op_names(ray.get(actor.get_dynamo_calls.remote()))[-1] == "free"


@pytest.mark.asyncio
async def test_zero_output_request_added_then_freed(build_token_tracking_engine):
    """An output-less request (e.g. validation abort) is admitted and freed."""
    actor = RecordingKVRouterActor.remote(block_size=16)
    engine = build_token_tracking_engine(
        [request_output([0], prompt_len=3, finished=True)], actor
    )

    prompt = {"prompt_token_ids": [1, 2, 3]}
    await consume(
        engine.generate(
            prompt, SamplingParams(output_kind=RequestOutputKind.DELTA), "r"
        )
    )
    await drain(engine)

    assert op_names(ray.get(actor.get_dynamo_calls.remote())) == ["add_request", "free"]


@pytest.mark.asyncio
async def test_hooks_for_unknown_request_book_nothing():
    """Hooks for a never-admitted request id touch neither state nor Dynamo."""
    actor = LifecycleActor(block_size=16)
    await actor.on_prefill_complete("missing")
    await actor.on_decode_progress("missing", 3)
    await actor.on_request_completed("missing")

    assert await actor.get_request_lifecycle("missing") is None
    assert actor._kv_router.calls == []


@pytest.mark.asyncio
async def test_active_load_views():
    """Active load is per-worker; completion evicts the request and frees it."""
    actor = LifecycleActor(block_size=16)
    await actor.on_request_added("a", 1, list(range(8)))
    await actor.on_request_added("b", 1, [])
    await actor.on_request_added("c", 2, [])
    assert await actor.get_worker_active_load(1) == 2
    assert await actor.get_worker_active_load(2) == 1

    await actor.on_request_completed("a")
    assert await actor.get_worker_active_load(1) == 1
    assert set(await actor.get_active_request_ids()) == {"b", "c"}
    assert await actor.get_request_lifecycle("a") is None
    assert ("free", "a") in actor._kv_router.calls


@pytest.mark.asyncio
async def test_actor_failure_isolation(build_token_tracking_engine):
    """A failing actor never disrupts the engine's output stream."""
    engine = build_token_tracking_engine(delta_steps(2), RaisingActor.remote())

    outputs = await consume(
        engine.generate("p", SamplingParams(output_kind=RequestOutputKind.DELTA), "r")
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
        engine.generate("p", SamplingParams(output_kind=RequestOutputKind.DELTA), "r")
    )

    assert len(outputs) == 2
    assert engine._kv_reporter is None  # resolution failed; retried next call


def test_derive_kv_event_block_size():
    assert derive_kv_event_block_size({"block_size": 32}) == 32
    assert derive_kv_event_block_size({}) == CacheConfig.DEFAULT_BLOCK_SIZE


if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
