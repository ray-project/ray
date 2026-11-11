HAProxy streaming metrics: design, validation and overhead
==========================================================

Ray branch ``ttft-itl-metrics-final`` (rebased on master, vLLM 0.33) together
with ray-haproxy branch ``stream-metrics`` measure, at HAProxy:

* TTFT: stream start in HAProxy to the first token-bearing chunk forwarded.
* ITL: interval between token-bearing chunks forwarded by HAProxy, divided by
  the chunk's token count and recorded once per token (Dynamo's normalization;
  the first chunk records TTFT only).
* Request latency: stream start to response completion; routing latency: the
  ingress-router round trip.

Only direct streaming with HAProxy is instrumented (``RAY_SERVE_ENABLE_LLM_STREAMING_METRICS=1``).

Design
------

Replica (Python, ``llm_metrics.py``): every streamed choice gets
``"ray_token_count":N``, the tokens generated since that choice's previous chunk.

* Completions: vLLM's stream generator is cloned with copies of its
  response/choice models. vLLM already passes delta ``token_ids`` to its choice
  model when ``return_token_ids`` is set on a renderer-only request copy; the
  clone binds ``as_list`` to ``len``, so ``N`` arrives as an integer field, and
  the hidden ID fields read vLLM's ``weight_versions`` input (``None`` unless
  requested) to serialize vLLM's own nulls. No Python frame runs per event. The
  copies omit only ``OpenAIBaseModel``'s per-construction extra-field logging
  validator, which vLLM's own constructions never trigger. The counted choice is
  also closed (``extra="ignore"`` instead of vLLM's ``"allow"``), so pydantic
  builds no extras dict per choice and the prompt IDs passed for the internal
  ``return_token_ids`` are simply ignored. That is used only after checking the
  generator's code: every choice construction's keyword names (a code constant)
  are declared fields, so ``"allow"`` would never store an extra; the generator
  never reads ``choices`` back, so nothing assigns attributes a closed model
  rejects; and it has no ``**`` calls or nested functions the check cannot see.
  Otherwise the open model is used, with an excluded field consuming the prompt
  IDs.
* Echo completions: a per-request clone attaches counts from the engine outputs.
* Chat: ``N`` is the delta of vLLM's own ``ChatStreamProcessor.num_output_tokens``,
  so tokens a reasoning/tool parser withholds count toward the next chunk.

HAProxy (C, ``ray-haproxy/src/stream_metrics.c``): an HTX filter scans response
DATA blocks in place on the forwarding thread for ``"choices":[{"index":I`` and
``"ray_token_count":N``, carrying split markers across fragments, and records
into per-thread histograms. No copies, queues, parser threads or JSON decoding
per event; the same thread's Lua task exports exact cumulative histograms to
Ray's metrics agent, reading Ray's auth token from its existing source.

Memory: per-stream observer state is ~100 bytes (freed in ``detach``) plus one
small group per replica identity per thread. Attaching any HTX data filter
disables HAProxy's zero-copy fast-forwarding, so every streamed chunk needs a
response buffer. With Ray's default 16 KiB ``tune.bufsize`` those come from
HAProxy's per-thread pool cache: HAProxy RSS at 256 streams went from 15 MB to
18 MB. With ingress-router body forwarding, ``tune.bufsize`` is 8 MiB, above
HAProxy's 512 KiB per-thread cache, so every buffer is a glibc allocation that
glibc's dynamic thresholds map, fault in and return continuously. When the
filter is active with such buffers, Ray starts HAProxy with fixed glibc
thresholds (``mmap_threshold`` 32 MiB, ``trim_threshold`` 256 MiB; tunables
already in ``GLIBC_TUNABLES`` are kept), so freed buffers stay in the heap for
reuse (``haproxy.py``). Forced HTX defragmentation was
measured to be rare (<256 per thread per run), so no HAProxy patch is shipped.

Benchmark protocol
------------------

2x H100, two TP=1 Qwen3.5-0.8B replicas, round-robin routing, prefix caching off.
``vllm bench serve``: random 1024/1024 tokens, ``--ignore-eos``, Poisson arrivals
(18 and 36 req/s; 6 req/s in the fresh-deployment rounds) capped at 128/256 in
flight, many pinned single-threaded client processes (no client saturation, no
cgroup throttling), identical seeds for every arm. Each deployment is warmed up
first; only steady-state requests (the middle 60% of the interval in which all
clients issue requests) count.

36 req/s is past the vLLM frontend's capacity: each replica's event-loop thread
is busy ~35 s of a ~49 s run and vLLM emits ~0.7-0.8 chunks per token natively
and ~0.85-0.9 with the instrumented models (it merges tokens that queue up).
There, any per-event cost shifts how many tokens vLLM merges, and run-to-run
noise is large (chunks per token varies +-10% between runs of the same arm). 18
req/s is below capacity (~1.0 chunks per token).

Two HAProxy configurations:

* Default: Ray's 16 KiB ``tune.bufsize`` (``RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=0``).
* Body forwarding: ``RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY=1``, which
  raises ``tune.bufsize`` to 8 MiB so ingress routers can read request bodies
  (``deploy.py``'s default).

Same engines (``abtest.py``): on one warmed deployment, OFF is restored in place
(native vLLM generators and routes in every replica, HAProxy reloaded without the
filter, exporter and routing timer), rotating the arm order per cycle. Two
benchmark-only arms attribute the cost: ``copies`` (native routes, vLLM
generators using the same validator-free model copies as ON, including the
closed choice whenever ON uses it, no count, no observer) and ``py`` (ON in
the replicas, HAProxy without the observer). ON minus
COPIES is therefore the metrics' own cost; ON minus OFF also includes the
validator saving. Every run also records per-thread scheduler statistics
(``/proc/<pid>/task/<tid>/schedstat``) of the replicas and HAProxy, and the
``GLIBC_TUNABLES`` each HAProxy process received.

``abstats.py`` reports the mean of the paired per-cycle deltas with a bootstrap
95% CI, pooling suites when given several run directories. Suites on the same
deployment an hour apart differed by more than their own CIs, so pooled CIs are
optimistic. A cycle with a run whose mean TPOT exceeds the load point's all-run
median by more than 10% is reported and excluded in a second table: the Anyscale
workspace autosnapshot zips, hashes and uploads the workspace every 7.5 minutes
and has slowed both replicas together by 20%+ for a whole run, in arms with and
without the observer.

Two ITL views are reported. vLLM bench's ITL is per SSE chunk; when vLLM's
frontend falls behind it merges tokens into fewer chunks, so per-chunk ITL moves
with chunks per token. TPOT is per token and is what HAProxy's ITL measures (its
mean equals the client's mean TPOT, see Correctness).

Results
-------

Correctness, on every ON run below (100 with the default configuration; with
body forwarding 28 with the shipped allocator thresholds, 16 from the allocator
suite and 42 earlier): HAProxy TTFT, request and routing counts equal the
client's requests; ITL counts equal the client's output tokens minus tokens that arrived
in a stream's first chunk (0-75 of up to 1.57M per run); HAProxy mean ITL equals
the client's mean TPOT within 0.004%; HAProxy TTFT and request latency are
1.3-2.5 ms below the client's (the client-HAProxy leg); zero collection errors.

Percent change, mean of the paired per-cycle deltas [bootstrap 95% CI].

Current implementation (ray commit "Close the counted completion choice model",
ray-haproxy ``49de10d``), default configuration (16 KiB ``tune.bufsize``),
completions at 36 req/s (256 in flight), two suites on one deployment (A: OFF,
COPIES, ON; B: COPIES, PY, ON)::

                              cycles  TTFT mean            TTFT p50             TPOT mean            ITL per chunk, mean  ITL per chunk, p50     ITL per chunk, p99
    Metrics' own cost: ON minus COPIES
    suite A                   16      +0.85 [-0.12,+1.83]  +0.92 [-0.24,+2.08]  -0.50 [-1.23,+0.17]  +1.46 [+0.42,+2.52]  +2.57 [+1.38,+3.69]   +0.05 [-0.91,+0.97]
    suite B                   20      +0.23 [-0.93,+1.43]  +0.77 [-0.66,+2.25]  -0.41 [-0.87,+0.10]  +0.26 [-0.88,+1.45]  +1.15 [-0.58,+2.89]   +0.28 [-0.45,+0.99]
    pooled                    36      +0.51 [-0.27,+1.30]  +0.84 [-0.12,+1.80]  -0.45 [-0.86,-0.05]  +0.79 [-0.03,+1.61]  +1.78 [+0.66,+2.89]   +0.18 [-0.41,+0.77]

    What users see: ON minus OFF
    suite A                   16      -3.00 [-4.20,-1.77]  -4.37 [-5.88,-2.82]  +2.30 [+1.66,+3.02]  -6.06 [-7.09,-5.01]  -14.74 [-15.98,-13.47] +1.01 [+0.13,+1.89]

    Attribution (suite B; replicas identical in ON and PY, HAProxy identical in PY and COPIES)
    HAProxy, ON minus PY      20      +0.70 [-0.24,+1.72]  +0.93 [-0.36,+2.28]  -0.15 [-0.79,+0.47]  +0.32 [-0.72,+1.28]  +0.81 [-0.53,+2.19]   -0.23 [-1.00,+0.53]
    replica, PY minus COPIES  20      -0.46 [-1.29,+0.40]  -0.14 [-1.06,+0.77]  -0.24 [-1.01,+0.52]  -0.04 [-1.15,+1.02]  +0.35 [-0.88,+1.47]   +0.54 [-0.31,+1.40]

Pooled chunks per token, ON minus COPIES: -1.19% [-1.85,-0.52]. COPIES minus OFF
(suite A, means): TTFT -3.81%, TPOT +2.82%, per-chunk ITL p50 -16.85%, chunks
per token +11.07%. ON's higher TPOT against OFF therefore comes with the
validator-free models and closed choice, which let vLLM's saturated frontend
merge fewer tokens, not with the count (which changes TPOT by -0.45%). The two
suites' per-chunk p50 estimates differ by about their CIs, so the pooled CI is
optimistic.

Earlier measurements, before the closed choice (ray commit "Keep HAProxy's 8 MiB
buffers in the heap with stream metrics", which changes only the body forwarding
configuration over its parent) and ray-haproxy ``49de10d``. The closed choice
changes only the completions replica's per-event cost, so the HAProxy-side
results below still apply.

Default configuration (16 KiB ``tune.bufsize``)::

    ON minus OFF (what users see)
    endpoint     rate (cap)  cycles  TTFT mean            TTFT p50             TPOT mean            ITL per chunk, mean  ITL per chunk, p50
    completions  36/s (256)  20      -3.99 [-6.33,-1.97]  -3.34 [-4.50,-2.16]  +1.22 [+0.53,+1.85]  -7.35 [-8.92,-5.72]  -10.37 [-12.32,-8.56]
    completions  18/s (128)  12      -0.57 [-1.45,+0.36]  -0.47 [-1.39,+0.47]  +0.40 [-0.64,+1.39]  +0.34 [-0.65,+1.28]  +0.02 [-0.85,+0.91]
    chat         18/s (128)   8      -1.41 [-3.99,+0.92]  -1.58 [-3.69,+0.34]  -1.16 [-2.63,-0.02]  -1.42 [-3.25,-0.08]  -1.24 [-2.18,-0.51]

    Metrics' own cost: ON minus COPIES
    completions  36/s (256)  16 [1]  -0.57 [-2.68,+1.33]  +0.00 [-1.41,+1.41]  -0.46 [-1.48,+0.46]  +0.45 [-1.68,+2.50]  +2.21 [-1.03,+5.49]
                             44 [2]  +0.71 [-0.50,+1.88]  +1.03 [+0.04,+1.98]  -0.19 [-0.76,+0.35]  +1.80 [+0.31,+3.21]  +3.71 [+1.74,+5.65]
    completions  18/s (128)   4      +1.29 [-2.12,+4.38]  +1.42 [-1.71,+4.48]  +0.91 [-0.92,+2.74]  +0.86 [-0.99,+2.70]  +0.37 [-0.62,+1.75]

    [1] Fresh deployment, ON and COPIES only, COPIES' model classes built once
        per replica like ON's.
    [2] All four suites. In the three earlier ones (TTFT +3.95 [+2.25,+6.07],
        +0.52 [-1.88,+2.61], -0.57 [-1.98,+1.04]) the harness rebuilt COPIES'
        model classes at every switch to that arm.

Body forwarding (8 MiB ``tune.bufsize``)::

    ON minus OFF, before the allocator thresholds
    completions  36/s (256)  10      -1.12 [-2.67,+0.62]  -1.46 [-3.26,+0.57]  +0.31 [-1.49,+2.07]  -6.93 [-9.40,-4.47]
    completions  18/s (128)  11 [3]  +0.10 [-1.74,+2.15]  -0.17 [-1.62,+1.51]  +0.24 [-1.38,+1.74]  +0.11 [-1.53,+1.71]
    chat         18/s (128)  12      -2.12 [-3.91,-0.41]  -2.45 [-3.73,-1.23]  -1.02 [-2.64,+0.58]  -1.27 [-3.29,+0.57]

    ON minus COPIES
    before the thresholds    17 [4]  +1.25 [-0.44,+2.88]  +2.69 [+1.36,+3.96]  -0.27 [-1.51,+1.00]  +2.83 [+0.74,+4.93]
    with the thresholds      28 [5]  +1.18 [-0.84,+3.08]  +0.91 [-0.81,+2.54]  -0.51 [-1.19,+0.17]  +1.98 [-0.87,+4.46]  +3.93 [+0.46,+7.23]

    [3] 12 cycles; cycle 7 disturbed (ON run +25% TPOT; both replicas slowed
        alike). With it: TTFT +2.78 [-1.24,+8.89], TPOT +2.25 [-0.91,+6.80].
    [4] Two suites (10 + 8 cycles), earlier harness; one cycle disturbed (its
        PY run +18% TPOT; its ON run, next in time, +28% TTFT).
    [5] Two suites (16 + 12 cycles) on one fresh deployment.

Attribution at 36 req/s before the closed choice (both configurations, 20
cycles; replicas identical in ON and PY, HAProxy identical in PY and COPIES)::

    HAProxy side, ON minus PY      -0.30 [-2.57,+2.06]  TPOT +0.50 [-0.29,+1.27]  ITL per chunk mean -0.30 [-3.20,+2.87]  p50 -0.62 [-3.96,+2.97]
    replica side, PY minus COPIES  +1.15 [-1.00,+3.48]  TPOT -0.87 [-1.60,-0.07]  ITL per chunk mean +0.87 [-2.64,+4.46]  p50 +2.31 [-1.43,+6.01]

Where the cost is:

* Replica: in vLLM's generator the count costs 0.145 us per streamed event (IQR
  0.10-0.18) against the same models without it, down from ~0.23 us with the
  open choice (``micro_gen2.py``, interleaved blocks). ~0.11 us of that is
  pydantic validating and serializing one integer field: an ``int``, strict
  ``int``, ``Any`` or shorter-named field all cost 0.11-0.13 us
  (``micro_count.py``). The instrumented generator is ~2.7 us per event cheaper
  than vLLM's own, so ON is cheaper than OFF. At 36 req/s (suite B) the loop
  threads' CPU per token was COPIES 43.4 us, PY and ON 43.6 us (schedstat).
  Earlier, at 18 req/s (before the closed choice, 4 cycles), CPU per event was
  COPIES 57.4 us, ON +0.41, PY +0.61, OFF +3.70; a py-spy profile of the loop
  thread (500 Hz, two 40 s windows per arm) found no frame with a consistent
  difference.
* HAProxy: 23.9 -> 25.4 us of CPU per chunk with the observer at 16 KiB, on
  HAProxy's own threads (~0.65 of its 4 threads busy at 36 req/s), off the
  replicas' critical path. With 8 MiB buffers and glibc's dynamic thresholds
  the observer raised it from 29.7 to 33.7 us and page faults from 5.5k to
  11.7k/s (36 req/s, HAProxy sampled every 10 s; with a 1 GiB trim threshold:
  28.6 us, 2.1k faults/s). With the shipped thresholds ON used 29.7 and 28.6 us
  per chunk against COPIES' 30.0 and 30.1 us (two suites).
* No CPU contention: the replica loop threads waited ~0.01 s in the run queue
  per ~35 s of run time in every arm.

At or below the frontend's capacity the metrics' cost is within measurement
noise. Past it, at 36 req/s, the current implementation's own cost (ON minus
COPIES, 36 cycles) is TTFT +0.5%, TPOT -0.5%, per-chunk ITL mean +0.8%, p50
+1.8% [+0.7,+2.9] and p99 +0.2%. The per-chunk median moves with how many
tokens vLLM merges (ON emits 1.2% fewer chunks per token), consistent with the
count's ~0.15 us per event being amplified by a saturated event loop; with the
open choice (~0.23 us) it was +3.7% over 44 cycles. The client's per-chunk gaps
(suite A, ``itl_shape.py``) show no difference in reads that coalesce chunks (at
most 0.003% of gaps under 0.3 ms in any arm); ON has 0.7 points more gaps of
8 ms or longer than COPIES. Every chunk
must carry its count for HAProxy to compute per-token ITL, and an integer field
validated and serialized natively by pydantic-core is the cheapest carrier
measured. Against OFF, ON's TTFT is 3% shorter and its median per-chunk gap 15%
shorter, while TPOT is 2.3% longer, as with COPIES.

Allocator thresholds, measured with ``haproxy_latency.py`` (8 MiB buffers, 256
streams, 41.8k chunks/s, 3 reps per setting, observer arm)::

    glibc setting                       page faults/s  p99.9 latency   RSS
    dynamic (glibc default)             16-17k         5.3-7.6 ms      88-104 MB
    trim 1 GiB, mmap 32 MiB             0.5-0.9k       2.5-2.7 ms      80-105 MB
    trim 256 MiB, mmap 32 MiB (Ray)     0.4-1.1k       2.8-4.0 ms      96-120 MB
    trim 64 MiB, mmap 32 MiB            8-9k           5.3-6.0 ms      56-128 MB

Memory with the thresholds: ``stress_haproxy.py`` (8 MiB, 256 concurrent
vLLM-like streams with 30% client aborts, 6 rounds of 3000 requests): RSS flat
at ~140 MB (dynamic) and ~157 MB (thresholds), byte-identical bodies, zero
errors. Large request bodies on the deployment (``large_body_rss.py``: bursts of
128 concurrent 4 MiB bodies, which HAProxy buffers for the ingress router):
HAProxy RSS after 8 bursts 3.0 GB with glibc's dynamic thresholds (observer
off; not returned after 30 s idle) and 3.3 GB with the thresholds (+15 to +43
MB per burst over the last four). The difference is bounded by the trim
threshold per glibc arena (one per HAProxy thread plus the main arena).

HAProxy in isolation (``haproxy_latency.py``): emulated replicas send one SSE
event per stream per 6.1 ms step to 256 keep-alive streams (41.8k chunks/s)
through the production binary and globals (4 threads, ``splice-response``,
``http-no-delay``), observer vs. none, alternating reps:

* 16 KiB ``tune.bufsize`` (4 reps): HAProxy 0.80 -> 0.83 cores; one-way latency
  per chunk, per rep, mean -38 to +65 us; p99 +63 to +165 us; page faults 3-7/s
  -> 30-39/s; RSS 15 -> 18 MB.
* 8 MiB ``tune.bufsize`` with glibc's dynamic thresholds (4 reps): HAProxy task
  CPU per chunk is unchanged (task profiler: ~22 us in both arms; process CPU,
  which includes allocation and page faults, +2-8%);
  one-way latency per chunk mean -7 to +59 us; p99.9 1.5-2.1 ms -> 6.4-6.9 ms,
  as more chunks fall into multi-millisecond allocation stalls (15-19k page
  faults/s in both arms).

A constant per-chunk delay cancels out of ITL and adds ~0.05% to TTFT.

Fresh deployments (body forwarding, 3 OFF, 3 ON, before the final ``len``-based
counting step), ON minus OFF means::

    rate    TTFT mean        TPOT mean       (OFF deploy-to-deploy spread)
    6/s     +1.55%           +0.25%          TTFT +-1.3%
    18/s    +1.62%           +0.48%          TTFT +-2.1%
    36/s    -0.01%           -3.04%          TTFT +-4.9%

The previous (ring + parser-thread + Python wrapper) design measured +2.95%,
+1.94%, -0.48% TTFT in the same protocol.

Memory safety: ASan builds of HAProxy and the observer passed 20k adversarial
requests (exact counts, byte-identical bodies), 10k with 30% client aborts, and
vLLM-like 1000-event streams at 8 MiB ``tune.bufsize``; no sanitizer reports,
clean soft-stop exits. Native unit tests run under ASan/UBSan/LSan, including a
randomized scanner test against a reference model (verified to fail under an
injected carry bug).

Reproduction
------------

The scripts assume this workspace's layout: a venv at ``/mnt/local_storage/sm/sm_env``
with vLLM 0.33 whose ``ray`` package overlays this branch's ``serve``/``llm``
sources on the installed core, HAProxy builds under
``/mnt/local_storage/sm/haproxy_build`` and the harness in ``/home/ray/default/sm_work``.

::

    python deploy.py --mode on --out runs/x \
        --extra-env '{"RAY_SERVE_INGRESS_REQUEST_ROUTER_FORWARD_BODY": "0"}'  # default config
    python deploy.py --mode on --out runs/x            # body forwarding (8 MiB)
    python bench.py run --out runs/x/warmup --rate 36 --conc 256 --num 768 --clients 32
    python abtest.py --out runs/ab --cycles 10 --points 36:256:1536:32 \
        --arms off:dist_final copies:dist_final on:dist_final   # same engines
    python abtest.py --out runs/ab --first-cycle 10 --cycles 10 ...  # append cycles
    python abtest.py ... --arms off:dist_final copies:dist_final py:dist_final on:dist_final
    python abtest.py ... --backend openai-chat --endpoint /v1/chat/completions
    python abtest.py ... --arms py:dist_final on:dist_final pyg:dist_final ong:dist_final  # glibc
    python abstats.py runs/ab [runs/ab2 ...]           # paired deltas, CIs, checks; pools suites
    python cpu_per_event.py runs/ab [copies|py]        # replica loop / HAProxy CPU per event
    python correctness.py runs/ab [runs/ab2 ...]       # HAProxy counts and means vs. client
    python micro_gen2.py                               # generator CPU per event: native, copies, ON
    python micro_count.py                              # count-field variants (construct + serialize)
    python micro_fields.py                             # response model variants (construct + serialize)
    python micro_variants.py v0|v5|native              # earlier generator variants
    python itl_shape.py runs/ab copies on off          # per-chunk ITL distribution by arm
    python profile_events.py --out runs/prof           # py-spy of the replica loop, per arm
    python mode_rss.py --out runs/rss --rounds 8 copies on  # HAProxy RSS, large request bodies
    ./tuning_sweep2.sh                                 # glibc thresholds, haproxy_latency.py
    ./campaign.sh off:-:off_1 on:final:on_1 ...       # interleaved fresh deployments
    python compare.py off:runs/off_1 on:runs/on_1 ...
    python haproxy_latency.py --binary DIST/haproxy --reps 5 [--bufsize 16384] [--profile]
    python stress_haproxy.py --binary DIST/haproxy --requests 20000 --concurrency 128 [--aborts]
