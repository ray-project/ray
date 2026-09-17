# KV-aware routing on asynchronous RL rollouts

This reproducer compares three routers on one asynchronous, multi-turn RL rollout workload:

- `session-affinity`: **ConsistentHashRouter** pins every turn of a rollout to one replica using its stable session ID.
- `pure-kv-cache`: **PureKVCacheAffinityRouter** is a demonstration-only router that modifies `KVAwareRouter` to score only based on matching GPU KV-cache overlap. It is not an upstream or production router.
- `kv-token-aware`: **KVAwareRouter** scores KV-cache overlap together with active prefill and decode-token load.

## Setup

- **Ray cluster:** build a cluster image from `anyscale/ray-llm:2.58.0-py312-cu130` with Dynamo 1.4.0 installed, then start a Ray cluster with **8 × H100s**.

  ```dockerfile
  FROM anyscale/ray-llm:2.58.0-py312-cu130
  RUN python -m pip install --no-cache-dir "ai-dynamo-runtime==1.4.0"
  ```

- **Model & parallelism:** serve `openai/gpt-oss-120b` as **4 replicas × TP=2**.
- **Model runtime:** set `AGENTIC_RUNTIME_PYTHON` to the Python executable used by model replicas. It must be able to import `ai-dynamo-runtime==1.4.0`. The setup phase then rebuilds and installs a patched Dynamo wheel into this interpreter for the pure-KV-cache variant.
- **Client:** run the benchmark from a shell connected to the Ray cluster.
- **Client tooling:** install `uv`, and keep the AIPerf and Dynamo Git checkouts at `/home/ray/default/aiperf` and `/home/ray/default/dynamo` or override `AIPERF_SRC` and `DYNAMO_SOURCE` before running the script. If Cargo, `protoc`, or `libclang` is unavailable, setup installs artifact-local fallbacks to build the Dynamo wheel.
- **Model cache:** the default `HF_HOME` is `/mnt/cluster_storage/hf_cache`, the shared Hugging Face cache for this cluster. Override it for another cluster if needed.

If Dynamo is not already checked out locally, create the required source checkouts before running the reproducer:

```bash
cd /home/ray/default
git clone https://github.com/ai-dynamo/dynamo.git dynamo
git clone https://github.com/ai-dynamo/aiperf.git aiperf
```

For example:

```bash
cd /home/ray/default/ray/python/ray/llm/examples/blogs/kv_token_aware_routing/async_rl_rollout_straggler
export AGENTIC_RUNTIME_PYTHON=/path/to/model-runtime/bin/python
./reproduce.sh --out /path/to/artifact
```

`--out` is required and must not exist. `reproduce.sh` creates an artifact-local UV environment for AIPerf.

The runtime hook in `scripts/raynoophook.py` removes the managed cluster's inherited working directory before vLLM's EngineCore reconnects to Ray. Without it, every engine can attempt to upload that multi-gigabyte directory. The adjacent `sitecustomize.py` restores two Cutlass type aliases required by this vLLM image and fixes vLLM 0.26's zero TCPStore-port handling, which otherwise makes co-located TP=2 replicas race for port 100.

## What the command does

1. It snapshots the pinned AIPerf and Dynamo commits beneath `$OUT/sources`, patches only those snapshots, creates `$OUT/aiperf-venv`, builds and installs the Dynamo wheel in `AGENTIC_RUNTIME_PYTHON`, and generates the workload.
2. It deploys each router variant with Ray's supported Python `build_openai_app` builder, warms the shared prefix with a finite excluded HTTP request set, then runs measured AIPerf client traffic from `$OUT/aiperf-venv`.
3. It shuts down the Serve application after each variant, validates the collected artifacts, and writes:

```text
/path/to/artifact/async_rl_rollout_router_comparison.png
```

For a `KVAwareRouter` subclass, `build_openai_app` automatically enables vLLM KV-event publishing and Ray's corresponding event consumer.

## Compatibility

- **Ray:** 2.58.0, from the cluster image above.
- **AIPerf:** 0.12.0, commit `c2f5e9d459005d362457716bbd865d247232fa30` (2026-08-01). The patch adds closed-loop turn concurrency and fixed-schedule DAG timestamps.
- **Dynamo:** 1.4.0, commit `dfc15c35d9cecffd909e8b10ab6ec62d4fa3d844` (2026-08-04). The patch adds the demonstration-only pure-KV-cache scoring mode and a stable equal-cache tie-break; it changes neither session affinity nor full KVAwareRouter scoring.

## Workload

- 80 globally unique rollouts: 10 steps × 8 rollouts, each with 10 serial turns. AIPerf maintains 16 HTTP turns in flight, and a completed turn immediately releases a credit to a ready continuation or rollout; no step waits for another step.
- Turn one contains a shared 2,048-token system prompt, a 512-token shared step state, and a 1,024-token rollout-specific brief. Later turns carry forward the rollout's preceding assistant output.
- Normal turns generate 1,024 tokens. In every step, rollout 0 and rollout 4 generate 8,192 tokens on turn 10; rollout 4 starts 2.2 seconds after rollout 0.
- Before measuring `session-affinity`, a finite set of globally unique straggler session keys is calibrated so both long rollouts in every step map to one live ConsistentHash replica. The rewritten workload is then used unchanged for all three router variants.
- Prefix caching is GPU-resident. CPU KV offload is not enabled.

## Evaluation metrics

- **p99 rollout end-to-end latency:** for each of the 80 rollouts, elapsed time from its first turn's client request start to its last turn's response completion; report the 99th percentile across rollouts.
- **Prefix cache hit rate:** sum the server-reported `usage_prompt_cache_read_tokens` across all 800 measured responses, divided by summed server-reported `usage_prompt_tokens`.
- **Mean active decode-block load CV:** every 0.5 seconds, reconstruct each replica's in-flight decoding KV load by summing 16-token KV blocks for its requests. A request contributes its input tokens plus output tokens linearly estimated from first output to completion. Compute the coefficient of variation (population standard deviation divided by mean) across all four replicas, including idle replicas as zero, then average the nonempty samples. The deployment explicitly sets `block_size=16`, which is also this CUDA vLLM version's default.

The comparison plot reports these three metrics. Lower p99 latency and decode-block CV are better; higher prefix cache hit rate is better.

## FAQ

- **Why not use Ray 2.58.0 out of the box?** This benchmark uses Ray 2.58.0 as its base, including its upstream ConsistentHashRouter, KVAwareRouter, and stock Dynamo 1.4.0 runtime. The only routing change is the rebuilt Dynamo wheel for the demonstration-only pure-KV-cache scorer; full KVAwareRouter behavior is unchanged. The model runtime also needs two small vLLM/Cutlass compatibility fixes.
- **Why customize Dynamo?** `pure-kv-cache` is a demonstration-only variant that scores matching KV-cache overlap and nothing else. Comparing it with full KVAwareRouter isolates the benefit of token-load awareness; the full KVAwareRouter scoring path is otherwise unchanged.
- **Why customize AIPerf?** The workload needs closed-loop *turn* concurrency: a credit remains occupied until a streaming turn finishes, then releases the rollout's next serial turn or another ready rollout. The patch also carries deterministic root-turn timestamps for the asynchronous fixed schedule.
- **Why calibrate session IDs?** Unique stable session IDs naturally can hash to the same replica, but a single run might not contain the collision needed to make the straggler effect visible. Calibration queries the live ConsistentHash ring and deterministically chooses still-unique straggler IDs that collide on one replica, then reuses that exact workload for every router variant.
- **Why use `RoutingLogMixin`?** It does not alter routing. It records each route decision, tokenized request, and KVA tracker status, and samples SelectionService's per-replica load every 0.5 seconds. Those records validate lifecycle behavior and derive the decode-load CV causal metric.

## Layout

```text
reproduce.sh       required --out entry point
scripts/           setup, deployment, workload, traffic, validation, analysis, and plotting
patches/           AIPerf support and the demonstration-only Dynamo scoring patch
```
