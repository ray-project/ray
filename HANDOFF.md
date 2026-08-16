# Handoff: Ray Serve LLM Governance Middleware
**Date**: 2026-08-16
**Status**: Phases 1–4 implemented on `llm-governance-middleware-foundation`. No PR opened. Two commits not pushed. Local macOS cannot run the vLLM-coupled tests.

**Do not commit this file.** It is a local working note for the next session. Ray PRs that add repo-root agent notes get closed.

**Issue**: [ray-project/ray#65259](https://github.com/ray-project/ray/issues/65259)
**Maintainer**: richardliaw (Anyscale)
**Adapters (out of this repo)**: nagasatish007 → TealTiger; kindrat86 → AgentShield

## Current state

The branch adds a **semantic** governance layer on the OpenAI ingress. It does not touch vLLM/SGLang. Users subclass `LLMMiddleware`, pass instances into `GovernanceIngress` through the existing `ingress_cls_config` / `ingress_extra_kwargs` on `build_openai_app`, and can block or inspect chat / completions / transcription traffic.

What works:

- `RequestContext` and `BlockedResponse` dataclasses, with HTTP mapping (`400` PII, `402` budget, `403` access, `429` throttle + `Retry-After`).
- `LLMMiddleware` ABC: `before_inference` required; `after_inference` and `on_inference_complete` have pass-through defaults.
- `MiddlewareChain` runs hooks in list order. First `BlockedResponse` stops `before`/`after`. `on_inference_complete` continues after a middleware error (logged, not re-raised). `before`/`after` errors are logged and re-raised (fail-closed).
- `GovernanceIngress` overrides `OpenAiIngress._process_llm_request` and runs the three hooks around `_get_response`.
- Public alpha API at `ray.serve.llm.governance` (`GovernanceIngress`, `LLMMiddleware`, `RequestContext`, `BlockedResponse`).
- User guide at `doc/source/serve/llm/user-guides/governance.md`, plus API autosummary entries.
- Reference-only `PIIMiddleware` (email/SSN regex) and `BudgetMiddleware` (in-memory per-replica counters). Not public.
- Tests: chain, context, reference middleware, ingress (mocked handle), builder wiring, HTTP e2e via `serve.run` + `httpx` + `MockVLLMEngine`.

What is intentionally not in v1:

- No stream buffering / redaction. `after_inference` on a stream sees the **first chunk only**.
- No `on_stream_chunk` hook.
- No per-model `LLMConfig.middleware` list. App/ingress level only.
- No cluster-wide budget store.
- `estimated_input_tokens` exists on `RequestContext` and is never populated.
- Embeddings and scoring do not run these hooks.
- Direct streaming (`RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING`) is rejected by the existing builder when a custom ingress is set. Governance never sees those requests.

## What was just worked on

Branch: `llm-governance-middleware-foundation` (ahead of `origin/llm-governance-middleware-foundation` by 2 commits). Working tree clean.

Unique commits vs `upstream/master`:

| SHA | Message |
|-----|---------|
| `127a13d69c` | Foundation dataclasses |
| `4fbcd11c61` | `LLMMiddleware` + `MiddlewareChain` + unit tests |
| `9c38bbe94b` | `GovernanceIngress` + context helpers |
| `4c6c1a710c` | Ingress integration tests (typo in subject: "governanc ingess") |
| `8d1dffd8bd` | Harden hooks: optional `after_inference`, aclose on block/disconnect, lazy import, context + reference tests |
| `d6fe1dd856` | Public API, docs, HTTP e2e tests |

Unpushed: `8d1dffd8bd` and `d6fe1dd856`.

Sibling branch `feat/llm-governance-middleware-foundation` is stale (only the first dataclass commit). Ignore it.

No open PR for this work. Duplicate-check before opening: `gh pr list --repo ray-project/ray --state open --search "governance"` and `gh pr list --repo ray-project/ray --state open --search "65259 in:body"`. Last check (2026-08-16) found none.

## How it works

Default Serve LLM path (this is the only path governance covers):

```
Client POST /v1/chat/completions
  → Serve HTTP proxy + FastAPI middleware
  → GovernanceIngress.chat()            # inherited from OpenAiIngress
  → GovernanceIngress._process_llm_request()
       1. build_request_context(body, raw_request)
       2. chain.before_inference(body, context)     # block → JSON error, no LLM call
       3. _get_response() → LLMServer → engine
       4. _peek_at_generator(upstream)
       5. chain.after_inference(body, first_chunk, context)
       6. non-stream: JSONResponse + on_inference_complete(usage)
          stream: StreamingResponse wrapping SSE + on_inference_complete in finally
  → Client
```

`LLMRouter` is **not** on this path. It only exists when direct streaming is on, and that mode cannot use a custom ingress.

Wiring users actually write:

```python
from ray.serve.llm import LLMConfig, build_openai_app
from ray.serve.llm.governance import GovernanceIngress, LLMMiddleware, BlockedResponse

app = build_openai_app({
    "llm_configs": [llm_config],
    "ingress_cls_config": {
        "ingress_cls": GovernanceIngress,  # or "ray.serve.llm.governance:GovernanceIngress"
        "ingress_extra_kwargs": {"middlewares": [DenyModelMiddleware()]},
    },
})
```

`build_openai_app` already forwards `ingress_extra_kwargs` into the ingress constructor. No builder fork.

Request-context sources (`build_request_context` in `context.py`):

| Field | Source |
|-------|--------|
| `model_id` | `body.model` |
| `request_id` | `request.state.request_id`, else `x-request-id` |
| `session_id` | `session_id_from_headers` (same matcher as the Serve proxy) |
| `max_tokens` | `body.max_tokens` or `body.max_completion_tokens` |
| `user_id` | `request.state.user_id`, else `body.user`, else `x-user-id` |
| `tenant_id` | `x-tenant-id` |
| `headers` | copy of inbound HTTP headers |
| `estimated_input_tokens` | unused |

Public types in `python/ray/serve/llm/governance.py` are thin `@PublicAPI(stability="alpha")` subclasses of the internal types so `isinstance(public_blocked, InternalBlockedResponse)` stays true. `MiddlewareChain` checks the **internal** `BlockedResponse`.

## Key decisions made

1. **Hook at `OpenAiIngress`, not the engine.** Matches Ray's documented extension path (`ingress_cls_config`). No vLLM/SGLang changes, no `build_openai_app` API change.
2. **List order = execution order.** Cheap checks (ACL, budget) before expensive ones (PII). First block wins.
3. **`after_inference` is optional.** Originally abstract; that made `BudgetMiddleware` uninstantiable (`TypeError` at construct time, not import time). Default is `return response`. Only `before_inference` is abstract.
4. **Streaming v1 is audit-only.** No buffering, no TTFT hit. Full-response scan / redaction is v2 and must be opt-in.
5. **Direct streaming is out of scope.** Existing builder already raises if `ingress_cls_config` is non-default under `RAY_SERVE_LLM_ENABLE_DIRECT_STREAMING`.
6. **Fail-closed on `before`/`after` exceptions; fail-open on `on_inference_complete`.** A broken completion hook must not drop the response the client already received. Chain still runs later completion hooks.
7. **Close both the peek wrapper and the upstream generator.** `_peek_at_generator` returns a replay wrapper. `aclose` on the wrapper does **not** close the LLM generator if the wrapper was never iterated. Callers pass `extra_close=(upstream,)`.
8. **SSE wrapper acloses the inner generator.** `_openai_json_wrapper` does not aclose its source. Without `_sse_stream_with_completion_hook`, `on_inference_complete` would only run on GC, and a client disconnect would skip budget updates.
9. **`OpenAiIngress._get_response` now acloses the DeploymentHandle stream** in a `finally`. Same Python 3.10+ `async for` / `aclose` gap. This is the one change outside `governance/`.
10. **Lazy-import `GovernanceIngress` from the internal package `__init__`.** Ingress imports OpenAI protocol models, which need vLLM or SGLang. Chain/context/middleware unit tests must import the package without an engine. Public `ray.serve.llm.governance` still imports ingress eagerly (users of that module already have Serve LLM).
11. **Reference middleware is internal-only.** `PIIMiddleware` / `BudgetMiddleware` are examples for tests and docs, not a product. Production adapters live in other packages. Budget counters are per ingress replica on purpose.
12. **`ray.serve.llm.governance` is on the API-doc unwalked allowlist** (`ci/ray_ci/doc/cmd_check_api_discrepancy.py`), same as `ray.serve.llm.ingress`. Needed because the symbols are `@PublicAPI` but the module is not walked yet.

## What's next

In priority order:

1. **Human review every changed line.** `AGENTS.md` forbids agent-only PRs. The submitter must be able to defend the streaming aclose paths and the public-API subclass trick.
2. **Push the two local commits** (`git push -u origin HEAD`) only when ready.
3. **Re-run the duplicate-work check**, then open the PR against `ray-project/ray` with:
   - Why this is not duplicating an existing issue/PR (`#65259`, no open PR).
   - Test commands and results.
   - That AI assistance was used.
   - DCO already present (`Signed-off-by: Divyam19 <divyamgupta19@gmail.com>`).
4. **Run the vLLM-coupled tests on Linux/CI.** This Mac venv cannot. Files that need an engine: `test_governance_ingress.py`, `test_governance_e2e.py`, `test_governance_builder.py`, `test_reference_middleware.py` (imports `ChatCompletionRequest`).
5. **Do not start TealTiger/AgentShield in this repo.** Those are separate packages that subclass `LLMMiddleware`.
6. After the foundation PR: populate `estimated_input_tokens`, cluster budget store, streaming full-response scan (v2), per-model middleware config if maintainers want it.

## Known issues

- **Local test story is broken on macOS.** Repo `ray.llm` needs vLLM 0.26; no arm64 wheel. `.venv` has a stale nightly Ray wheel. `python/ray` is partly symlinked via `setup-dev.py`. Full `pytest python/ray/llm/tests/serve/cpu/governance/` will fail at import here. Pure files (`test_middleware_chain.py`, `test_context.py`) can run with `--noconftest` if Ray imports at all.
- **Client disconnect charges nothing.** `on_inference_complete` does fire (that was the harden commit), but usage is `{}` because the final chunk never arrived. `BudgetMiddleware` skips `total_tokens <= 0`. Tokens were spent; budget is not updated. Documented as a v1 limitation, not a silent crash.
- **`test_governance_e2e_budget_allows_then_blocks` is non-streaming only.** Streaming success is a separate SSE smoke test. Do not assume stream usage accounting is e2e-covered.
- **Ingress CPU is on the hot path.** Heavy PII on the ingress replica hurts TTFT. Docs already recommend ~2:1 ingress:LLMServer. Keep reference PII regex-only.
- **Do not put non-serializable objects on the request body** before `_get_response`. The body is pickled across the DeploymentHandle.
- **First generator yield can be a `list` of chunks.** Peek + `substitute_first` already handle this. Do not "simplify" that branch.
- **Commit `4c6c1a710c` subject is sloppy.** Fine to leave; do not rewrite published history unless the user asks.
- A live canvas of the hop-by-hop flow may exist at `~/.cursor/projects/Users-divyamgupta19-Documents-GitHub-ray/canvases/governance-data-flow.canvas.tsx`. Not in the repo.

## Environment notes

- Repo: `/Users/divyamgupta19/Documents/GitHub/ray`
- Branch: `llm-governance-middleware-foundation`
- Python: `.venv` (3.10), nightly `ray==3.0.0.dev0`. Prefer this venv; never install into system Python.
- `ray/llm` in site-packages is typically a symlink to `python/ray/llm`. Governance edits are live without a rebuild. A C++/`ray._raylet` change would need `python/ray/setup-dev.py` + a core rebuild; this branch does not need that.
- Commits already use `-s` (DCO). Keep using `git commit -s`.
- Pre-commit: `pip install -U pre-commit==3.5.0 && pre-commit install` then `pre-commit run`.
- Tests (Linux / CI, from repo root):

```bash
python -m pytest python/ray/llm/tests/serve/cpu/governance/ -v
```

- Tests that can be attempted locally without vLLM:

```bash
.venv/bin/python -m pytest \
  python/ray/llm/tests/serve/cpu/governance/test_middleware_chain.py \
  python/ray/llm/tests/serve/cpu/governance/test_context.py \
  --noconftest -q
```

- Bazel already globs `serve/cpu/**/test_*.py`. No BUILD edit needed for new files under `python/ray/llm/tests/serve/cpu/governance/`.
- Docs: `doc/source/serve/llm/user-guides/governance.md`. Style source of truth is `doc/source/ray-contribute/writing-style.md`.

## Architecture context

Two layers:

1. **Library** (no Serve deployment): `context.py`, `middleware.py`, `chain.py`, `utils.py`, `reference_middleware.py`.
2. **Ingress** (Serve deployment): `GovernanceIngress` subclasses `OpenAiIngress` and is the only place hooks meet HTTP.

`OpenAiIngress.chat` / `completions` / `transcriptions` all call `_process_llm_request`. Overriding that one method covers those three paths. Embeddings/scoring have different methods and are skipped.

`BlockedResponse` is a dataclass, not an exception. Hooks return it. The ingress maps it to an OpenAI-style JSON error:

```json
{"error": {"message": "...", "type": "ACCESS_DENIED", "code": "ACCESS_DENIED"}}
```

THROTTLED always requires `retry_after` (`__post_init__` raises otherwise) and becomes HTTP 429.

## Files of interest

Edit these; ignore the rest of the monorepo.

| File | Role |
|------|------|
| `python/ray/llm/_internal/serve/core/governance/ingress.py` | Runtime. `GovernanceIngress._process_llm_request`, stream wrappers, aclose helpers. Read this first. |
| `python/ray/llm/_internal/serve/core/governance/middleware.py` | `LLMMiddleware` ABC |
| `python/ray/llm/_internal/serve/core/governance/chain.py` | Ordered hook runner |
| `python/ray/llm/_internal/serve/core/governance/context.py` | `RequestContext`, `BlockedResponse`, `build_request_context`, `blocked_response_to_http`, `usage_to_dict` |
| `python/ray/llm/_internal/serve/core/governance/utils.py` | `extract_request_text` / `extract_response_text` for reference PII |
| `python/ray/llm/_internal/serve/core/governance/reference_middleware.py` | `PIIMiddleware`, `BudgetMiddleware` (internal) |
| `python/ray/llm/_internal/serve/core/governance/__init__.py` | Internal exports; lazy `GovernanceIngress` |
| `python/ray/serve/llm/governance.py` | Public alpha API |
| `python/ray/llm/_internal/serve/core/ingress/ingress.py` | Parent `OpenAiIngress`; `_get_response` aclose change |
| `python/ray/llm/_internal/serve/core/ingress/builder.py` | `ingress_cls_config` + direct-streaming rejection (unchanged, but this is the wire-up) |
| `python/ray/llm/_internal/serve/core/ingress/utils.py` | `_peek_at_generator`, `_openai_json_wrapper` (do not break these contracts) |
| `doc/source/serve/llm/user-guides/governance.md` | User guide |
| `python/ray/llm/tests/serve/cpu/governance/` | All tests |
| `python/ray/llm/tests/serve/mocks/mock_vllm_engine.py` | `PiiChatMockEngine` for response-scan e2e |
| `ci/ray_ci/doc/cmd_check_api_discrepancy.py` | Allowlist entry for `ray.serve.llm.governance` |

Prior session that implemented the harden + public API work: [Governance middleware audit](10b66a51-034f-4461-9397-d3d21af30fa9).
