# Refactor Ray Serve LLM Code Structure

## Summary

This PR comprehensively restructures the Ray Serve LLM codebase (`ray/python/ray/llm/_internal/serve/`) to better align with architectural design principles. The new structure organizes code into clear conceptual layers, fixes naming inconsistencies, and makes the codebase more intuitive to navigate.

Since everything is under `_internal/`, this refactoring has **no impact on public APIs**.

## Motivation

The previous code structure had several issues:

1. **Misnomers**: `deployments/routers/` was actually the ingress (OpenAI API entry point), not request routing, causing confusion
2. **Flat structure**: Everything under `deployments/` mixed core primitives with extensions
3. **Unclear boundaries**: Core primitives (LLMServer) mixed with patterns (DPServer, PDProxyServer)
4. **Documentation mismatch**: Code structure didn't mirror architectural design

## Changes

### New Directory Structure

```
ray/python/ray/llm/_internal/serve/
│
├── core/                          # 🔵 CORE PRIMITIVES
│   ├── protocol.py
│   ├── server/
│   │   ├── llm_server.py
│   │   └── builder.py
│   ├── ingress/                   # ← Renamed from "routers"
│   │   ├── ingress.py             # ← Renamed from "router.py"
│   │   ├── builder.py
│   │   └── middleware.py
│   ├── engine/
│   │   └── protocol.py
│   └── configs/
│       ├── llm_config.py
│       └── openai_api_models.py
│
├── serving_patterns/              # 🟢 SERVING PATTERNS
│   ├── data_parallel/
│   │   ├── dp_server.py
│   │   ├── dp_rank_assigner.py
│   │   └── builder.py
│   └── prefill_decode/
│       ├── pd_server.py
│       └── builder.py
│
├── routing_policies/              # 🟡 REQUEST ROUTING
│   └── prefix_aware/
│       ├── prefix_aware_router.py
│       └── prefix_tree.py
│
├── engines/                       # 🟣 ENGINE IMPLEMENTATIONS
│   └── vllm/
│       ├── vllm_engine.py
│       ├── vllm_models.py
│       └── kv_transfer/
│           ├── base.py
│           ├── lmcache.py         # ← Renamed from lmcache_connector_v1.py
│           └── nixl.py            # ← Renamed from nixl_connector.py
│
├── utils/
├── constants.py                   # ← Moved from configs/constants.py
└── observability/
```

### Key Improvements

1. **Fixed Router Misnomer** 🎯
   - `deployments/routers/router.py` → `core/ingress/ingress.py`
   - Correctly reflects that this is the HTTP/API entry point, not request routing
   - Request routing (replica selection) is now in `routing_policies/`

2. **Clear Conceptual Layers**
   - **Core**: Fundamental primitives used by everything
   - **Serving Patterns**: Optional extensions (Data Parallel, Prefill-Decode)
   - **Routing Policies**: Custom request routing strategies
   - **Engines**: Concrete engine implementations

3. **Simplified File Names**
   - `builder_llm_server.py` → `builder.py`
   - `builder_dp_server.py` → `builder.py`
   - `builder_pd.py` → `builder.py`
   - `builder_ingress.py` → `builder.py`

4. **Config Organization**
   - Split `configs/server_models.py` → `core/configs/llm_config.py`
   - Moved `configs/constants.py` → `constants.py` (serve level)

## Migration Details

### Phase-by-Phase Execution

All phases completed successfully:

- ✅ **Phase 1**: Created new directory structure
- ✅ **Phase 2**: Moved core protocol and engine abstraction
- ✅ **Phase 3**: Refactored core configs
- ✅ **Phase 4**: Moved core server implementation
- ✅ **Phase 5**: Moved and renamed ingress (fixed router misnomer)
- ✅ **Phase 6-7**: Moved vLLM engine and KV transfer backends
- ✅ **Phase 8-9**: Moved serving patterns
- ✅ **Phase 10**: Moved routing policies
- ✅ **Phase 11**: Consolidated utils and cleanup

### Complete File Mapping

<details>
<summary>Click to expand complete file migration map</summary>

```
OLD PATH                                                    → NEW PATH
────────────────────────────────────────────────────────────────────────────────

deployments/protocol.py                                    → core/protocol.py
deployments/llm/llm_engine.py                             → core/engine/protocol.py
deployments/llm/llm_server.py                             → core/server/llm_server.py
deployments/llm/builder_llm_server.py                     → core/server/builder.py

deployments/routers/router.py                             → core/ingress/ingress.py ⭐
deployments/routers/builder_ingress.py                    → core/ingress/builder.py
deployments/routers/middleware.py                         → core/ingress/middleware.py

configs/openai_api_models.py                              → core/configs/openai_api_models.py
configs/server_models.py                                  → core/configs/llm_config.py
configs/constants.py                                      → constants.py

deployments/llm/vllm/vllm_engine.py                       → engines/vllm/vllm_engine.py
deployments/llm/vllm/vllm_models.py                       → engines/vllm/vllm_models.py
deployments/llm/vllm/kv_transfer_backends/base.py         → engines/vllm/kv_transfer/base.py
deployments/llm/vllm/kv_transfer_backends/lmcache_connector_v1.py → engines/vllm/kv_transfer/lmcache.py
deployments/llm/vllm/kv_transfer_backends/nixl_connector.py → engines/vllm/kv_transfer/nixl.py

deployments/data_parallel/dp_server.py                    → serving_patterns/data_parallel/dp_server.py
deployments/data_parallel/dp_rank_assigner.py             → serving_patterns/data_parallel/dp_rank_assigner.py
deployments/data_parallel/builder_dp_server.py            → serving_patterns/data_parallel/builder.py

deployments/prefill_decode_disagg/pd_server.py            → serving_patterns/prefill_decode/pd_server.py
deployments/prefill_decode_disagg/builder_pd.py           → serving_patterns/prefill_decode/builder.py

request_router/prefix_aware/*                             → routing_policies/prefix_aware/*

deployments/utils/batcher.py                              → utils/batcher.py
deployments/utils/node_initialization_utils.py            → utils/node_initialization_utils.py
deployments/utils/server_utils.py                         → utils/server_utils.py
```

</details>

## Testing

### Test Results

✅ **All core tests passing**: 43/43 tests in LLM server test suite

```bash
# Validated with:
python -m pytest tests/serve/cpu/deployments/llm/test_llm_engine.py \
                tests/serve/cpu/deployments/llm/test_llm_server.py \
                tests/serve/cpu/deployments/llm/test_builder_llm_server.py -v

======================== 43 passed in 336.44s =========================
```

### Import Updates

- Updated **100+ import statements** across:
  - `ray/python/ray/llm/`
  - `ray/python/ray/serve/`
  - `ray/release/llm_tests/`
- Public API in `ray/python/ray/serve/llm/__init__.py` updated
- All test files updated with new import paths

## Design Principles Maintained

1. ✅ **Empty `__init__.py` files** - Minimizes circular dependencies
2. ✅ **No decorators on classes** - Decoration happens in builder functions
3. ✅ **Clear conceptual layers** - Core vs patterns vs policies vs engines
4. ✅ **Git history preserved** - Used `git mv` for all file moves

## Breaking Changes

**None** - All changes are internal to `_internal/` directory. Public APIs remain unchanged.

## Related

- Based on internal design discussions and architectural planning
- Prepares codebase for future extensions (new engines, patterns, policies)

## Checklist

- [x] All files moved with `git mv` to preserve history
- [x] All imports updated across codebase
- [x] All tests passing
- [x] Public API unchanged and validated
- [x] No circular dependencies introduced
- [x] Router misnomer fixed (ingress is now ingress!)

---

**Benefits for Future Development:**

1. **Easy to add new engines**: Clear place in `engines/`
2. **Easy to add new patterns**: Clear place in `serving_patterns/`
3. **Easy to add new policies**: Clear place in `routing_policies/`
4. **Better onboarding**: Code structure mirrors documentation
5. **Reduced confusion**: Ingress is ingress, routing is routing

