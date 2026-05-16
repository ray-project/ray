# Manual review list: anyscale-ray exact-rule cleanup

Generated 2026-05-15 from the post-cleanup live state (184 total rules).

These are the exact rules that survived the cleanup pass at [DOC-947](https://anyscale1.atlassian.net/browse/DOC-947). Each needs a hand-picked target. The audit script (`audit_live.py` in this directory) could not find a high-confidence (conf-5) source-tree match for any of them, which is why they remain as exact rules with their original targets.

Each rule's current target may be 404, may be content-wrong, or may be fine. Re-probe at session resumption to know which. The audit script's classification of these rules (target_dead vs target_wrong_content) at the time of the last clean audit:

- ~37 had `target_dead` (target final URL returns 404)
- ~67 had `target_wrong_content` (target alive but slug suggests mismatch — heuristic, often a false positive)

Note: many `target_wrong_content` rules are legitimate intentional renames where the page itself was re-titled. Default to keeping the existing target unless you confirm via the source tree that a better destination exists.

## Reviewing one rule

1. Probe the `from` URL on `docs.ray.io` to confirm the rule still has a reason to exist. If the page itself returns 200 today, the rule fires nothing under `force: false` — consider deletion.
2. Probe the current `to` URL. If 200, the rule works. If 404, it's broken.
3. If broken: search the Ray source tree for the topic. The page's intended destination is usually a renamed file under `doc/source/`. Pick the canonical path.
4. Apply via `RtdClient.update_redirect(pk, redirect)` from a session in `/Users/douglas/repos/docs/.claude/worktrees/doc-928-rtd-redirect-download/.agent-workspace/` or via the dashboard.

## Review candidates (34 rules)

| pk | pos | from | current to |
|---|---|---|---|
| 7511 | 18 | `/en/latest/ray-observability/state/ray-state-api-reference.html` | `/en/latest/ray-observability/api/state/api.html` |
| 7510 | 19 | `/en/latest/ray-observability/state/cli.html` | `/en/latest/ray-observability/api/state/cli.html` |
| 7506 | 23 | `/en/latest/ray-air/package-ref.html` | `/en/latest/ray-air/api/api.html` |
| 7481 | 38 | `/en/latest/tune-package-ref.html` | `/en/latest/tune/api_docs/overview.html` |
| 7479 | 40 | `/en/latest/tune-advanced-tutorial.html` | `/en/latest/tune/tutorials/tune-advanced-tutorial.html` |
| 7471 | 47 | `/en/latest/autoscaling.html` | `/en/latest/cluster/launcher.html` |
| 7469 | 48 | `/en/latest/rllib-toc.html` | `/en/latest/rllib/rllib-toc.html` |
| 7468 | 49 | `/en/latest/rllib-training.html` | `/en/latest/rllib/rllib-training.html` |
| 7466 | 50 | `/en/latest/rllib-models.html` | `/en/latest/rllib/rllib-models.html` |
| 7464 | 51 | `/en/latest/rllib-sample-collection.html` | `/en/latest/rllib/rllib-sample-collection.html` |
| 7462 | 52 | `/en/latest/rllib-concepts.html` | `/en/latest/rllib/rllib-concepts.html` |
| 7458 | 53 | `/en/latest/using-ray.html` | `/en/latest/ray-core/using-ray.html` |
| 7451 | 56 | `/en/latest/using-ray-with-gpus.html` | `/en/latest/ray-core/using-ray-with-gpus.html` |
| 7447 | 58 | `/en/latest/troubleshooting.html` | `/en/latest/ray-core/troubleshooting.html` |
| 7445 | 59 | `/en/latest/advanced.html` | `/en/latest/ray-core/advanced.html` |
| 7431 | 63 | `/en/latest/auto_examples/plot_lbfgs.html` | `/en/latest/ray-core/examples/plot_lbfgs.html` |
| 7430 | 64 | `/en/latest/auto_examples/plot_example-lm.html` | `/en/latest/ray-core/examples/plot_example-lm.html` |
| 7428 | 65 | `/en/latest/auto_examples/dask_xgboost/dask_xgboost.html` | `/en/latest/ray-core/examples/dask_xgboost/dask_xgboost.html` |
| 7427 | 66 | `/en/latest/auto_examples/modin_xgboost/modin_xgboost.html` | `/en/latest/ray-core/examples/modin_xgboost/modin_xgboost.html` |
| 7425 | 67 | `/en/latest/auto_examples/plot_example-a3c.html` | `/en/latest/ray-core/examples/plot_example-a3c.html` |
| 7424 | 68 | `/en/latest/auto_examples/using-ray-with-pytorch-lightning.html` | `/en/latest/ray-core/examples/using-ray-with-pytorch-lightning.html` |
| 7416 | 70 | `/en/latest/xgboost-ray.html` | `/en/latest/ray-more-libs/xgboost-ray.html` |
| 7415 | 71 | `/en/latest/lightgbm-ray.html` | `/en/latest/ray-more-libs/lightgbm-ray.html` |
| 7414 | 72 | `/en/latest/ray-lightning.html` | `/en/latest/ray-more-libs/ray-lightning.html` |
| 7402 | 78 | `/en/latest/data/dataset-pipeline.html` | `/en/latest/data/getting-started.html` |
| 7322 | 124 | `/en/latest/rllib/package_ref/evaluation/rllib-concepts.html` | `/en/latest/rllib/package_ref/evaluation.html` |
| 7321 | 125 | `/en/latest/rllib/package_ref/evaluation/rllib-training.html` | `/en/latest/rllib/package_ref/evaluation.html` |
| 7311 | 133 | `/en/latest/rllib-training.htmlexploration-api` | `/en/latest/rllib/rllib-training.html` |
| 7278 | 153 | `/en/latest/serve/tutorial.html` | `/en/latest/serve/tutorials/index.html` |
| 7277 | 154 | `/en/latest/ray-design-patterns/map-reduce.html#pattern-map-and-reduce` | `/en/latest/ray-core/tasks/patterns/map-reduce.html` |
| 7274 | 156 | `/en/latest/cluster/launcher.html` | `/en/latest/cluster/commands.html` |
| 7269 | 159 | `/en/latest/serve/core-apis.html` | `/en/latest/serve/package-ref.html#core-apis` |
| 7267 | 161 | `/en/latest/data/package-ref.html#custom-datasource-api` | `/en/latest/data/api/input_output.html#datasource-api` |
| 7238 | 183 | `/en/latest/workflows/concepts.html` | `/en/latest/workflows/index.html` |

## Intentional version-pinned rules (3 rules, keep as exact)

These three rules pin specific historical releases to canonical paths on `/en/latest/`. They are intentionally `exact` and should not be page-converted.

| pk | pos | from | to |
|---|---|---|---|
| 7518 | 13 | `/en/releases-0.8.7/cluster/launcher.html` | `/en/latest/cluster/getting-started.html` |
| 7514 | 15 | `/en/releases-2.0.0rc0/ray-air/config-scaling.html` | `/en/latest/train/config_guide.html#scaling-configurations-in-train-scalingconfig` |
| 7513 | 16 | `/en/releases-2.0.0rc0/cluster/cluster_under_construction/getting-started.html` | `/en/latest/cluster/vms/getting-started.html` |
