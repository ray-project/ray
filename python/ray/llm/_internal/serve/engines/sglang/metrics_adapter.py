"""SGLang engine metrics adapter for Ray Serve LLM.

Bridges SGLang's prometheus_client multiprocess output to ray.util.metrics
so SGLang engine metrics appear on Ray's Prometheus endpoint with the same
single flag (``log_engine_metrics``) as vLLM users already have.

Mechanism:

1. ``claim_multiproc_dir()`` runs before ``import sglang`` and sets
   ``PROMETHEUS_MULTIPROC_DIR`` to a replica-owned tempdir. SGLang's
   ``set_prometheus_multiproc_dir()`` respects an existing env value, so
   we win the race deterministically. Verified upstream that Ray does not
   use this env elsewhere and SGLang does not overwrite it.

2. ``start()`` launches an asyncio background task. Every
   ``scrape_interval_seconds`` it instantiates a fresh
   ``MultiProcessCollector`` against the multiproc dir, walks every metric
   family that SGLang's ``TokenizerMetricsCollector`` /
   ``SchedulerMetricsCollector`` wrote there, and re-emits each sample
   through ``ray.util.metrics.{Counter, Gauge}``. Ray Serve appends
   ``application`` / ``deployment`` / ``replica`` labels automatically.

3. ``stop()`` cancels the loop. The multiproc tempdir is left for the OS
   to clean up so adapter shutdown does not block replica teardown.

Counter samples are converted to deltas because ``ray.util.metrics.Counter``
takes increments while ``prometheus_client`` exposes cumulative totals.
Histogram samples are emitted as raw ``_bucket`` / ``_sum`` / ``_count``
gauges with the ``le`` label preserved so PromQL ``histogram_quantile()``
keeps working on the renamed metric. A switch to delta-replay into a true
``ray.util.metrics.Histogram`` is deferred until cardinality measurement.

The adapter publishes its own observability metrics under
``ray_serve_sglang_adapter_*`` so operators can detect scrape failures,
counter resets, and label-schema mismatches without reading replica logs.

Tracking issue: https://github.com/ray-project/ray/issues/62791
"""
import asyncio
import logging
import os
import tempfile
import time
from typing import Any, Dict, Optional, Set, Tuple

logger = logging.getLogger(__name__)

DEFAULT_SCRAPE_INTERVAL_SECONDS = 10.0

_SCRAPE_INTERVAL_ENV = "RAY_SGLANG_METRICS_SCRAPE_INTERVAL_SECONDS"
_MULTIPROC_ENV = "PROMETHEUS_MULTIPROC_DIR"
_SGLANG_METRIC_PREFIX = "sglang:"
_RAY_METRIC_PREFIX = "ray_serve_sglang_"

# Defensive caps. SGLang publishes ~100 metric families; per-family label
# combinations are bounded by feature flags (LoRA, hicache, PD, ...). The
# caps below add 2-3x headroom and exist purely to contain a runaway
# upstream change rather than to enforce a quota.
_MAX_METRICS = 512
_MAX_LABEL_COMBINATIONS_PER_METRIC = 4096

# prometheus_client family.type values that we know how to forward. Anything
# else (summary / info / stateset / unknown) is silently skipped because
# SGLang does not emit them today.
_HANDLED_FAMILY_TYPES = frozenset({"counter", "gauge", "histogram"})


def _rename_metric(sglang_name: str) -> str:
    """Convert ``sglang:foo_total`` to ``ray_serve_sglang_foo_total``.

    ``ray.util.metrics`` rejects ``:`` in metric names; only ``[a-zA-Z0-9_]``
    is allowed, so the prefix swap doubles as a sanitization step.
    """
    if sglang_name.startswith(_SGLANG_METRIC_PREFIX):
        suffix = sglang_name[len(_SGLANG_METRIC_PREFIX) :]
    else:
        suffix = sglang_name
    return _RAY_METRIC_PREFIX + suffix


def _read_scrape_interval(default: float = DEFAULT_SCRAPE_INTERVAL_SECONDS) -> float:
    """Resolve scrape interval, allowing operator override via env."""
    raw = os.environ.get(_SCRAPE_INTERVAL_ENV)
    if not raw:
        return default
    try:
        value = float(raw)
    except ValueError:
        logger.warning(
            "Invalid %s=%r; falling back to default %.1fs.",
            _SCRAPE_INTERVAL_ENV,
            raw,
            default,
        )
        return default
    if value < 1.0:
        logger.warning(
            "%s=%r is below the 1s floor; clamping to 1s.",
            _SCRAPE_INTERVAL_ENV,
            raw,
        )
        return 1.0
    return value


class SGLangMetricsAdapter:
    """Background scraper that mirrors SGLang multiproc metrics into Ray.

    Multi-TP is supported transparently: every SGLang scheduler subprocess
    inherits the same multiproc dir from the parent env and writes its
    samples with a ``tp_rank`` label, so a single adapter per replica
    aggregates all ranks. The single adapter assumption matches the one-
    adapter-per-replica lifecycle wired up in ``SGLangServer``.
    """

    def __init__(
        self,
        scrape_interval_seconds: Optional[float] = None,
    ):
        if scrape_interval_seconds is None:
            scrape_interval_seconds = _read_scrape_interval()
        self._scrape_interval = scrape_interval_seconds
        self._task: Optional[asyncio.Task] = None
        self._stopping = False

        # (sample_ray_name, label_keys_tuple, label_vals_tuple) -> last
        # observed cumulative counter value. Used to compute deltas.
        self._last_counter_values: Dict[Tuple, float] = {}

        # Lazy ray.util.metrics objects keyed by ray metric name.
        self._counters: Dict[str, Any] = {}
        self._gauges: Dict[str, Any] = {}

        # First observation fixes a metric's tag_keys. Later samples with
        # a different tag_keys schema are dropped (with one log) instead
        # of crashing the loop on a ray.util.metrics validation error.
        self._tag_keys: Dict[str, Tuple[str, ...]] = {}
        self._schema_mismatch_logged: Set[str] = set()

        # Cardinality caps + counters (drop reasons feed self-metrics).
        self._label_combos: Dict[str, Set[Tuple[Tuple[str, ...], Tuple[str, ...]]]] = {}
        self._dropped_metric_cap_logged = False
        self._dropped_combo_cap_logged: Set[str] = set()

        # Self-metrics. Lazily created on first scrape so they appear in
        # the same registry as the forwarded metrics.
        self._self_scrapes_total: Optional[Any] = None
        self._self_emitted_total: Optional[Any] = None
        self._self_dropped_total: Optional[Any] = None
        self._self_last_scrape_seconds: Optional[Any] = None
        self._self_scrape_duration_seconds: Optional[Any] = None

    # ------------------------------------------------------------------
    # Setup helpers
    # ------------------------------------------------------------------

    @staticmethod
    def claim_multiproc_dir(env_name: str = _MULTIPROC_ENV) -> str:
        """Set ``PROMETHEUS_MULTIPROC_DIR`` before SGLang imports prometheus_client.

        Returns the directory path. If the env is already set (e.g. by the
        deployment's runtime config), reuse it after ensuring the directory
        exists. Otherwise create a tempdir owned by this replica.

        Must run *before* ``import sglang`` so SGLang's
        ``set_prometheus_multiproc_dir()`` sees the env and reuses our path
        instead of allocating its own.
        """
        existing = os.environ.get(env_name)
        if existing:
            os.makedirs(existing, exist_ok=True)
            return existing
        path = tempfile.mkdtemp(prefix="ray_sglang_metrics_")
        os.environ[env_name] = path
        logger.info("SGLang metrics adapter claimed multiproc dir: %s", path)
        return path

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def start(self) -> None:
        """Launch the background scrape loop. Idempotent."""
        if self._task is not None and not self._task.done():
            return
        self._stopping = False
        self._task = asyncio.create_task(self._scrape_loop())

    async def stop(self) -> None:
        """Cancel the scrape loop. Safe to call multiple times."""
        self._stopping = True
        task = self._task
        if task is None:
            return
        self._task = None
        task.cancel()
        try:
            await task
        except (asyncio.CancelledError, Exception):
            pass

    # ------------------------------------------------------------------
    # Scrape loop
    # ------------------------------------------------------------------

    async def _scrape_loop(self) -> None:
        try:
            from prometheus_client import CollectorRegistry
            from prometheus_client.multiprocess import MultiProcessCollector
        except ImportError as e:
            logger.warning(
                "prometheus_client not available; SGLang metrics adapter is "
                "a no-op. Install prometheus-client to enable. (%s)",
                e,
            )
            return

        # Defer collector construction so SGLang's collectors have a chance
        # to write at least one sample before we read.
        registry: Optional[CollectorRegistry] = None

        while not self._stopping:
            if registry is None:
                registry = self._build_registry(
                    CollectorRegistry, MultiProcessCollector
                )

            if registry is not None:
                self._scrape_iteration(registry)

            try:
                await asyncio.sleep(self._scrape_interval)
            except asyncio.CancelledError:
                raise

    def _build_registry(
        self, CollectorRegistry: Any, MultiProcessCollector: Any
    ) -> Optional[Any]:
        try:
            registry = CollectorRegistry()
            MultiProcessCollector(registry)
            return registry
        except Exception:
            logger.exception(
                "Failed to set up SGLang multiproc collector. Adapter idle "
                "this cycle; will retry on next interval."
            )
            return None

    def _scrape_iteration(self, registry: Any) -> None:
        start_t = time.monotonic()
        outcome = "success"
        emitted = 0
        try:
            emitted = self._scrape_once(registry)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("SGLang metrics scrape iteration failed")
            outcome = "error"
        duration = time.monotonic() - start_t
        self._update_self_metrics(outcome=outcome, emitted=emitted, duration=duration)

    def _scrape_once(self, registry: Any) -> int:
        """Walk every family once and forward to ray.util.metrics.

        Returns the number of samples emitted.
        """
        emitted = 0
        for family in registry.collect():
            family_type = getattr(family, "type", None)
            if family_type not in _HANDLED_FAMILY_TYPES:
                continue

            family_name = getattr(family, "name", "")
            if family_name.startswith(_RAY_METRIC_PREFIX):
                # Skip self-metrics in case the multiproc dir is ever shared
                # with Ray's own writers.
                continue

            if family_type == "counter":
                emitted += self._handle_counter_family(family)
            elif family_type == "gauge":
                emitted += self._handle_gauge_family(family)
            elif family_type == "histogram":
                emitted += self._handle_histogram_family(family)
        return emitted

    # ------------------------------------------------------------------
    # Family handlers
    # ------------------------------------------------------------------

    @staticmethod
    def _split_labels(
        sample_labels: Dict[str, str],
    ) -> Tuple[Tuple[str, ...], Tuple[str, ...]]:
        keys = tuple(sorted(sample_labels.keys()))
        vals = tuple(sample_labels[k] for k in keys)
        return keys, vals

    def _handle_counter_family(self, family: Any) -> int:
        from ray.util.metrics import Counter

        emitted = 0
        for sample in family.samples:
            sample_ray_name = _rename_metric(sample.name)
            label_keys, label_vals = self._split_labels(sample.labels)

            cache_key = (sample_ray_name, label_keys, label_vals)
            previous = self._last_counter_values.get(cache_key)
            current = float(sample.value)
            if previous is None:
                # Baseline: do not emit until we have a delta.
                self._last_counter_values[cache_key] = current
                continue

            delta = current - previous
            self._last_counter_values[cache_key] = current
            if delta < 0:
                # Counter reset (engine restart / replica refresh).
                self._record_drop("counter_reset")
                continue
            if delta == 0:
                continue

            metric = self._get_or_create(
                self._counters,
                sample_ray_name,
                label_keys,
                family.documentation,
                Counter,
            )
            if metric is None:
                continue
            if not self._labels_match_schema(sample_ray_name, label_keys, label_vals):
                continue

            try:
                metric.inc(delta, tags=dict(zip(label_keys, label_vals)))
                emitted += 1
            except Exception:
                self._record_drop("emit_error")
                logger.exception(
                    "Counter emit failed for %s; sample dropped.", sample_ray_name
                )
        return emitted

    def _handle_gauge_family(self, family: Any) -> int:
        return self._emit_as_gauge(family)

    def _handle_histogram_family(self, family: Any) -> int:
        # Phase 1 strategy: emit raw bucket / sum / count samples as gauges.
        # The ``le`` label is preserved on _bucket samples so PromQL
        # ``histogram_quantile()`` keeps working on the renamed metric.
        return self._emit_as_gauge(family)

    def _emit_as_gauge(self, family: Any) -> int:
        from ray.util.metrics import Gauge

        emitted = 0
        for sample in family.samples:
            sample_ray_name = _rename_metric(sample.name)
            label_keys, label_vals = self._split_labels(sample.labels)

            metric = self._get_or_create(
                self._gauges,
                sample_ray_name,
                label_keys,
                family.documentation,
                Gauge,
            )
            if metric is None:
                continue
            if not self._labels_match_schema(sample_ray_name, label_keys, label_vals):
                continue

            try:
                metric.set(float(sample.value), tags=dict(zip(label_keys, label_vals)))
                emitted += 1
            except Exception:
                self._record_drop("emit_error")
                logger.exception(
                    "Gauge emit failed for %s; sample dropped.", sample_ray_name
                )
        return emitted

    # ------------------------------------------------------------------
    # Cardinality + schema guards
    # ------------------------------------------------------------------

    def _get_or_create(
        self,
        cache: Dict[str, Any],
        name: str,
        label_keys: Tuple[str, ...],
        documentation: str,
        cls: Any,
    ) -> Optional[Any]:
        existing = cache.get(name)
        if existing is not None:
            return existing

        if len(self._tag_keys) >= _MAX_METRICS:
            if not self._dropped_metric_cap_logged:
                logger.warning(
                    "SGLang metrics adapter hit metric cap (%d). New metric "
                    "%s dropped. Existing metrics keep emitting.",
                    _MAX_METRICS,
                    name,
                )
                self._dropped_metric_cap_logged = True
            self._record_drop("metric_cap")
            return None

        try:
            metric = cls(
                name,
                description=documentation or name,
                tag_keys=label_keys,
            )
        except Exception:
            self._record_drop("create_error")
            logger.exception(
                "Failed to create ray.util.metrics object for %s; samples dropped.",
                name,
            )
            return None

        cache[name] = metric
        self._tag_keys[name] = label_keys
        self._label_combos.setdefault(name, set())
        return metric

    def _labels_match_schema(
        self,
        name: str,
        label_keys: Tuple[str, ...],
        label_vals: Tuple[str, ...],
    ) -> bool:
        expected = self._tag_keys.get(name)
        if expected is None:
            return False  # Should not happen — caller created via _get_or_create.

        if expected != label_keys:
            if name not in self._schema_mismatch_logged:
                logger.warning(
                    "Label-schema mismatch for %s: expected %s, got %s. "
                    "Sample dropped; further mismatches for this metric will "
                    "be silent.",
                    name,
                    expected,
                    label_keys,
                )
                self._schema_mismatch_logged.add(name)
            self._record_drop("schema_mismatch")
            return False

        combos = self._label_combos.setdefault(name, set())
        combo = (label_keys, label_vals)
        if combo not in combos:
            if len(combos) >= _MAX_LABEL_COMBINATIONS_PER_METRIC:
                if name not in self._dropped_combo_cap_logged:
                    logger.warning(
                        "SGLang metrics adapter hit label-combo cap (%d) for "
                        "%s. New label combinations will be dropped until "
                        "replica restart.",
                        _MAX_LABEL_COMBINATIONS_PER_METRIC,
                        name,
                    )
                    self._dropped_combo_cap_logged.add(name)
                self._record_drop("label_cap")
                return False
            combos.add(combo)
        return True

    # ------------------------------------------------------------------
    # Self-metrics
    # ------------------------------------------------------------------

    def _ensure_self_metrics(self) -> None:
        if self._self_scrapes_total is not None:
            return
        try:
            from ray.util.metrics import Counter, Gauge

            self._self_scrapes_total = Counter(
                _RAY_METRIC_PREFIX + "adapter_scrapes_total",
                description="Total scrape iterations performed by the SGLang metrics adapter.",
                tag_keys=("outcome",),
            )
            self._self_emitted_total = Counter(
                _RAY_METRIC_PREFIX + "adapter_emitted_samples_total",
                description="Total SGLang samples re-emitted to ray.util.metrics.",
            )
            self._self_dropped_total = Counter(
                _RAY_METRIC_PREFIX + "adapter_dropped_samples_total",
                description=(
                    "Total samples dropped by the SGLang metrics adapter. "
                    "Reasons: counter_reset, schema_mismatch, metric_cap, "
                    "label_cap, emit_error, create_error."
                ),
                tag_keys=("reason",),
            )
            self._self_last_scrape_seconds = Gauge(
                _RAY_METRIC_PREFIX + "adapter_last_scrape_unix_seconds",
                description="Unix timestamp of the most recent scrape iteration.",
            )
            self._self_scrape_duration_seconds = Gauge(
                _RAY_METRIC_PREFIX + "adapter_scrape_duration_seconds",
                description="Wall-clock duration of the most recent scrape iteration.",
            )
        except Exception:
            logger.exception(
                "Failed to register SGLang adapter self-metrics; continuing."
            )

    def _update_self_metrics(self, outcome: str, emitted: int, duration: float) -> None:
        self._ensure_self_metrics()
        try:
            if self._self_scrapes_total is not None:
                self._self_scrapes_total.inc(1, tags={"outcome": outcome})
            if self._self_emitted_total is not None and emitted:
                self._self_emitted_total.inc(emitted)
            if self._self_last_scrape_seconds is not None:
                self._self_last_scrape_seconds.set(time.time())
            if self._self_scrape_duration_seconds is not None:
                self._self_scrape_duration_seconds.set(duration)
        except Exception:
            logger.exception("SGLang adapter self-metrics update failed.")

    def _record_drop(self, reason: str) -> None:
        self._ensure_self_metrics()
        if self._self_dropped_total is None:
            return
        try:
            self._self_dropped_total.inc(1, tags={"reason": reason})
        except Exception:
            logger.exception(
                "SGLang adapter dropped-sample counter failed for reason=%s.", reason
            )
