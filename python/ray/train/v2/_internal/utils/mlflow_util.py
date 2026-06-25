"""MLflow tracking utility class (internal use).

Wraps ``MlflowClient`` to provide a unified interface for experiment
management, parameter/metric logging, and artifact tracking.

Key design choices:
- Uses ``MlflowClient`` exclusively (no global ``mlflow.*`` state).
- Graceful degradation: all API calls are wrapped in ``_safe_call``,
  which logs warnings and optionally re-raises based on ``raise_on_error``.
- Nested dict flattening for parameter logging (fixes #33246).
- Param length protection (key <= 250, value <= 500).
- Batch metric logging via ``log_batch`` for performance.
- NaN/Inf filtering for metric values.
"""

from __future__ import annotations

import json
import logging
import math
import time
from typing import Any, Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


class _MLflowTrackerUtil:
    """MLflow tracking utility (internal use).

    Provides experiment management, parameter/metric logging, and artifact
    tracking via ``MlflowClient``. All operations are safe-by-default:
    failures are logged as warnings and do not propagate unless
    ``raise_on_error=True``.

    Args:
        tracking_uri: MLflow tracking server URI. ``None`` uses default.
        raise_on_error: If ``True``, re-raise MLflow API errors instead
            of logging warnings.
    """

    # MLflow server parameter limits.
    _MAX_PARAM_KEY_LEN = 250
    _MAX_PARAM_VALUE_LEN = 500

    def __init__(
        self,
        tracking_uri: Optional[str] = None,
        raise_on_error: bool = False,
    ):
        try:
            from mlflow.tracking import MlflowClient
        except ImportError as err:
            raise ImportError(
                "mlflow is required for MLflowLoggerCallback. "
                "Install with: pip install mlflow"
            ) from err

        self._raise_on_error = raise_on_error
        self._client = MlflowClient(tracking_uri=tracking_uri)

    def _safe_call(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Call an MLflow API method with graceful degradation.

        Args:
            fn: The ``MlflowClient`` method to call.
            *args: Positional arguments forwarded to *fn*.
            **kwargs: Keyword arguments forwarded to *fn*.

        Returns:
            The return value of *fn*, or ``None`` on failure.
        """
        try:
            return fn(*args, **kwargs)
        except Exception as e:
            if self._raise_on_error:
                raise
            logger.warning("MLflow API call failed: %s", e, exc_info=True)
            return None

    # ------------------------------------------------------------------
    # Experiment management
    # ------------------------------------------------------------------

    def setup_experiment(
        self,
        experiment_name: str,
        artifact_location: Optional[str] = None,
    ) -> Optional[str]:
        """Get or create an MLflow experiment.

        Args:
            experiment_name: Experiment name.
            artifact_location: Artifact storage location.

        Returns:
            Experiment ID, or ``None`` on failure.
        """

        def _setup() -> str:
            experiment = self._client.get_experiment_by_name(experiment_name)
            if experiment is None:
                return self._client.create_experiment(
                    experiment_name,
                    artifact_location=artifact_location,
                )
            return experiment.experiment_id

        return self._safe_call(_setup)

    # ------------------------------------------------------------------
    # Run management
    # ------------------------------------------------------------------

    def start_run(
        self,
        experiment_id: str,
        run_name: Optional[str] = None,
        tags: Optional[Dict[str, str]] = None,
    ) -> Optional[str]:
        """Create a new MLflow run.

        Args:
            experiment_id: The experiment to create the run in.
            run_name: Run name.
            tags: Run tags.

        Returns:
            Run ID, or ``None`` on failure.
        """

        def _start() -> str:
            run = self._client.create_run(
                experiment_id=experiment_id,
                run_name=run_name,
                tags=tags,
            )
            return run.info.run_id

        return self._safe_call(_start)

    def end_run(self, run_id: str, status: str = "FINISHED") -> None:
        """Terminate an MLflow run.

        Args:
            run_id: The run to terminate.
            status: Final status (``FINISHED`` or ``FAILED``).
        """
        self._safe_call(self._client.set_terminated, run_id, status=status)

    # ------------------------------------------------------------------
    # Parameter logging
    # ------------------------------------------------------------------

    def log_params(self, run_id: str, params: Dict[str, Any]) -> None:
        """Log parameters with automatic dict flattening and length protection.

        Nested dicts are flattened with ``.`` separator
        (e.g., ``{"a": {"b": 1}}`` becomes ``{"a.b": "1"}``).
        Parameters are sent in a single ``log_batch`` call.

        Args:
            run_id: The run to log parameters to.
            params: Parameter dictionary (may be nested).
        """
        from mlflow.entities import Param

        flat_params = self._flatten_dict(params)
        params_to_log: List[Param] = []
        for key, value in flat_params.items():
            if len(key) > self._MAX_PARAM_KEY_LEN:
                logger.warning(
                    "MLflow param key too long (%d > %d), skipping: %s...",
                    len(key),
                    self._MAX_PARAM_KEY_LEN,
                    key[:50],
                )
                continue
            safe_value = self._to_safe_param_value(value)
            if len(safe_value) > self._MAX_PARAM_VALUE_LEN:
                logger.warning(
                    "MLflow param value too long (%d > %d), truncating key '%s'",
                    len(safe_value),
                    self._MAX_PARAM_VALUE_LEN,
                    key,
                )
                safe_value = (
                    safe_value[: self._MAX_PARAM_VALUE_LEN - 11] + "[truncated]"
                )
            params_to_log.append(Param(key=key, value=safe_value))

        if params_to_log:
            self._safe_call(self._client.log_batch, run_id, params=params_to_log)

    # ------------------------------------------------------------------
    # Metric logging
    # ------------------------------------------------------------------

    def log_metrics_batch(
        self,
        run_id: str,
        metrics: Dict[str, Any],
        step: Optional[int] = None,
    ) -> None:
        """Log metrics in a single batch call.

        Filters out NaN/Inf values and non-numeric types.

        Args:
            run_id: The run to log metrics to.
            metrics: Metric dictionary.
            step: Optional step number.
        """
        from mlflow.entities import Metric

        timestamp = int(time.time() * 1000)
        effective_step = int(step) if step is not None else 0
        metrics_to_log: List[Metric] = []

        for key, value in metrics.items():
            try:
                float_value = float(value)
            except (ValueError, TypeError):
                logger.debug(
                    "Cannot convert metric '%s' with value '%s' to float, skipping.",
                    key,
                    value,
                )
                continue
            if math.isnan(float_value):
                logger.debug("Skipping NaN metric '%s'.", key)
                continue
            if math.isinf(float_value):
                logger.debug("Skipping Inf metric '%s'.", key)
                continue
            metrics_to_log.append(
                Metric(
                    key=key, value=float_value, timestamp=timestamp, step=effective_step
                )
            )

        if metrics_to_log:
            self._safe_call(self._client.log_batch, run_id, metrics=metrics_to_log)

    # ------------------------------------------------------------------
    # Artifact logging
    # ------------------------------------------------------------------

    def log_artifacts(
        self,
        run_id: str,
        local_dir: str,
        artifact_path: Optional[str] = None,
    ) -> None:
        """Log a directory of artifacts.

        Args:
            run_id: The run to log artifacts to.
            local_dir: Local directory containing artifacts.
            artifact_path: Remote subdirectory within the run's artifacts.
        """
        self._safe_call(
            self._client.log_artifacts, run_id, local_dir, artifact_path=artifact_path
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_dict(
        d: Dict[str, Any],
        parent_key: str = "",
        sep: str = ".",
    ) -> Dict[str, Any]:
        """Flatten a nested dict with a separator.

        Args:
            d: Dict to flatten.
            parent_key: Prefix for nested keys.
            sep: Separator between key levels.

        Returns:
            Flattened dict.
        """
        items: List[Tuple[str, Any]] = []
        for k, v in d.items():
            new_key = f"{parent_key}{sep}{k}" if parent_key else k
            if isinstance(v, dict):
                items.extend(_MLflowTrackerUtil._flatten_dict(v, new_key, sep).items())
            else:
                items.append((new_key, v))
        return dict(items)

    @staticmethod
    def _to_safe_param_value(value: Any) -> str:
        """Convert a value to an MLflow-safe string.

        Args:
            value: Any value.

        Returns:
            String representation.
        """
        if isinstance(value, (str, int, float, bool)):
            return str(value)
        try:
            return json.dumps(value, ensure_ascii=False)
        except (TypeError, ValueError):
            return str(value)
