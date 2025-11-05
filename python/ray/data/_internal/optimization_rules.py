"""Enterprise optimization rules for Ray Data based on SLA expectations.

This module provides a rule-based system for applying optimizations based on
enterprise requirements expressed through the expectations API. Rules are
organized by optimization strategy and are easy to understand and maintain.
"""

from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, TYPE_CHECKING

from ray.util.annotations import DeveloperAPI

if TYPE_CHECKING:
    from ray.data.expectations import OptimizationStrategy
    from ray.data._internal.execution.interfaces.execution_options import (
        ExecutionResources,
    )


@dataclass
class OptimizationRule:
    """A single optimization rule that applies adjustments based on strategy.

    Rules are declarative and easy to understand. Each rule specifies:
    - What to adjust (e.g., "resource_limits", "batch_size")
    - How to adjust it based on strategy
    - When to apply it (optional conditions)

    Attributes:
        name: Human-readable name for this rule.
        description: What this rule does.
        apply_fn: Function that takes context and returns adjusted value.
        condition_fn: Optional function that determines if rule should apply.
        priority: Lower numbers are applied first (default: 100).
    """

    name: str
    description: str
    apply_fn: Callable[[Any, Any], Any]
    condition_fn: Optional[Callable[[Any], bool]] = None
    priority: int = 100

    def should_apply(self, context: Any) -> bool:
        """Check if this rule should be applied."""
        if self.condition_fn is None:
            return True
        return self.condition_fn(context)


@dataclass
class OptimizationContext:
    """Context for applying optimization rules.

    Contains all the information needed to make optimization decisions.
    """

    strategy: "OptimizationStrategy"
    max_execution_time_seconds: Optional[float] = None
    elapsed_time: float = 0.0
    base_resource_limits: Optional["ExecutionResources"] = None
    total_resources: Optional["ExecutionResources"] = None
    base_batch_size: Optional[int] = None
    base_parallelism: Optional[int] = None
    base_object_store_memory_fraction: float = 0.25
    base_reservation_ratio: float = 0.5
    base_num_cpus: Optional[float] = None
    operation_type: Optional[str] = None  # "cpu_intensive", "io_bound", etc.

    def get_time_ratio(self) -> Optional[float]:
        """Get ratio of remaining time (0.0 = deadline, 1.0 = just started)."""
        if self.max_execution_time_seconds is None:
            return None
        if self.max_execution_time_seconds <= 0:
            return 0.0
        remaining = self.max_execution_time_seconds - self.elapsed_time
        return max(0.0, min(1.0, remaining / self.max_execution_time_seconds))

    def is_deadline_approaching(self, threshold: float = 0.3) -> bool:
        """Check if deadline is approaching (less than threshold% time remaining)."""
        ratio = self.get_time_ratio()
        return ratio is not None and ratio < threshold


class OptimizationRulesEngine:
    """Engine for applying optimization rules based on enterprise requirements.

    This engine uses a rule-based approach to make optimization decisions.
    Rules are organized by optimization strategy and are easy to understand
    and maintain.

    Example:
        >>> engine = OptimizationRulesEngine()
        >>> context = OptimizationContext(
        ...     strategy=OptimizationStrategy.PERFORMANCE,
        ...     max_execution_time_seconds=300,
        ...     base_resource_limits=ExecutionResources.for_limits()
        ... )
        >>> adjusted_limits = engine.adjust_resource_limits(context)
    """

    def __init__(self):
        """Initialize the rules engine with default rules."""
        self._rules: Dict[str, List[OptimizationRule]] = {
            "resource_limits": self._create_resource_limits_rules(),
            "batch_size": self._create_batch_size_rules(),
            "parallelism": self._create_parallelism_rules(),
            "object_store_memory": self._create_object_store_memory_rules(),
            "reservation_ratio": self._create_reservation_ratio_rules(),
            "num_cpus": self._create_num_cpus_rules(),
        }

    def adjust_resource_limits(
        self, context: OptimizationContext
    ) -> Optional["ExecutionResources"]:
        """Adjust resource limits based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted resource limits, or None if no base limits provided.
        """
        if context.base_resource_limits is None:
            return None

        return self._apply_rules(
            "resource_limits",
            context.base_resource_limits,
            context,
        )

    def adjust_batch_size(self, context: OptimizationContext) -> Optional[int]:
        """Adjust batch size based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted batch size, or None if no base batch size provided.
        """
        if context.base_batch_size is None:
            return None

        return self._apply_rules("batch_size", context.base_batch_size, context)

    def adjust_parallelism(self, context: OptimizationContext) -> Optional[int]:
        """Adjust parallelism based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted parallelism, or None if no base parallelism provided.
        """
        if context.base_parallelism is None:
            return None

        return self._apply_rules("parallelism", context.base_parallelism, context)

    def adjust_object_store_memory_fraction(
        self, context: OptimizationContext
    ) -> float:
        """Adjust object store memory fraction based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted object store memory fraction.
        """
        return self._apply_rules(
            "object_store_memory",
            context.base_object_store_memory_fraction,
            context,
        )

    def adjust_reservation_ratio(self, context: OptimizationContext) -> float:
        """Adjust resource reservation ratio based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted reservation ratio.
        """
        return self._apply_rules(
            "reservation_ratio", context.base_reservation_ratio, context
        )

    def adjust_num_cpus(self, context: OptimizationContext) -> Optional[float]:
        """Adjust num_cpus per task based on optimization strategy.

        Args:
            context: The optimization context.

        Returns:
            Adjusted num_cpus, or None if no base num_cpus provided.
        """
        if context.base_num_cpus is None:
            return None

        return self._apply_rules("num_cpus", context.base_num_cpus, context)

    def _apply_rules(
        self, rule_type: str, base_value: Any, context: OptimizationContext
    ) -> Any:
        """Apply rules of a specific type to adjust a value.

        Args:
            rule_type: Type of rules to apply.
            base_value: Base value to adjust.
            context: Optimization context.

        Returns:
            Adjusted value.
        """
        rules = self._rules.get(rule_type, [])
        # Sort by priority (lower priority = applied first)
        sorted_rules = sorted(rules, key=lambda r: r.priority)

        value = base_value
        for rule in sorted_rules:
            if rule.should_apply(context):
                value = rule.apply_fn(value, context)

        return value

    def _create_resource_limits_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting resource limits."""
        from ray.data.expectations import OptimizationStrategy

        def performance_multiplier(value: Any, context: OptimizationContext) -> Any:
            """Increase resources for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value

            # If deadline is approaching, be even more aggressive
            if context.is_deadline_approaching(threshold=0.3):
                multiplier = 1.5
            elif context.is_deadline_approaching(threshold=0.5):
                multiplier = 1.3
            else:
                multiplier = 1.2

            # Cap at 90% of total resources to leave headroom
            if context.total_resources is not None:
                return value.scale(multiplier).min(context.total_resources.scale(0.9))
            return value.scale(multiplier)

        def cost_multiplier(value: Any, context: OptimizationContext) -> Any:
            """Decrease resources for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            return value.scale(0.7)

        return [
            OptimizationRule(
                name="performance_resource_boost",
                description="Increase resource limits for PERFORMANCE strategy",
                apply_fn=performance_multiplier,
                priority=10,
            ),
            OptimizationRule(
                name="cost_resource_reduction",
                description="Decrease resource limits for COST strategy",
                apply_fn=cost_multiplier,
                priority=10,
            ),
        ]

    def _create_batch_size_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting batch sizes."""
        from ray.data.expectations import OptimizationStrategy

        def performance_batch_size(value: int, context: OptimizationContext) -> int:
            """Increase batch size for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value

            # Larger batches for better throughput
            # If deadline is tight, use even larger batches
            if context.is_deadline_approaching(threshold=0.3):
                multiplier = 1.5
            else:
                multiplier = 1.2

            return max(1, int(value * multiplier))

        def cost_batch_size(value: int, context: OptimizationContext) -> int:
            """Decrease batch size for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            # Smaller batches reduce memory usage
            return max(1, int(value * 0.7))

        return [
            OptimizationRule(
                name="performance_batch_size_increase",
                description="Increase batch size for PERFORMANCE strategy",
                apply_fn=performance_batch_size,
                priority=10,
            ),
            OptimizationRule(
                name="cost_batch_size_decrease",
                description="Decrease batch size for COST strategy",
                apply_fn=cost_batch_size,
                priority=10,
            ),
        ]

    def _create_parallelism_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting parallelism."""
        from ray.data.expectations import OptimizationStrategy

        def performance_parallelism(value: int, context: OptimizationContext) -> int:
            """Increase parallelism for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value

            # More parallelism for faster execution
            if context.is_deadline_approaching(threshold=0.3):
                multiplier = 1.5
            else:
                multiplier = 1.2

            return max(1, int(value * multiplier))

        def cost_parallelism(value: int, context: OptimizationContext) -> int:
            """Decrease parallelism for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            # Less parallelism reduces resource usage
            return max(1, int(value * 0.7))

        return [
            OptimizationRule(
                name="performance_parallelism_increase",
                description="Increase parallelism for PERFORMANCE strategy",
                apply_fn=performance_parallelism,
                priority=10,
            ),
            OptimizationRule(
                name="cost_parallelism_decrease",
                description="Decrease parallelism for COST strategy",
                apply_fn=cost_parallelism,
                priority=10,
            ),
        ]

    def _create_object_store_memory_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting object store memory fraction."""
        from ray.data.expectations import OptimizationStrategy

        def performance_memory(value: float, context: OptimizationContext) -> float:
            """Increase object store memory for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value
            # More memory for better caching
            return min(value * 1.5, 0.5)  # Cap at 50%

        def cost_memory(value: float, context: OptimizationContext) -> float:
            """Decrease object store memory for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            # Less memory to reduce costs
            return max(value * 0.7, 0.15)  # At least 15%

        return [
            OptimizationRule(
                name="performance_memory_increase",
                description="Increase object store memory for PERFORMANCE strategy",
                apply_fn=performance_memory,
                priority=10,
            ),
            OptimizationRule(
                name="cost_memory_decrease",
                description="Decrease object store memory for COST strategy",
                apply_fn=cost_memory,
                priority=10,
            ),
        ]

    def _create_reservation_ratio_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting resource reservation ratio."""
        from ray.data.expectations import OptimizationStrategy

        def performance_reservation(
            value: float, context: OptimizationContext
        ) -> float:
            """Increase reservation ratio for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value
            # Higher reservation for more predictable throughput
            return 0.6

        def cost_reservation(value: float, context: OptimizationContext) -> float:
            """Decrease reservation ratio for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            # Lower reservation for better resource sharing
            return 0.4

        return [
            OptimizationRule(
                name="performance_reservation_increase",
                description="Increase reservation ratio for PERFORMANCE strategy",
                apply_fn=performance_reservation,
                priority=10,
            ),
            OptimizationRule(
                name="cost_reservation_decrease",
                description="Decrease reservation ratio for COST strategy",
                apply_fn=cost_reservation,
                priority=10,
            ),
        ]

    def _create_num_cpus_rules(self) -> List[OptimizationRule]:
        """Create rules for adjusting num_cpus per task."""
        from ray.data.expectations import OptimizationStrategy

        def performance_num_cpus(value: float, context: OptimizationContext) -> float:
            """Increase num_cpus for PERFORMANCE strategy."""
            if context.strategy != OptimizationStrategy.PERFORMANCE:
                return value

            # More CPUs per task for faster execution
            multiplier = 1.5 if context.is_deadline_approaching() else 1.2
            return value * multiplier

        def cost_num_cpus(value: float, context: OptimizationContext) -> float:
            """Decrease num_cpus for COST strategy."""
            if context.strategy != OptimizationStrategy.COST:
                return value
            # Fewer CPUs per task allows more parallelism with same resources
            return max(0.1, value * 0.7)

        return [
            OptimizationRule(
                name="performance_num_cpus_increase",
                description="Increase num_cpus for PERFORMANCE strategy",
                apply_fn=performance_num_cpus,
                priority=10,
            ),
            OptimizationRule(
                name="cost_num_cpus_decrease",
                description="Decrease num_cpus for COST strategy",
                apply_fn=cost_num_cpus,
                priority=10,
            ),
        ]

    def add_rule(self, rule_type: str, rule: OptimizationRule) -> None:
        """Add a custom rule to the engine.

        Args:
            rule_type: Type of rule (e.g., "resource_limits", "batch_size").
            rule: The rule to add.
        """
        if rule_type not in self._rules:
            self._rules[rule_type] = []
        self._rules[rule_type].append(rule)


# Global rules engine instance
_OPTIMIZATION_RULES_ENGINE: Optional[OptimizationRulesEngine] = None


@DeveloperAPI
def get_optimization_rules_engine() -> OptimizationRulesEngine:
    """Get the global optimization rules engine instance.

    Returns:
        The global OptimizationRulesEngine instance.
    """
    global _OPTIMIZATION_RULES_ENGINE
    if _OPTIMIZATION_RULES_ENGINE is None:
        _OPTIMIZATION_RULES_ENGINE = OptimizationRulesEngine()
    return _OPTIMIZATION_RULES_ENGINE
