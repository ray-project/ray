from collections import OrderedDict

DEFAULT_FUSION_PATTERNS = OrderedDict()


def register_fusion_pattern(fusion_pattern):
    """Registers a fusion pattern function for a specified layer type in the DEFAULT_FUSION_PATTERNS dictionary."""
    layer_type = fusion_pattern.name

    if layer_type in DEFAULT_FUSION_PATTERNS.keys():
        raise
    DEFAULT_FUSION_PATTERNS[layer_type] = fusion_pattern


def get_fusion_patterns(skip_fusion_patterns: str = None):
    """Returns a copy of the default fusion patterns, optionally excluding specific patterns."""
    default_fusion_patterns = DEFAULT_FUSION_PATTERNS.copy()
    if skip_fusion_patterns:
        for pattern in skip_fusion_patterns:
            default_fusion_patterns.pop(pattern)

    return default_fusion_patterns


from .elimination import *
from .fusion import *
