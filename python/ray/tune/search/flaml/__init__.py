from ray.tune.search.flaml.flaml_search import CFO, BlendSearch

from ray._private.usage.usage_lib import TagKey, record_extra_usage_tag

record_extra_usage_tag(TagKey.TUNE_SEARCHER, "FLAML")

__all__ = ["BlendSearch", "CFO"]
