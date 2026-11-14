"""The inputs of ``read_mcap(samples=...)``: ``TopicSpec`` and its rules.

A ``TopicSpec`` says how a sample reads one topic. ``steps`` holds the rules
for where the topic's steps fall, and ``match`` the rules for which message
fills each step::

    from ray.data.datasource.mcap import TopicSpec, match, steps

    TopicSpec(steps.before(1, fps=5), match.nearest())
"""

from ray.data._internal.datasource_v2.formats.mcap.mcap_topic_spec import TopicSpec
from ray.data.datasource.mcap import match, steps

__all__ = ["TopicSpec", "match", "steps"]
