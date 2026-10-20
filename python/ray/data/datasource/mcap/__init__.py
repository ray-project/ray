"""Building blocks of ``read_mcap(samples=...)``'s ``TopicSpec``.

``steps`` holds the rules for where a topic's steps fall, and ``match`` the
rules for which message fills each step::

    from ray.data.datasource import TopicSpec
    from ray.data.datasource.mcap import match, steps

    TopicSpec(steps.before(1, fps=5), match.nearest())
"""

from ray.data.datasource.mcap import match, steps

__all__ = ["match", "steps"]
