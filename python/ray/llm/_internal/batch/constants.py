from typing import Literal


class TaskType:
    @classmethod
    def values(cls):
        """Return a set of all valid task type values."""
        return {
            value
            for key, value in vars(cls).items()
            if not key.startswith("_") and isinstance(value, str)
        }


class vLLMTaskType(TaskType):
    """The type of task to run on the vLLM engine."""

    # Generate text.
    GENERATE = "generate"

    # Generate embeddings.
    EMBED = "embed"

    # Classification (e.g., sequence classification models).
    CLASSIFY = "classify"

    # Scoring (e.g., cross-encoder models).
    SCORE = "score"


class SGLangTaskType(TaskType):
    """The type of task to run on the SGLang engine."""

    # Generate text.
    GENERATE = "generate"

    # Score items against a query, optionally with embedding overrides.
    SCORE = "score"


# Same value as `sglang.srt.constants.MIS_DELIMITER_TOKEN_ID`; copied so the
# processor and CPU stages don't import SGLang.
SGLANG_MIS_DELIMITER_TOKEN_ID = 9999

TypeVLLMTaskType = Literal[tuple(vLLMTaskType.values())]
TypeSGLangTaskType = Literal[tuple(SGLangTaskType.values())]
