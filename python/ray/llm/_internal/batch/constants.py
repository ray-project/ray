from typing import Literal

class vLLMTaskType:
    """The type of task to run on the vLLM engine."""

    # Generate text.
    GENERATE = "generate"

    # Generate embeddings.
    EMBED = "embed"

    # Classification (e.g., sequence classification models).
    CLASSIFY = "classify"

    # Scoring (e.g., cross-encoder models).
    SCORE = "score"

    @classmethod
    def values(cls):
        """Return a set of all valid task type values."""
        return {
            value
            for key, value in vars(cls).items()
            if not key.startswith("_") and isinstance(value, str)
        }

TypeVLLMTaskType = Literal[tuple(vLLMTaskType.values())]
