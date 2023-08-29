from ray.train._internal.storage import _use_storage_context

# Import this first so it can be used in other modules
if _use_storage_context():
    from ray.tune.analysis.experiment_analysis import (
        NewExperimentAnalysis as ExperimentAnalysis,
    )
else:
    from ray.tune.analysis.experiment_analysis import ExperimentAnalysis

__all__ = ["ExperimentAnalysis"]
