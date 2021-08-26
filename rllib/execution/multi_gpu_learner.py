from ray.rllib.execution.multi_gpu_learner_thread import \
    MultiGPULearnerThread, _MultiGPULoaderThread
from ray.rllib.utils.deprecation import deprecation_warning

deprecation_warning("multi_gpu_learner.py", "multi_gpu_learner_thread.py")
TFMultiGPULearner = MultiGPULearnerThread
_LoaderThread = _MultiGPULoaderThread
