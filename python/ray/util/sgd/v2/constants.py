from ray.util.sgd.v2.backends.torch import TorchConfig

BACKEND_NAME_TO_CONFIG_CLS = {"torch": TorchConfig}

TIME_THIS_ITER_S = "_time_this_iter_s"

IS_FINAL_OUTPUT = "_sgd_is_final_output"

# Time between BackendExecutor.fetch_next checks when fetching
# new results after signaling the training function to continue.
RESULT_FETCH_TIMEOUT = 0.2
