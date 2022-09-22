# Key to denote the preprocessor in the checkpoint dict.
PREPROCESSOR_KEY = "_preprocessor"

# Key to denote the model in the checkpoint dict.
MODEL_KEY = "model"

# Key to denote which dataset is the evaluation dataset.
# Only used in trainers which do not support multiple
# evaluation datasets.
EVALUATION_DATASET_KEY = "evaluation"

# Key to denote which dataset is the training dataset.
# This is the dataset that the preprocessor is fit on.
TRAIN_DATASET_KEY = "train"

# Key to denote all user-specified auxiliary datasets in DatasetConfig.
WILDCARD_KEY = "*"

# Name to use for the column when representing tensors in table format.
TENSOR_COLUMN_NAME = "__value__"

# The maximum length of strings returned by `__repr__` for AIR objects constructed with
# default values.
MAX_REPR_LENGTH = int(80 * 1.5)

# Timeout used when putting exceptions raised by runner thread into the queue.
_ERROR_REPORT_TIMEOUT = 10

# Timeout when fetching new results after signaling the training function to continue.
_RESULT_FETCH_TIMEOUT = 0.2

# Timeout for fetching exceptions raised by the training function.
_ERROR_FETCH_TIMEOUT = 1
