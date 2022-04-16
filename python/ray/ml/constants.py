# Key to denote the preprocessor in the checkpoint dict.
PREPROCESSOR_KEY = "_preprocessor"

# Key to denote the model in the checkpoint dict.
MODEL_KEY = "model"

# Key to denote which dataset is the training dataset.
# This is the dataset that the preprocessor is fit on.
TRAIN_DATASET_KEY = "train"

# Key to denote which dataset is the evaluation dataset.
# Only used in trainers which do not support multiple
# evaluation datasets.
EVALUATION_DATASET_KEY = "evaluation"
