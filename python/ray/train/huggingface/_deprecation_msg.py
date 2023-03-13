deprecation_msg = (
    "`ray.train.huggingface` has been split into "
    "`ray.train.huggingface.transformers` and `ray.train.huggingface.accelerate`,"
    " with `HuggingFaceTrainer`, `HuggingFacePredictor` and `HuggingFaceCheckpoint` "
    "renamed to `TransformersTrainer`, `TransformersPredictor` and "
    "`TransformersCheckpoint` respectively. Update your code to use the new import "
    "paths. This will raise an exception in the future."
)
