from ray.util.annotations import DeveloperAPI


@DeveloperAPI
class RaggedTensorNotSupportedError(ValueError):
    pass


@DeveloperAPI
class TensorArrayCastingError(ValueError):
    pass
