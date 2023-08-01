from ray.util.annotations import PublicAPI


@PublicAPI(stability="stable")
class RayServeException(Exception):
    pass


@PublicAPI(stability="alpha")
class RayServeTimeout(Exception):
    def __init__(self, is_first_message: bool):
        super().__init__()
        self.is_first_message = is_first_message
