from ray._common.deprecation import Deprecated


@Deprecated(
    help="The SpecDict API has been deprecated and cancelled without " "replacement.",
    error=True,
)
class SpecDict:
    pass
