__all__: list[str] = []

# Classes
class ExportClassName:
    # Classes
    class Params:
        int_value: int
        float_value: float

        # Functions
        def __init__(self, int_param: int = ..., float_param: float = ...) -> None: ...



    # Functions
    def getIntParam(self) -> int: ...

    def getFloatParam(self) -> float: ...

    @staticmethod
    def originalName() -> str: ...

    @classmethod
    def create(cls, params: ExportClassName.Params = ...) -> ExportClassName: ...



# Functions
def testEchoBooleanFunction(flag: bool) -> bool: ...


