from pytorch_lightning.loggers.logger import Logger

class AIRLightningLogger(Logger):
    def __init__(self) -> None:
        super().__init__()
        