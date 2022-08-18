import torch.nn as nn

from ..configs import ModelConfig

class ModelIO:
    def save(self):
        pass

    def load(self):
        pass


class ModelWithEncoder(nn.Module, ModelIO):

    def __init__(self, config: ModelConfig) -> None:
        super(nn.Module).__init__()
        super(ModelIO).__init__()
        # save config
        self.config = config
        # encoder
        self.encoder, self.encoder_out_dim = self._make_encoder()
    
    def _make_encoder(self) -> Tuple[nn.Module, int]:
        if isinstance(self.config.encoder, str):
            # interpret this as a registered model name
            encoder = None
        elif self.config.encoder:
            # if not empty and not a string, assume it is the model
            encoder = self.config.encoder
        else:
            # return a default encoder if none 
            encoder = model_catalog.get_encoder(self.config)
        output = torch_dryrun(encoder, self.config.observation_space)
        return encoder, output.shape[-1]

        