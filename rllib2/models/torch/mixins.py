import pytorch_lightning


class WithEncoderMixin(nn.Module):
    def __init__(self, encoder: Optional[Encoder] = None) -> None:
        super().__init__()
        self.encoder = encoder

    def encode(self, batch: SampleBatch):
        return self.encoder(batch)

    def __call__(self, batch, encode: bool = True, **kwargs):
        # TODO: This is probably not a good design pattern
        # The following usage is more intuitive than creating a different EncoderOutPut class
        #   state = self.encode({'obs': batch.obs})
        #   next_state = self.encode({'obs': batch.next_obs})
        encoded_batch = kwargs.pop("encoded_batch")
        if self.encoder and encode:
            encoded_batch = self.encoder(batch, **kwargs)
        return super().__call__(batch, encoded_batch=encoded_batch)


class ModelIO:
    def save(self):
        pass

    def load(self):
        pass
