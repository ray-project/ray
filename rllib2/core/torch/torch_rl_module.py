from dataclasses import dataclass

class TorchRLModule(nn.Module):

    def __init__(self):
        pass

    def forward(self):
        pass

    def forward_train(self, sample_dict) -> RLmoduleTrainOutput:
        pass



############ Examples



###################################################
################# PPO RLModule
###################################################

@dataclass
class PPORLOutput:
    act_dist =


class PPOTorchRLModule(TorchRLModule):

    def __init__(self, config):
        super(PPOTorchRLModule, self).__init__()

        if self.config:
            self.pi = ...
        else:
            self.pi = model_catalog.make_pi(config.pi)
        self.vf = model_catalog.make_vf(config.vf)

    def forward(self, batch: SampleBatch) -> :
        pass

    def load(self):
        pass

    def save(self):
        pass