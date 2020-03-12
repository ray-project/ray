# fixme: tf_trainer will run a gpu runner even if no
# gpus are available

# todo: add a flag that does this
import logging
logging.getLogger("ray.util.sgd.torch.torch_runner").setLevel(logging.DEBUG)

import sys
log = logging.getLogger("rqtorch")
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
log.addHandler(handler)
log.setLevel(logging.DEBUG)

from ray.util.sgd.torch import TrainingOperator

def model_creator(config):
    from model import Net
    res = Net()

    # from torchsummary import summary
    # summary(res, input_size=(3, 500, 500))

    return res

def data_creator(config):
    from torch.utils.data import Dataset
    from torchvision.datasets import CIFAR10
    from torchvision.transforms import Compose, RandomResizedCrop, ToTensor

    stl = CIFAR10(
        '~/datasets',
        train=True,
        download=True,
        transform=Compose([
            ToTensor()
        ]))

    class Data(Dataset):
        def __getitem__(self, i):
            x, y = stl[i]
            return x, x

        def __len__(self):
            return len(stl)

    return Data()

def optimizer_creator(model, config):
    if config.optim == "ranger":
        from radam import RAdam
        from lookahead import Lookahead

        return Lookahead(RAdam(model.parameters()))

    if config.optim == "radam":
        from radam import RAdam
        return RAdam(model.parameters())

    if config.optim == "lookahead":
        from torch.optim import Adam
        from lookahead import Lookahead

        return Lookahead(Adam(model.parameters(), amsgrad=True))

    if config.optim == "adam":
        from torch.optim import Adam
        return Adam(model.parameters(), amsgrad=True)

def loss_creator(config):
    from torch.nn import MSELoss
    return MSELoss()

class NICTrainingOperator(TrainingOperator):
    def forward(self, features, target):
        self.output = self.model(features)
        loss = self.criterion(self.output, target)
        return loss

    def batch_interval_log(self, interval):
        # caption="Batch {}".format(batch_idx)
        self.logger.log_image("Output examples", self.output)

from ray_sgd_additions import System

sys = System()

sys.add_custom_train_argument(
    "--optim",
    type=str,
    default="ranger",
    help="Optimizer algorithm to use.")

args = sys.parse_args()

# self.config.args = args # if necessary
sys.config.optim = args.optim

sys.init_ray()
sys.create_trainer(
    model_creator,
    data_creator,
    optimizer_creator,
    loss_creator,
    training_operator_cls=NICTrainingOperator)
# todo: use torch.utils.data.Subset or equivalent instead of num_steps? that would
# allow us to use its len() property
sys.train()
