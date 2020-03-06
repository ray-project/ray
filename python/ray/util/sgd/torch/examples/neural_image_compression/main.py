
# fixme: tf_trainer will run a gpu runner even if no
# gpus are available

# todo: no logging available
# todo: impossible to get dataset size from within the training operator

# todo: add a flag that does this
import logging
logging.getLogger("ray.util.sgd.torch.torch_runner").setLevel(logging.DEBUG)

def model_creator(config):
    from model import Net
    res = Net()

    # from torchsummary import summary
    # summary(res, input_size=(3, 500, 500))

    return res

def data_creator(config):
    from torch.utils.data import Dataset
    from torchvision.datasets import STL10
    from torchvision.transforms import Compose, RandomResizedCrop, ToTensor

    stl = STL10(
        '~/datasets',
        split='train+unlabeled', download=True,
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
    loss_creator)
# todo: use torch.utils.data.Subset or equivalent instead of num_steps? that would
# allow us to use its len() property
sys.train()
