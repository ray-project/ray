import torch.nn as nn

from timm.data import Dataset, create_loader, resolve_data_config
from timm.models import create_model
from timm.optim import create_optimizer

from ray.util.sgd import TorchTrainer
from ray.util.sgd.torch import TrainingOperator

class Namespace:
    pass

# class TrainOp(TrainingOperator):
#     def setup(self, config):
#         pass

#     @override(TrainingOperator)
#     def train_batch(self, batch, batch_info):
#         pass

def model_creator(config):
    return create_model('resnet101')

def data_creator(config):
    dataset_train = Dataset('data/train')
    dataset_eval = Dataset('data/val')

    params = {
        "batch_size": 32,
        "input_size": (3, 224, 224),
        "use_prefetcher": False # config["use_gpu"]
    }

    train_loader = create_loader(dataset_train, **params)
    eval_loader = create_loader(dataset_eval, **params)

    return train_loader, eval_loader

def optimizer_creator(model, config):
    params = Namespace()
    params.opt = 'sgd'
    params.lr = .01
    params.opt_eps = 1e-8
    params.momentum = .9
    params.weight_decay = .0001

    return create_optimizer(params, model)

def loss_creator(config):
    return nn.CrossEntropyLoss()

def main():
    trainer = TorchTrainer(
        model_creator=model_creator,
        data_creator=data_creator,
        optimizer_creator=optimizer_creator,
        loss_creator=loss_creator,
        use_tqdm=True)

    a = trainer.train()
    print(a)

    trainer.shutdown()

if __name__ == '__main__':
    main()
