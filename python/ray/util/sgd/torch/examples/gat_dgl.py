# -*- coding: utf-8-*-
import os
import torch
import time
import argparse
from dgl.dataloading import NodeCollator
from torchvision import transforms
import ray
from ray.util.sgd import TorchTrainer
from ray.util.sgd.utils import AverageMeterCollection
from ray.util.sgd.torch import TrainingOperator
import dgl
import tqdm
import numpy as np
import torch as th
import torch.nn as nn
import torch.nn.functional as F
from dgl.data import RedditDataset
from dgl.nn.pytorch import GATConv
from torch.utils.data import DataLoader

print("Current Path: " + os.getcwd())
torch.manual_seed(42)

# define the model class
class GAT(nn.Module):
    def __init__(self, in_feats, n_hidden, n_classes, n_layers, n_heads, activation, feat_drop, attn_drop,
                 negative_slope, residual):
        super().__init__()

        self.n_layers = n_layers
        self.activation = activation
        self.n_hidden = n_hidden
        self.n_heads = n_heads
        self.n_classes = n_classes
        self.convs = nn.ModuleList()

        # input layer
        self.convs.append(
            GATConv((in_feats, in_feats), n_hidden, n_heads,
                    feat_drop, attn_drop, negative_slope, residual, self.activation))
        # hidden layer
        for l in range(1, n_layers - 1):
            # due to multi-head, the in_dim = num_hidden * num_heads
            self.convs.append(GATConv(
                (n_hidden * n_heads, n_hidden * n_heads), n_hidden, n_heads,
                feat_drop, attn_drop, negative_slope, residual, self.activation))
        ##output layer
        self.convs.append(GATConv(
            (n_hidden * n_heads, n_hidden * n_heads), n_classes, n_heads,
            feat_drop, attn_drop, negative_slope, residual, None))

    def forward(self, blocks, x):
        h = x
        for l, (layer, block) in enumerate(zip(self.convs, blocks)):
            h_dst = h[:block.number_of_dst_nodes()]
            if l != len(self.convs) - 1:
                h = layer(block, (h, h_dst)).flatten(1)
                h = F.dropout(h, p=0.5, training=self.training)
            else:
                h = layer(block, (h, h_dst))
        h = h.mean(1)
        return h.log_softmax(dim=-1)


def compute_acc(pred, labels):
    """
    Compute the accuracy of prediction given the labels.
    """
    return (th.argmax(pred, dim=1) == labels).float().sum() / len(pred)

class _NodeCollator(NodeCollator):
    def collate(self, items):
        # input_nodes, output_nodes, blocks
        result = super().collate(items)
        return result
    
class CustomTrainingOperator(TrainingOperator):
    def setup(self, config):
        # transforms for images
        transform = transforms.Compose([
            transforms.ToTensor(),
            transforms.Normalize((0.1307,), (0.3081,))
        ])

        # load reddit data
        data = RedditDataset()
        g = data[0]
        g.ndata['features'] = g.ndata['feat']
        g.ndata['labels'] = g.ndata['label']
        self.in_feats = g.ndata['features'].shape[1]
        self.n_classes = data.num_classes
        # add self loop,
        g = dgl.remove_self_loop(g)
        g = dgl.add_self_loop(g)
        # Create csr/coo/csc formats before launching training processes
        g.create_formats_()
        self.g = g
        train_nid = th.nonzero(g.ndata['train_mask'], as_tuple=True)[0]
        val_nid = th.nonzero(g.ndata['val_mask'], as_tuple=True)[0]
        test_nid = th.nonzero(g.ndata['test_mask'], as_tuple=True)[0]
        self.train_nid = train_nid
        self.val_nid = val_nid
        self.test_nid = test_nid

        # Create sampler
        sampler = dgl.dataloading.MultiLayerNeighborSampler(
            [int(fanout) for fanout in args.fan_out.split(',')])
        # Create PyTorch DataLoader for constructing blocks

        collator = _NodeCollator(g, train_nid, sampler)
        train_dataloader = DataLoader(collator.dataset,
                                collate_fn=collator.collate,
                                batch_size=args.batch_size,
                                shuffle=False,
                                drop_last=False,
                                num_workers=args.num_workers
                                )
        # Define model and optimizer, residual is set to True
        model = GAT(self.in_feats, args.n_hidden, self.n_classes, args.n_layers,
                    args.n_heads, F.elu, args.feat_drop, args.attn_drop, args.negative_slope, True)
        self.convs = model.convs
        # Define optimizer.
        optimizer = torch.optim.Adam(model.parameters(), lr=args.lr)
        # Register model, optimizer, and loss.
        self.model, self.optimizer = self.register(
            models=model,
            optimizers=optimizer)
        # Register data loaders.
        self.register_data(train_loader=train_dataloader)

    def train_epoch(self, iterator, info):
        meter_collection = AverageMeterCollection()
        tic = time.time()
        iter_tput = []
        model = self.model
        # for batch_idx,batch in enumerate(iterator):
        for step, (input_nodes, seeds, blocks) in enumerate(iterator):
            tic_step = time.time()
            # do some train
            optimizer = self.optimizer
            device = 0
            if self.use_gpu:
                blocks = [block.int().to(device) for block in blocks]
            batch_inputs = blocks[0].srcdata['features']
            batch_labels = blocks[-1].dstdata['labels']
            batch_pred = model(blocks, batch_inputs)
            loss = F.nll_loss(batch_pred, batch_labels)
            optimizer.zero_grad()
            loss.backward()
            optimizer.step()
            iter_tput.append(len(seeds) / (time.time() - tic_step))
            if step % 20 == 0:
                acc = compute_acc(batch_pred, batch_labels)
                gpu_mem_alloc = th.cuda.max_memory_allocated() / 1000000 if th.cuda.is_available() else 0
                print('Epoch {:05d} | Step {:05d} | Loss {:.4f} | Train Acc {:.4f} | Speed (samples/sec) {:.4f} | GPU '
                      '{:.1f} MB'.format(
                    info['epoch_idx'] + 1, step, loss.item(), acc.item(), np.mean(iter_tput[3:]), gpu_mem_alloc))
        status = meter_collection.summary()
        return status

    def validate(self, validation_loader, info):
        meter_collection = AverageMeterCollection()
        model = self.model
        g = self.g
        train_nid = self.train_nid
        val_nid = self.val_nid
        test_nid = self.test_nid
        device = 0
        model.eval()
        with th.no_grad():
            x = g.ndata['features']
            for l, layer in enumerate(self.convs):

                if l < args.n_layers - 1:
                    y = th.zeros(g.number_of_nodes(),
                                 args.n_hidden * args.n_heads if l != len(self.convs) - 1 else self.n_classes)
                else:
                    y = th.zeros(g.number_of_nodes(), args.n_hidden if l != len(self.convs) - 1 else self.n_classes)
                sampler = dgl.dataloading.MultiLayerFullNeighborSampler(1)
                collator = _NodeCollator(g, th.arange(g.number_of_nodes()), sampler)
                dataloader = DataLoader(collator.dataset,
                                        collate_fn=collator.collate,
                                        batch_size=args.batch_size,
                                        shuffle=False,
                                        drop_last=False,
                                        num_workers=args.num_workers
                                        )
                for input_nodes, output_nodes, blocks in dataloader:
                    block = blocks[0]
                    # print("block:",block)
                    block = block.int().to(device)
                    h = x[input_nodes].to(device)
                    h_dst = x[output_nodes].to(device)
                    if l != len(self.convs) - 1:
                        h = layer(block, (h, h_dst)).flatten(1)
                    else:
                        h = layer(block, (h, h_dst)).mean(1)
                        h = h.log_softmax(dim=-1)

                    y[output_nodes] = h.cpu()
                x = y
            pred = y
        labels = g.ndata['labels']
        train_acc, val_acc, test_acc = compute_acc(pred[train_nid], labels[train_nid]), \
                                       compute_acc(pred[val_nid], labels[val_nid]), \
                                       compute_acc(pred[test_nid], labels[test_nid])
        metrics = {"num_samples": pred.size(0), "val_acc": val_acc, "test_acc":test_acc}
        meter_collection.update(metrics, n=metrics.pop("num_samples", 1))
        status = meter_collection.summary()
        return status

def run(num_workers=1, use_gpu=False, num_epochs=2):
    trainer = TorchTrainer(
        training_operator_cls=CustomTrainingOperator,
        num_workers=num_workers,
        config={
            "lr": 1e-3,
            "batch_size": 1024,
        },
        use_gpu=use_gpu,
        backend="nccl",
        # use_tqdm=True,
    )

    for i in range(num_epochs):
        trainer.train()
    testResults = trainer.validate()
    trainer.shutdown()
    print(testResults)
    print("success!")
    
# Use ray.init(address="auto") if running on a Ray cluster.
if __name__ == '__main__':
    argparser = argparse.ArgumentParser("multi-gpu training")
    argparser.add_argument('--gpu', type=int, default=1,
                           help="GPU device ID. Use -1 for CPU training")
    argparser.add_argument('--num-epochs', type=int, default=2)
    argparser.add_argument('--n-hidden', type=int, default=128)
    argparser.add_argument('--n-layers', type=int, default=2)
    argparser.add_argument('--n-heads', type=int, default=4)
    argparser.add_argument('--fan-out', type=str, default='10,25')
    argparser.add_argument('--batch-size', type=int, default=1024)
    argparser.add_argument('--lr', type=float, default=0.001)
    argparser.add_argument('--feat-drop', type=float, default=0.)
    argparser.add_argument('--attn-drop', type=float, default=0.)
    argparser.add_argument('--negative-slope', type=float, default=0.2)
    argparser.add_argument('--num-workers', type=int, default=4,
                           help="Number of sampling processes. Use 0 for no extra process.")
    args = argparser.parse_args()
    ray.init(dashboard_host="10.3.68.117", dashboard_port=8888)
    ###connect to started ray cluster
    # ray.init(address='auto', _redis_password='5241590000000000')
    start_time = time.time()
    run(num_workers=3, use_gpu=True, num_epochs=args.num_epochs)
