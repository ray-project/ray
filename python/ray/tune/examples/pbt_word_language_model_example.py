#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Train word-level language modeling RNN on Wikitext-2 using Population Based
Training. Perturbs learning rate and drop rate.

Dataset:
https://www.salesforce.com/products/einstein/ai-research/the-wikitext-dependency-language-modeling-dataset/.

Adapted from https://github.com/pytorch/examples/tree/master/word_language_model.
"""
import argparse
from io import open
import math
import os
import time

import numpy as np

import torch
import torch.nn as nn
import torch.nn.functional as F

import ray
from ray.tune import Trainable, run
from ray.tune.schedulers import PopulationBasedTraining

############################### DATA ###############################
class Dictionary(object):
    def __init__(self):
        self.word2idx = {}
        self.idx2word = []

    def add_word(self, word):
        if word not in self.word2idx:
            self.idx2word.append(word)
            self.word2idx[word] = len(self.idx2word) - 1
        return self.word2idx[word]

    def __len__(self):
        return len(self.idx2word)


class Corpus(object):
    def __init__(self, path):
        self.dictionary = Dictionary()
        self.train = self.tokenize(os.path.join(path, 'train.txt'))
        self.valid = self.tokenize(os.path.join(path, 'valid.txt'))
        self.test = self.tokenize(os.path.join(path, 'test.txt'))

    def tokenize(self, path):
        """Tokenizes a text file."""
        assert os.path.exists(path), (
            'Could not find dataset at {}.'.format(path)
        )
        # Add words to the dictionary
        with open(path, 'r', encoding="utf8") as f:
            for line in f:
                words = line.split() + ['<eos>']
                for word in words:
                    self.dictionary.add_word(word)

        # Tokenize file content
        with open(path, 'r', encoding="utf8") as f:
            idss = []
            for line in f:
                words = line.split() + ['<eos>']
                ids = []
                for word in words:
                    ids.append(self.dictionary.word2idx[word])
                idss.append(torch.tensor(ids).type(torch.int64))
            ids = torch.cat(idss)

        return ids


############################### MODEL ###############################
class RNNModel(nn.Module):
    """Container module with an encoder, a recurrent module, and a decoder."""

    def __init__(self,
                 rnn_type,
                 ntoken,
                 ninp,
                 nhid,
                 nlayers,
                 dropout=0.5,
                 tie_weights=False):
        super(RNNModel, self).__init__()
        self.drop = nn.Dropout(dropout)
        self.encoder = nn.Embedding(ntoken, ninp)
        if rnn_type in ['LSTM', 'GRU']:
            self.rnn = getattr(nn, rnn_type)(ninp,
                                             nhid,
                                             nlayers,
                                             dropout=dropout)
        else:
            try:
                nonlinearity = (
                    {'RNN_TANH': 'tanh', 'RNN_RELU': 'relu'}[rnn_type]
                )
            except KeyError:
                raise ValueError(
                    "An invalid option for `--model` was supplied,"
                    " options are ['LSTM', 'GRU', 'RNN_TANH' or 'RNN_RELU']"
                )
            self.rnn = nn.RNN(ninp,
                              nhid,
                              nlayers,
                              nonlinearity=nonlinearity,
                              dropout=dropout)
        self.decoder = nn.Linear(nhid, ntoken)

        # Optionally tie weights as in:
        # "Using the Output Embedding to Improve Language Models" (Press & Wolf 2016)
        # https://arxiv.org/abs/1608.05859
        # and
        # "Tying Word Vectors and Word Classifiers: A Loss Framework for Language Modeling" (Inan et al. 2016)
        # https://arxiv.org/abs/1611.01462
        if tie_weights:
            if nhid != ninp:
                raise ValueError(
                    'When using the tied flag, nhid must be equal to emsize'
                )
            self.decoder.weight = self.encoder.weight

        self.init_weights()

        self.rnn_type = rnn_type
        self.nhid = nhid
        self.nlayers = nlayers

    def init_weights(self):
        initrange = 0.1
        self.encoder.weight.data.uniform_(-initrange, initrange)
        self.decoder.bias.data.zero_()
        self.decoder.weight.data.uniform_(-initrange, initrange)

    def forward(self, input, hidden):
        emb = self.drop(self.encoder(input))
        output, hidden = self.rnn(emb, hidden)
        output = self.drop(output)
        decoded = self.decoder(output)
        return decoded, hidden

    def init_hidden(self, bsz):
        weight = next(self.parameters())
        if self.rnn_type == 'LSTM':
            return (weight.new_zeros(self.nlayers, bsz, self.nhid),
                    weight.new_zeros(self.nlayers, bsz, self.nhid))
        else:
            return weight.new_zeros(self.nlayers, bsz, self.nhid)


class PositionalEncoding(nn.Module):
    r"""Inject some information about the relative or absolute position of the
        tokens in the sequence. The positional encodings have the same
        dimension as the embeddings, so that the two can be summed. Here, we
        use sine and cosine functions of different frequencies.
    .. math::
        \text{PosEncoder}(pos, 2i) = sin(pos/10000^(2i/d_model))
        \text{PosEncoder}(pos, 2i+1) = cos(pos/10000^(2i/d_model))
        \text{where pos is the word position and i is the embed idx)
    Args:
        d_model: the embed dim (required).
        dropout: the dropout value (default=0.1).
        max_len: the max. length of the incoming sequence (default=5000).
    Examples:
        >>> pos_encoder = PositionalEncoding(d_model)
    """

    def __init__(self, d_model, dropout=0.1, max_len=5000):
        super(PositionalEncoding, self).__init__()
        self.dropout = nn.Dropout(p=dropout)

        pe = torch.zeros(max_len, d_model)
        position = torch.arange(0, max_len, dtype=torch.float).unsqueeze(1)
        div_term = torch.exp(
                        torch.arange(0, d_model, 2).float() *
                            (-math.log(10000.0) / d_model)
                   )
        pe[:, 0::2] = torch.sin(position * div_term)
        pe[:, 1::2] = torch.cos(position * div_term)
        pe = pe.unsqueeze(0).transpose(0, 1)
        self.register_buffer('pe', pe)

    def forward(self, x):
        r"""Inputs of forward function
        Args:
            x: the sequence fed to the positional encoder model (required).
        Shape:
            x: [sequence length, batch size, embed dim]
            output: [sequence length, batch size, embed dim]
        Examples:
            >>> output = pos_encoder(x)
        """

        x = x + self.pe[:x.size(0), :]
        return self.dropout(x)


class TransformerModel(nn.Module):
    """Container module with an encoder, a recurrent or transformer module, and
    a decoder.
    """

    def __init__(self, ntoken, ninp, nhead, nhid, nlayers, dropout=0.5):
        super(TransformerModel, self).__init__()
        try:
            from torch.nn import TransformerEncoder, TransformerEncoderLayer
        except:
            raise ImportError(
                'TransformerEncoder module does not'
                ' exist in PyTorch 1.1 or lower.'
            )
        self.model_type = 'Transformer'
        self.src_mask = None
        self.pos_encoder = PositionalEncoding(ninp, dropout)
        encoder_layers = TransformerEncoderLayer(ninp, nhead, nhid, dropout)
        self.transformer_encoder = TransformerEncoder(encoder_layers, nlayers)
        self.encoder = nn.Embedding(ntoken, ninp)
        self.ninp = ninp
        self.decoder = nn.Linear(ninp, ntoken)

        self.init_weights()

    def _generate_square_subsequent_mask(self, sz):
        mask = (torch.triu(torch.ones(sz, sz)) == 1).transpose(0, 1)
        mask = mask.float().masked_fill(
                    mask == 0,
                    float('-inf')
               ).masked_fill(mask == 1, float(0.0))
        return mask

    def init_weights(self):
        initrange = 0.1
        self.encoder.weight.data.uniform_(-initrange, initrange)
        self.decoder.bias.data.zero_()
        self.decoder.weight.data.uniform_(-initrange, initrange)

    def forward(self, src, has_mask=True):
        if has_mask:
            device = src.device
            if self.src_mask is None or self.src_mask.size(0) != len(src):
                mask = (
                    self._generate_square_subsequent_mask(len(src)).to(device)
                )
                self.src_mask = mask
        else:
            self.src_mask = None

        src = self.encoder(src) * math.sqrt(self.ninp)
        src = self.pos_encoder(src)
        output = self.transformer_encoder(src, self.src_mask)
        output = self.decoder(output)
        return F.log_softmax(output, dim=-1)


############################### TRAINABLE ###############################
class LanguageModel(Trainable):
    def _setup(self, config):
        self.args = config["args"]
        self.corpus = config["corpus"]
        self.lr = config["lr"]
        self.dropout = config["dropout"]

        # Bound dropout within a reasonable range.
        self.dropout = max(min(self.dropout, 0.4), 0)

        # Set the random seed manually for reproducibility.
        torch.manual_seed(self.args.seed)
        if torch.cuda.is_available():
            if not self.args.cuda:
                print(
                    "WARNING: You have a CUDA device, so you"
                    " should probably run with --cuda"
                )

        device = torch.device("cuda" if self.args.cuda else "cpu")

        #####################################################################
        # Load data
        #####################################################################

        # Starting from sequential data, batchify arranges the dataset into
        # columns. For instance, with the alphabet as the sequence and batch
        # size 4, we'd get
        # ┌ a g m s ┐
        # │ b h n t │
        # │ c i o u │
        # │ d j p v │
        # │ e k q w │
        # └ f l r x ┘.
        # These columns are treated as independent by the model, which means
        # that the dependence of e. g. 'g' on 'f' can not be learned, but
        # allows more efficient batch processing.

        def batchify(data, bsz):
            # Work out how cleanly we can divide the dataset into bsz parts.
            nbatch = data.size(0) // bsz
            # Trim off any extra elements that wouldn't cleanly fit
            # (remainders).
            data = data.narrow(0, 0, nbatch * bsz)
            # Evenly divide the data across the bsz batches.
            data = data.view(bsz, -1).t().contiguous()
            return data.to(device)

        self.eval_batch_size = 10
        self.train_data = batchify(self.corpus.train, self.args.batch_size)
        self.val_data = batchify(self.corpus.valid, self.eval_batch_size)

        #####################################################################
        # Build the model
        #####################################################################

        ntokens = len(self.corpus.dictionary)
        if self.args.model == 'Transformer':
            self.model = TransformerModel(ntokens,
                                          self.args.emsize,
                                          self.args.nhead,
                                          self.args.nhid,
                                          self.args.nlayers,
                                          self.dropout).to(device)
        else:
            self.model = RNNModel(args.model,
                                  ntokens,
                                  self.args.emsize,
                                  self.args.nhid,
                                  self.args.nlayers,
                                  self.dropout,
                                  self.args.tied).to(device)

        self.criterion = nn.CrossEntropyLoss()

    #########################################################################
    # Training code
    #########################################################################

    def _repackage_hidden(self, h):
        """Wraps hidden states in new Tensors, to detach them from their
        history.
        """
        if isinstance(h, torch.Tensor):
            return h.detach()
        else:
            return tuple(self._repackage_hidden(v) for v in h)


    # get_batch subdivides the source data into chunks of length args.bptt.
    # If source is equal to the example output of the batchify function, with
    # a bptt-limit of 2, we'd get the following two Variables for i = 0:
    # ┌ a g m s ┐ ┌ b h n t ┐
    # └ b h n t ┘ └ c i o u ┘
    # Note that despite the name of the function, the subdivison of data is not
    # done along the batch dimension (i.e. dimension 1), since that was handled
    # by the batchify function. The chunks are along dimension 0, corresponding
    # to the seq_len dimension in the LSTM.

    def _get_batch(self, source, i):
        seq_len = min(self.args.bptt, len(source) - 1 - i)
        data = source[i:i+seq_len]
        target = source[i+1:i+1+seq_len].view(-1)
        return data, target


    def _evaluate(self, data_source):
        # Turn on evaluation mode which disables dropout.
        self.model.eval()
        total_loss = 0.
        ntokens = len(self.corpus.dictionary)
        if self.args.model != 'Transformer':
            hidden = self.model.init_hidden(self.eval_batch_size)
        with torch.no_grad():
            for i in range(0, data_source.size(0) - 1, self.args.bptt):
                data, targets = self._get_batch(data_source, i)
                if self.args.model == 'Transformer':
                    output = self.model(data)
                else:
                    output, hidden = self.model(data, hidden)
                    hidden = self._repackage_hidden(hidden)
                output_flat = output.view(-1, ntokens)
                total_loss += (
                    len(data) * self.criterion(output_flat, targets).item()
                )
        return total_loss / (len(data_source) - 1)


    def _train(self):
        # Turn on training mode which enables dropout.
        self.model.train()
        total_loss = 0.
        start_time = time.time()
        ntokens = len(self.corpus.dictionary)
        if self.args.model != 'Transformer':
            hidden = self.model.init_hidden(self.args.batch_size)
        for batch, i in enumerate(
            range(0, self.train_data.size(0) - 1, self.args.bptt)
        ):
            data, targets = self._get_batch(self.train_data, i)
            # Starting each batch, we detach the hidden state from how it was
            # previously produced. If we didn't, the model would try
            # backpropagating all the way to start of the dataset.
            self.model.zero_grad()
            if self.args.model == 'Transformer':
                output = self.model(data)
            else:
                hidden = self._repackage_hidden(hidden)
                output, hidden = self.model(data, hidden)
            loss = self.criterion(output.view(-1, ntokens), targets)
            loss.backward()

            # `clip_grad_norm` helps prevent the exploding gradient problem in
            # RNNs / LSTMs.
            torch.nn.utils.clip_grad_norm_(self.model.parameters(),
                                           self.args.clip)
            for p in self.model.parameters():
                p.data.add_(-self.lr, p.grad.data)

            total_loss += loss.item()


            if batch % args.log_interval == 0 and batch > 0:
                cur_loss = total_loss / self.args.log_interval
                elapsed = time.time() - start_time
                print(
                    '| {:5d}/{:5d} batches | lr {:02.2f} | ms/batch {:5.2f} | '
                    'loss {:5.2f} | ppl {:8.2f}'.format(
                        batch,
                        len(self.train_data) // self.args.bptt,
                        self.lr,
                        elapsed * 1000 / self.args.log_interval,
                        cur_loss,
                        math.exp(cur_loss)
                    )
                )
                total_loss = 0
                start_time = time.time()

        val_loss = self._evaluate(self.val_data)

        return {'val_loss': val_loss}

    def _save(self, checkpoint_dir):
        checkpoint_path = os.path.join(checkpoint_dir, "model.pth")
        torch.save(self.model.state_dict(), checkpoint_path)
        return checkpoint_path

    def _restore(self, checkpoint_path):
        self.model.load_state_dict(torch.load(checkpoint_path))

if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='PyTorch Wikitext-2 RNN/LSTM Language Model'
    )
    parser.add_argument('--data', type=str, default='./data/wikitext-2',
                        help='location of the data corpus')
    parser.add_argument(
        '--model',
        type=str,
        default='LSTM',
        help=(
            'type of recurrent net'
            ' (RNN_TANH, RNN_RELU, LSTM, GRU, Transformer)'
        )
    )
    parser.add_argument('--emsize', type=int, default=200,
                        help='size of word embeddings')
    parser.add_argument('--nhid', type=int, default=200,
                        help='number of hidden units per layer')
    parser.add_argument('--nlayers', type=int, default=2,
                        help='number of layers')
    parser.add_argument('--clip', type=float, default=0.25,
                        help='gradient clipping')
    parser.add_argument('--epochs', type=int, default=40,
                        help='upper epoch limit')
    parser.add_argument('--batch_size', type=int, default=20, metavar='N',
                        help='batch size')
    parser.add_argument('--bptt', type=int, default=35,
                        help='sequence length')
    parser.add_argument('--tied', action='store_true',
                        help='tie the word embedding and softmax weights')
    parser.add_argument('--seed', type=int, default=1111,
                        help='random seed')
    parser.add_argument('--cuda', action='store_true',
                        help='use CUDA')
    parser.add_argument('--log-interval', type=int, default=200, metavar='N',
                        help='report interval')
    parser.add_argument(
        '--nhead',
        type=int,
        default=2,
        help=(
            'the number of heads in the encoder/decoder'
            ' of the transformer model'
        )
    )

    args = parser.parse_args()

    # With a large population, we might need a large object store.
    OBJ_STORE_MEM_GB = 10
    ray.init(object_store_memory=OBJ_STORE_MEM_GB*10**9)

    corpus = Corpus(args.data)

    pbt = PopulationBasedTraining(
        time_attr="training_iteration",
        metric="val_loss",
        mode="min",
        perturbation_interval=1,
        hyperparam_mutations={
            "lr": lambda: np.random.uniform(15, 25),
            "dropout": lambda : np.random.uniform(0, 0.4)
        })

    run(
        LanguageModel,
        name="word_language_model_exp",
        scheduler=pbt,
        **{
            "resources_per_trial": {
                "cpu": 1,
                "gpu": 0.1 if args.cuda else 0,
            },
            "stop": {
                "training_iteration": 50,
            },
            "num_samples": 12,
            "config": {
                "args": args,
                "corpus": corpus,
                "lr": 20,
                "dropout": 0.2
            },
        })
