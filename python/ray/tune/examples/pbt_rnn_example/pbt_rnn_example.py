#!/usr/bin/env python
# coding: utf-8

# ![](https://i.imgur.com/eBRPvWB.png)
#
# # Practical PyTorch: Classifying Names with a Character-Level RNN
#
# We will be building and training a basic character-level RNN to classify words. A character-level RNN reads words as a series of characters - outputting a prediction and "hidden state" at each step, feeding its previous hidden state into each next step. We take the final prediction to be the output, i.e. which class the word belongs to.
#
# Specifically, we'll train on a few thousand surnames from 18 languages of origin, and predict which language a name is from based on the spelling:

import glob
import torch
import unicodedata
import string
import torch.nn as nn
from torch.autograd import Variable
import time
import math
import random
from ray.tune import trial
import ray
from ray import tune
from ray.tune import Trainable, run
from ray.tune.schedulers import PopulationBasedTraining
from ray.tune import track
import numpy as np

all_filenames = glob.glob('names/*.txt')


all_letters = string.ascii_letters + " .,;'"
n_letters = len(all_letters)

# Turn a Unicode string to plain ASCII, thanks to http://stackoverflow.com/a/518232/2809427
def unicode_to_ascii(s):
    return ''.join(
        c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn'
        and c in all_letters
    )



# Build the category_lines dictionary, a list of names per language
category_lines = {}
all_categories = []

# Read a file and split into lines
def readLines(filename):
    lines = open(filename).read().strip().split('\n')
    return [unicode_to_ascii(line) for line in lines]

for filename in all_filenames:
    category = filename.split('/')[-1].split('.')[0]
    all_categories.append(category)
    lines = readLines(filename)
    category_lines[category] = lines

n_categories = len(all_categories)



# # Turning Names into Tensors
#
# Now that we have all the names organized, we need to turn them into Tensors to make any use of them.
#
# To represent a single letter, we use a "one-hot vector" of size `<1 x n_letters>`. A one-hot vector is filled with 0s except for a 1 at index of the current letter, e.g. `"b" = <0 1 0 0 0 ...>`.
#
# To make a word we join a bunch of those into a 2D matrix `<line_length x 1 x n_letters>`.
#
# That extra 1 dimension is because PyTorch assumes everything is in batches - we're just using a batch size of 1 here.

# Just for demonstration, turn a letter into a <1 x n_letters> Tensor
def letter_to_tensor(letter):
    tensor = torch.zeros(1, n_letters)
    letter_index = all_letters.find(letter)
    tensor[0][letter_index] = 1
    return tensor

# Turn a line into a <line_length x 1 x n_letters>,
# or an array of one-hot letter vectors
def line_to_tensor(line):
    tensor = torch.zeros(len(line), 1, n_letters)
    for li, letter in enumerate(line):
        letter_index = all_letters.find(letter)
        tensor[li][0][letter_index] = 1
    return tensor



# # Creating the Network
#
# Before autograd, creating a recurrent neural network in Torch involved cloning the parameters of a layer over several timesteps. The layers held hidden state and gradients which are now entirely handled by the graph itself. This means you can implement a RNN in a very "pure" way, as regular feed-forward layers.
#
# This RNN module (mostly copied from [the PyTorch for Torch users tutorial](https://github.com/pytorch/tutorials/blob/master/Introduction%20to%20PyTorch%20for%20former%20Torchies.ipynb)) is just 2 linear layers which operate on an input and hidden state, with a LogSoftmax layer after the output.
#
# ![](https://i.imgur.com/Z2xbySO.png)



class RNN(nn.Module):
    def __init__(self, input_size, hidden_size, output_size):
        super(RNN, self).__init__()

        self.input_size = input_size
        self.hidden_size = hidden_size
        self.output_size = output_size

        self.i2h = nn.Linear(input_size + hidden_size, hidden_size)
        self.i2o = nn.Linear(input_size + hidden_size, output_size)
        self.softmax = nn.LogSoftmax()

    def forward(self, input, hidden):
        combined = torch.cat((input, hidden), 1)
        hidden = self.i2h(combined)
        output = self.i2o(combined)
        output = self.softmax(output)
        return output, hidden

    def init_hidden(self):
        return Variable(torch.zeros(1, self.hidden_size))

# # Preparing for Training


def category_from_output(output):
    top_n, top_i = output.data.topk(1) # Tensor out of Variable with .data
    category_i = top_i[0][0]
    return all_categories[category_i], category_i


# We will also want a quick way to get a training example (a name and its language):

def random_training_pair():
    category = random.choice(all_categories)
    line = random.choice(category_lines[category])
    category_tensor = Variable(torch.LongTensor([all_categories.index(category)]))
    line_tensor = Variable(line_to_tensor(line))
    return category, line, category_tensor, line_tensor

# # Training the Network
#

n_iters = 100000
print_every = 5000
plot_every = 100

def evaluate(line_tensor, model):
    hidden = model.init_hidden()

    for i in range(line_tensor.size()[0]):
        output, hidden = model(line_tensor[i], hidden)

    return output

def test(model):
    confusion = torch.zeros(n_categories, n_categories)
    n_confusion = 100

    num_correct = 0
    total = 0


    # Go through a bunch of examples and record which are correctly guessed
    for i in range(n_confusion):
        category, line, category_tensor, line_tensor = random_training_pair()
        output = evaluate(line_tensor, model)
        guess, guess_i = category_from_output(output)
        category_i = all_categories.index(category)
        confusion[category_i][guess_i] += 1
        if category_i == guess_i:
            num_correct += 1
        total += 1
    return num_correct/total

criterion = nn.NLLLoss()

def train(model, category_tensor, line_tensor, optimizer, counter):
    model.zero_grad()
    hidden = model.init_hidden()

    for i in range(line_tensor.size()[0]):
        output, hidden = model(line_tensor[i], hidden)

    loss = criterion(output, category_tensor)
    loss.backward()

    optimizer.step()
    if counter % print_every == 0:
        print('%s' % (loss.item()))
    return output, loss.item()



class PBTExperiment(Trainable):
    def _setup(self, config):
        self.lr = config["lr"]
        self.n_hidden = config["n_hidden"]
        self.loss = 0
        self.accuracy = 0
        self.epoch = 0
        self.plot_every = 1000
        track.init()
        self.model = RNN(n_letters, self.n_hidden, n_categories)
        self.optimizer = torch.optim.Adam(
            self.model.parameters(), lr=config["lr"])

    def _train(self):
        for i in range(100):
            category, line, category_tensor, line_tensor = random_training_pair()
            output, loss = train(self.model,category_tensor, line_tensor, self.optimizer, i)

            self.loss += loss
            acc = test(self.model)
            self.accuracy += acc
        self.epoch += 1
        return {"mean_accuracy": self.accuracy/(100*self.epoch),
            "loss": self.loss/(100*self.epoch)}



    def _save(self, checkpoint_dir):
        return {
            "accuracy": self.accuracy,
            "lr": self.lr,
            "n_hidden":self.n_hidden,
            "loss": self.loss
        }

    def _restore(self, checkpoint):
        self.accuracy = checkpoint["accuracy"]

    def reset_config(self, new_config):
        self.lr = new_config["lr"]
        self.n_hidden = new_config["n_hidden"]
        return True

pbt = PopulationBasedTraining(
    time_attr="training_iteration",
    metric="mean_accuracy",
    mode="max",
    perturbation_interval=20,
    hyperparam_mutations={
        # distribution for resampling
            "lr": lambda: 0.0001 * random.randint(1,50),
            "n_hidden" : lambda: random.randint(100,300)
    })
analysis = run(
    PBTExperiment,
    name="pbt_test",
    scheduler=pbt,
    reuse_actors=True,
    verbose=1,
    **{
        "stop": {
            "training_iteration": 50,
        },
        "num_samples": 4,
        "config": {
            "lr": tune.sample_from(lambda spec: 0.0001 * random.randint(1,50)),
            "n_hidden" : tune.sample_from(lambda spec: random.randint(100,300))
        }
    })


print("Best config is", analysis.get_best_config(metric="mean_accuracy"))
all_dataframes = analysis.trial_dataframes
df = all_dataframes[analysis.get_best_logdir(metric="mean_accuracy")]
# trial = analysis.trials[3]

# analysis.trial_dataframes[trial.logdir].mean_accuracy.plot()

#df.mean_accuracy.plot()
#df["loss"].plot()

