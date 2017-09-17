from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import collections
import csv
import gym
import gym.spaces
import numpy as np
import random
import sys
from spacy.en import English

from .rouge import Rouge
from .similarity import create_similarity_pipeline

from ray.rllib.models.preprocessors import Preprocessor

Datapoint = collections.namedtuple(
    'Datapoint', ['text', 'summary'])

class WordSequencePair(gym.Space):
    def __init__(self, past_context_size, future_context_size):
        self.shape = (4 * 300,)
        self.past_context_size = past_context_size
        self.future_context_size = future_context_size

class SimilaritySummarizationEnv(gym.Env):

    def __init__(self, filepath):
        self.action_space = gym.spaces.Discrete(2)
        # Word2Vec of the last two words in the text and the last two
        # words in the summary
        self.observation_space = WordSequencePair(4, 4)
        self.scorer = Rouge()
        self.data = []
        print("Processing dataset, this can take a few minutes...")
        self.nlp = English()
        csv.field_size_limit(sys.maxsize)
        with open(filepath) as f:
            reader = csv.reader(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                self.data.append(Datapoint(text=list(self.nlp(row[0]).sents),
                                           summary=list(self.nlp(row[1]).sents)))
        self.reset()

    def reset(self):
        self.current_document = random.randint(1, len(self.data)-1)
        self.current_token = 0
        self.prediction_so_far = []
        self.last_score = 0.0
        self.done = False
        return (self.observation_space.past_context_size * [self.nlp("")],
                self.observation_space.future_context_size * [self.nlp("")])

    def step(self, action):
        text = self.data[self.current_document].text
        summary = self.data[self.current_document].summary
        if action == 1:
            self.prediction_so_far.append(text[self.current_token])
        if self.done:
            return (self.observation_space.past_context_size * [self.nlp("")],
                    self.observation_space.future_context_size * [self.nlp("")]), 0.0, self.done, {}
        self.current_token += 1
        score = self.scorer.calculate_score(summary, self.prediction_so_far)
        reward = score - self.last_score
        self.last_score = score
        self.done = self.current_token == len(summary) or self.current_token >= len(text) - 1
        p = self.observation_space.past_context_size
        f = self.observation_space.future_context_size
        past = (p * [self.nlp("")] + text[:self.current_token])[-p:]
        future = (text[self.current_token:] + f * [self.nlp("")])[0:f]
        return (past, future), reward, self.done, {}

class SimilarityWord2VecPreprocessor(Preprocessor):

    nlp = spacy.load('en', create_pipeline=create_similarity_pipeline)

    def transform_shape(self, obs_shape):
        return (4*3,)

    def transform(self, observation):
        past, future = observation
        past = nlp(past.string)
        future = nlp(future.string)
        return np.concatenate([future[0].similarity(p) for p in past])
