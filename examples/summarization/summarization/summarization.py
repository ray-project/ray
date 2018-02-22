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
import textacy


from .rouge import Rouge

from ray.rllib.models.preprocessors import Preprocessor

Datapoint = collections.namedtuple(
    'Datapoint', ['text_sentences', 'summary_sentences',
                  'text_vectors', 'summary_vectors'])

class WordSequencePair(gym.Space):
    def __init__(self, past_context_size, future_context_size):
        self.shape = (4 * 300,)
        self.past_context_size = past_context_size
        self.future_context_size = future_context_size

def compute_tf_idf(filepath):
    print("Computing TF-IDF matrix...")

    vectorizer = textacy.Vectorizer(
         weighting='tfidf', normalize=True, smooth_idf=True,
         min_df=2, max_df=0.95)

    docs = []

    csv.field_size_limit(sys.maxsize)
    with open(filepath) as f:
        reader = csv.reader(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
        for row in reader:
            docs.append(row[0])

    corpus = textacy.Corpus('en', docs)

    doc_term_matrix = vectorizer.fit_transform(
         (doc.to_terms_list(ngrams=1, named_entities=True, as_strings=True)
          for doc in corpus))

    return vectorizer.vocabulary, doc_term_matrix


vocabulary, doc_term_matrix = compute_tf_idf("/tmp/wikipedia-summaries.csv")


def preprocess_document(parser, document, document_index):
    parsed_document = parser(document)
    sentences = [sentence.string.strip() for sentence in parsed_document.sents]
    vectors = []
    for sentence in parsed_document.sents:
        vector = np.zeros(300)
        for word in sentence:
            w = word.string.lower().strip(" ,.?;)(")
            if w in vocabulary:
                vector += word.vector * doc_term_matrix[document_index, vocabulary[w]]
        vectors.append(vector)
    return sentences, vectors

class SummarizationEnv(gym.Env):

    def __init__(self, filepath):
        self.action_space = gym.spaces.Discrete(2)
        # Word2Vec of the last two words in the text and the last two
        # words in the summary
        self.observation_space = WordSequencePair(4, 4)
        self.scorer = Rouge()
        self.data = []
        print("Processing dataset, this can take a few minutes...")
        parser = English()
        csv.field_size_limit(sys.maxsize)
        with open(filepath) as f:
            reader = csv.reader(f, delimiter=",", quotechar="|", quoting=csv.QUOTE_MINIMAL)
            for i, row in enumerate(reader):
                sentences1, vectors1 = preprocess_document(parser, row[0], i)
                sentences2, vectors2 = preprocess_document(parser, row[1], i)
                self.data.append(Datapoint(text_sentences=sentences1,
                                           summary_sentences=sentences2,
                                           text_vectors=vectors1,
                                           summary_vectors=vectors2))
        self.reset()

    def reset(self):
        self.current_document = random.randint(1, len(self.data)-1)
        self.current_token = 0
        self.prediction_so_far = []
        self.last_score = 0.0
        self.done = False
        null_sentence = np.zeros(300)
        return (self.observation_space.past_context_size * [null_sentence],
                self.observation_space.future_context_size * [null_sentence])

    def step(self, action):
        null_sentence = np.zeros(300)
        text = self.data[self.current_document].text_sentences
        text_vectors = self.data[self.current_document].text_vectors
        summary = self.data[self.current_document].summary_sentences
        if action == 1:
            self.prediction_so_far.append(text[self.current_token])
        if self.done:
            return (self.observation_space.past_context_size * [null_sentence],
                    self.observation_space.future_context_size * [null_sentence]), 0.0, self.done, {}
        self.current_token += 1
        score = self.scorer.calculate_score(summary, self.prediction_so_far)
        reward = score - self.last_score
        self.last_score = score
        self.done = self.current_token == len(summary) or self.current_token >= len(text) - 1
        p = self.observation_space.past_context_size
        f = self.observation_space.future_context_size
        past = (p * [null_sentence] + text_vectors[:self.current_token])[-p:]
        future = (text_vectors[self.current_token:] + f * [null_sentence])[0:f]
        return (past, future), reward, self.done, {}

class Word2VecPreprocessor(Preprocessor):

    def transform_shape(self, obs_shape):
        return (4,)

    def transform(self, observation):
        past, future = observation
        return np.array([np.dot(future[0], p) for p in past])
