"""Example training a memory neural net on the bAbI dataset.

References Keras and is based off of https://keras.io/examples/babi_memnn/.
"""

from __future__ import print_function

from tensorflow.keras.models import Sequential, Model, load_model
from tensorflow.keras.layers import Embedding
from tensorflow.keras.layers import Input, Activation, Dense, Permute, Dropout
from tensorflow.keras.layers import add, dot, concatenate
from tensorflow.keras.layers import LSTM
from tensorflow.keras.optimizers import RMSprop
from tensorflow.keras.utils import get_file
from tensorflow.keras.preprocessing.sequence import pad_sequences

from filelock import FileLock
import os
import argparse
import tarfile
import numpy as np
import re

from ray import air, tune


def tokenize(sent):
    """Return the tokens of a sentence including punctuation.

    >>> tokenize("Bob dropped the apple. Where is the apple?")
    ["Bob", "dropped", "the", "apple", ".", "Where", "is", "the", "apple", "?"]
    """
    return [x.strip() for x in re.split(r"(\W+)?", sent) if x and x.strip()]


def parse_stories(lines, only_supporting=False):
    """Parse stories provided in the bAbi tasks format

    If only_supporting is true, only the sentences
    that support the answer are kept.
    """
    data = []
    story = []
    for line in lines:
        line = line.decode("utf-8").strip()
        nid, line = line.split(" ", 1)
        nid = int(nid)
        if nid == 1:
            story = []
        if "\t" in line:
            q, a, supporting = line.split("\t")
            q = tokenize(q)
            if only_supporting:
                # Only select the related substory
                supporting = map(int, supporting.split())
                substory = [story[i - 1] for i in supporting]
            else:
                # Provide all the substories
                substory = [x for x in story if x]
            data.append((substory, q, a))
            story.append("")
        else:
            sent = tokenize(line)
            story.append(sent)
    return data


def get_stories(f, only_supporting=False, max_length=None):
    """Given a file name, read the file,
    retrieve the stories,
    and then convert the sentences into a single story.

    If max_length is supplied,
    any stories longer than max_length tokens will be discarded.
    """

    def flatten(data):
        return sum(data, [])

    data = parse_stories(f.readlines(), only_supporting=only_supporting)
    data = [
        (flatten(story), q, answer)
        for story, q, answer in data
        if not max_length or len(flatten(story)) < max_length
    ]
    return data


def vectorize_stories(word_idx, story_maxlen, query_maxlen, data):
    inputs, queries, answers = [], [], []
    for story, query, answer in data:
        inputs.append([word_idx[w] for w in story])
        queries.append([word_idx[w] for w in query])
        answers.append(word_idx[answer])
    return (
        pad_sequences(inputs, maxlen=story_maxlen),
        pad_sequences(queries, maxlen=query_maxlen),
        np.array(answers),
    )


def read_data(finish_fast=False):
    # Get the file
    try:
        path = get_file(
            "babi-tasks-v1-2.tar.gz",
            origin="https://s3.amazonaws.com/text-datasets/"
            "babi_tasks_1-20_v1-2.tar.gz",
        )
    except Exception:
        print(
            "Error downloading dataset, please download it manually:\n"
            "$ wget http://www.thespermwhale.com/jaseweston/babi/tasks_1-20_v1-2"  # noqa: E501
            ".tar.gz\n"
            "$ mv tasks_1-20_v1-2.tar.gz ~/.keras/datasets/babi-tasks-v1-2.tar.gz"  # noqa: E501
        )
        raise

    # Choose challenge
    challenges = {
        # QA1 with 10,000 samples
        "single_supporting_fact_10k": "tasks_1-20_v1-2/en-10k/qa1_"
        "single-supporting-fact_{}.txt",
        # QA2 with 10,000 samples
        "two_supporting_facts_10k": "tasks_1-20_v1-2/en-10k/qa2_"
        "two-supporting-facts_{}.txt",
    }
    challenge_type = "single_supporting_fact_10k"
    challenge = challenges[challenge_type]

    with tarfile.open(path) as tar:
        train_stories = get_stories(tar.extractfile(challenge.format("train")))
        test_stories = get_stories(tar.extractfile(challenge.format("test")))
    if finish_fast:
        train_stories = train_stories[:64]
        test_stories = test_stories[:64]
    return train_stories, test_stories


class MemNNModel(tune.Trainable):
    def build_model(self):
        """Helper method for creating the model"""
        vocab = set()
        for story, q, answer in self.train_stories + self.test_stories:
            vocab |= set(story + q + [answer])
        vocab = sorted(vocab)

        # Reserve 0 for masking via pad_sequences
        vocab_size = len(vocab) + 1
        story_maxlen = max(len(x) for x, _, _ in self.train_stories + self.test_stories)
        query_maxlen = max(len(x) for _, x, _ in self.train_stories + self.test_stories)

        word_idx = {c: i + 1 for i, c in enumerate(vocab)}
        self.inputs_train, self.queries_train, self.answers_train = vectorize_stories(
            word_idx, story_maxlen, query_maxlen, self.train_stories
        )
        self.inputs_test, self.queries_test, self.answers_test = vectorize_stories(
            word_idx, story_maxlen, query_maxlen, self.test_stories
        )

        # placeholders
        input_sequence = Input((story_maxlen,))
        question = Input((query_maxlen,))

        # encoders
        # embed the input sequence into a sequence of vectors
        input_encoder_m = Sequential()
        input_encoder_m.add(Embedding(input_dim=vocab_size, output_dim=64))
        input_encoder_m.add(Dropout(self.config.get("dropout", 0.3)))
        # output: (samples, story_maxlen, embedding_dim)

        # embed the input into a sequence of vectors of size query_maxlen
        input_encoder_c = Sequential()
        input_encoder_c.add(Embedding(input_dim=vocab_size, output_dim=query_maxlen))
        input_encoder_c.add(Dropout(self.config.get("dropout", 0.3)))
        # output: (samples, story_maxlen, query_maxlen)

        # embed the question into a sequence of vectors
        question_encoder = Sequential()
        question_encoder.add(
            Embedding(input_dim=vocab_size, output_dim=64, input_length=query_maxlen)
        )
        question_encoder.add(Dropout(self.config.get("dropout", 0.3)))
        # output: (samples, query_maxlen, embedding_dim)

        # encode input sequence and questions (which are indices)
        # to sequences of dense vectors
        input_encoded_m = input_encoder_m(input_sequence)
        input_encoded_c = input_encoder_c(input_sequence)
        question_encoded = question_encoder(question)

        # compute a "match" between the first input vector sequence
        # and the question vector sequence
        # shape: `(samples, story_maxlen, query_maxlen)`
        match = dot([input_encoded_m, question_encoded], axes=(2, 2))
        match = Activation("softmax")(match)

        # add the match matrix with the second input vector sequence
        response = add(
            [match, input_encoded_c]
        )  # (samples, story_maxlen, query_maxlen)
        response = Permute((2, 1))(response)  # (samples, query_maxlen, story_maxlen)

        # concatenate the match matrix with the question vector sequence
        answer = concatenate([response, question_encoded])

        # the original paper uses a matrix multiplication.
        # we choose to use a RNN instead.
        answer = LSTM(32)(answer)  # (samples, 32)

        # one regularization layer -- more would probably be needed.
        answer = Dropout(self.config.get("dropout", 0.3))(answer)
        answer = Dense(vocab_size)(answer)  # (samples, vocab_size)
        # we output a probability distribution over the vocabulary
        answer = Activation("softmax")(answer)

        # build the final model
        model = Model([input_sequence, question], answer)
        return model

    def setup(self, config):
        with FileLock(os.path.expanduser("~/.tune.lock")):
            self.train_stories, self.test_stories = read_data(config["finish_fast"])
        model = self.build_model()
        rmsprop = RMSprop(
            lr=self.config.get("lr", 1e-3), rho=self.config.get("rho", 0.9)
        )
        model.compile(
            optimizer=rmsprop,
            loss="sparse_categorical_crossentropy",
            metrics=["accuracy"],
        )
        self.model = model

    def step(self):
        # train
        self.model.fit(
            [self.inputs_train, self.queries_train],
            self.answers_train,
            batch_size=self.config.get("batch_size", 32),
            epochs=self.config.get("epochs", 1),
            validation_data=([self.inputs_test, self.queries_test], self.answers_test),
            verbose=0,
        )
        _, accuracy = self.model.evaluate(
            [self.inputs_train, self.queries_train], self.answers_train, verbose=0
        )
        return {"mean_accuracy": accuracy}

    def save_checkpoint(self, checkpoint_dir):
        file_path = checkpoint_dir + "/model"
        self.model.save(file_path)
        return file_path

    def load_checkpoint(self, path):
        # See https://stackoverflow.com/a/42763323
        del self.model
        self.model = load_model(path)


if __name__ == "__main__":
    import ray
    from ray.tune.schedulers import PopulationBasedTraining

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--smoke-test", action="store_true", help="Finish quickly for testing"
    )
    parser.add_argument(
        "--server-address",
        type=str,
        default=None,
        required=False,
        help="The address of server to connect to if using Ray Client.",
    )
    args, _ = parser.parse_known_args()

    if args.smoke_test:
        ray.init(num_cpus=2)
    elif args.server_address:
        ray.init(f"ray://{args.server_address}")

    perturbation_interval = 2
    pbt = PopulationBasedTraining(
        perturbation_interval=perturbation_interval,
        hyperparam_mutations={
            "dropout": lambda: np.random.uniform(0, 1),
            "lr": lambda: 10 ** np.random.randint(-10, 0),
            "rho": lambda: np.random.uniform(0, 1),
        },
    )

    tuner = tune.Tuner(
        MemNNModel,
        run_config=air.RunConfig(
            name="pbt_babi_memnn",
            stop={"training_iteration": 4 if args.smoke_test else 100},
            checkpoint_config=air.CheckpointConfig(
                checkpoint_frequency=perturbation_interval,
                checkpoint_score_attribute="mean_accuracy",
                num_to_keep=2,
            ),
        ),
        tune_config=tune.TuneConfig(
            scheduler=pbt,
            metric="mean_accuracy",
            mode="max",
            num_samples=2,
        ),
        param_space={
            "finish_fast": args.smoke_test,
            "batch_size": 32,
            "epochs": 1,
            "dropout": 0.3,
            "lr": 0.01,
            "rho": 0.9,
        },
    )
    tuner.fit()
