import pickle
import spacy
from .keras_decomposable_attention import build_model
import numpy


class DocumentSimilarity(object):

    def __init__(self):
        nlp = spacy.load('en')
        self.max_length=100
        nr_hidden=100

        shape = (self.max_length, nr_hidden, 3)
        settings = {
            'lr': 0.001,
            'dropout': 0.2,
            'batch_size': 100,
            'nr_epoch': 100,
            'tree_truncate': False,
            'gru_encode': False
        }

        self.model = build_model(get_embeddings(nlp.vocab), shape, settings)

        embeddings = get_embeddings(nlp.vocab)
        weights = pickle.load(open("/home/ubuntu/anaconda3/lib/python3.6/site-packages/spacy/data/en/similarity/model", "rb"))

        self.model.set_weights([embeddings] + weights)

    def __call__(self, doc):
        doc.user_hooks['similarity'] = self.predict
        doc.user_span_hooks['similarity'] = self.predict

    def predict(self, doc1, doc2):
        x1 = get_word_ids([doc1], max_length=self.max_length, tree_truncate=True)
        x2 = get_word_ids([doc2], max_length=self.max_length, tree_truncate=True)
        scores = self.model.predict([x1, x2])
        return scores[0]

def create_similarity_pipeline(nlp, max_length=100):
    return [
        nlp.tagger,
        nlp.entity,
        nlp.parser,
        DocumentSimilarity()
    ]

# nlp = spacy.load('en', create_pipeline=create_similarity_pipeline)

# from https://github.com/explosion/spaCy/blob/master/examples/keras_parikh_entailment/spacy_hook.py
def get_embeddings(vocab, nr_unk=100):
    nr_vector = max(lex.rank for lex in vocab) + 1
    vectors = numpy.zeros((nr_vector+nr_unk+2, vocab.vectors_length), dtype='float32')
    for lex in vocab:
        if lex.has_vector:
            vectors[lex.rank+1] = lex.vector / lex.vector_norm
    return vectors


# from https://github.com/explosion/spaCy/blob/master/examples/keras_parikh_entailment/spacy_hook.py
def get_word_ids(docs, rnn_encode=False, tree_truncate=False, max_length=100, nr_unk=100):
    Xs = numpy.zeros((len(docs), max_length), dtype='int32')
    for i, doc in enumerate(docs):
        if tree_truncate:
            if isinstance(doc, Span):
                queue = [doc.root]
            else:
                queue = [sent.root for sent in doc.sents]
        else:
            queue = list(doc)
        words = []
        while len(words) <= max_length and queue:
            word = queue.pop(0)
            if rnn_encode or (not word.is_punct and not word.is_space):
                words.append(word)
            if tree_truncate:
                queue.extend(list(word.lefts))
                queue.extend(list(word.rights))
        words.sort()
        for j, token in enumerate(words):
            if token.has_vector:
                Xs[i, j] = token.rank+1
            else:
                Xs[i, j] = (token.shape % (nr_unk-1))+2
            j += 1
            if j >= max_length:
                break
        else:
            Xs[i, len(words)] = 1
    return Xs
