import pickle
import spacy
from spacy_hook import get_embeddings, get_word_ids
from keras_decomposable_attention import build_model


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
