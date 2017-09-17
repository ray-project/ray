# Semantic similarity with decomposable attention (using spaCy and Keras)
# Practical state-of-the-art text similarity with spaCy and Keras
import numpy

from keras.layers import InputSpec, Layer, Input, Dense, merge
from keras.layers import Lambda, Activation, Dropout, Embedding, TimeDistributed
from keras.layers import Bidirectional, GRU, LSTM
from keras.layers.noise import GaussianNoise
from keras.layers.advanced_activations import ELU
import keras.backend as K
from keras.models import Sequential, Model, model_from_json
from keras.regularizers import l2
from keras.optimizers import Adam
from keras.layers.normalization import BatchNormalization
from keras.layers.pooling import GlobalAveragePooling1D, GlobalMaxPooling1D
from keras.layers import Merge


def build_model(vectors, shape, settings):
    '''Compile the model.'''
    max_length, nr_hidden, nr_class = shape
    # Declare inputs.
    ids1 = Input(shape=(max_length,), dtype='int32', name='words1')
    ids2 = Input(shape=(max_length,), dtype='int32', name='words2')

    # Construct operations, which we'll chain together.
    embed = _StaticEmbedding(vectors, max_length, nr_hidden, dropout=0.2, nr_tune=5000)
    if settings['gru_encode']:
        encode = _BiRNNEncoding(max_length, nr_hidden, dropout=settings['dropout'])
    attend = _Attention(max_length, nr_hidden, dropout=settings['dropout'])
    align = _SoftAlignment(max_length, nr_hidden)
    compare = _Comparison(max_length, nr_hidden, dropout=settings['dropout'])
    entail = _Entailment(nr_hidden, nr_class, dropout=settings['dropout'])

    # Declare the model as a computational graph.
    sent1 = embed(ids1) # Shape: (i, n)
    sent2 = embed(ids2) # Shape: (j, n)

    if settings['gru_encode']:
        sent1 = encode(sent1)
        sent2 = encode(sent2)

    attention = attend(sent1, sent2)  # Shape: (i, j)

    align1 = align(sent2, attention)
    align2 = align(sent1, attention, transpose=True)

    feats1 = compare(sent1, align1)
    feats2 = compare(sent2, align2)

    scores = entail(feats1, feats2)

    # Now that we have the input/output, we can construct the Model object...
    model = Model(input=[ids1, ids2], output=[scores])

    # ...Compile it...
    model.compile(
        optimizer=Adam(lr=settings['lr']),
        loss='categorical_crossentropy',
        metrics=['accuracy'])
    # ...And return it for training.
    return model


class _StaticEmbedding(object):
    def __init__(self, vectors, max_length, nr_out, nr_tune=1000, dropout=0.0):
        self.nr_out = nr_out
        self.max_length = max_length
        self.embed = Embedding(
                        vectors.shape[0],
                        vectors.shape[1],
                        input_length=max_length,
                        weights=[vectors],
                        name='embed',
                        trainable=False)
        self.tune = Embedding(
                        nr_tune,
                        nr_out,
                        input_length=max_length,
                        weights=None,
                        name='tune',
                        trainable=True,
                        dropout=dropout)
        self.mod_ids = Lambda(lambda sent: sent % (nr_tune-1)+1,
                              output_shape=(self.max_length,))

        self.project = TimeDistributed(
                            Dense(
                                nr_out,
                                activation=None,
                                bias=False,
                                name='project'))

    def __call__(self, sentence):
        def get_output_shape(shapes):
            print(shapes)
            return shapes[0]
        mod_sent = self.mod_ids(sentence)
        tuning = self.tune(mod_sent)
        #tuning = merge([tuning, mod_sent],
        #    mode=lambda AB: AB[0] * (K.clip(K.cast(AB[1], 'float32'), 0, 1)),
        #    output_shape=(self.max_length, self.nr_out))
        pretrained = self.project(self.embed(sentence))
        vectors = merge([pretrained, tuning], mode='sum')
        return vectors


class _BiRNNEncoding(object):
    def __init__(self, max_length, nr_out, dropout=0.0):
        self.model = Sequential()
        self.model.add(Bidirectional(LSTM(nr_out, return_sequences=True,
                                         dropout_W=dropout, dropout_U=dropout),
                                         input_shape=(max_length, nr_out)))
        self.model.add(TimeDistributed(Dense(nr_out, activation='relu', init='he_normal')))
        self.model.add(TimeDistributed(Dropout(0.2)))

    def __call__(self, sentence):
        return self.model(sentence)


class _Attention(object):
    def __init__(self, max_length, nr_hidden, dropout=0.0, L2=0.0, activation='relu'):
        self.max_length = max_length
        self.model = Sequential()
        self.model.add(Dropout(dropout, input_shape=(nr_hidden,)))
        self.model.add(
            Dense(nr_hidden, name='attend1',
                init='he_normal', W_regularizer=l2(L2),
                input_shape=(nr_hidden,), activation='relu'))
        self.model.add(Dropout(dropout))
        self.model.add(Dense(nr_hidden, name='attend2',
            init='he_normal', W_regularizer=l2(L2), activation='relu'))
        self.model = TimeDistributed(self.model)

    def __call__(self, sent1, sent2):
        def _outer(AB):
            att_ji = K.batch_dot(AB[1], K.permute_dimensions(AB[0], (0, 2, 1)))
            return K.permute_dimensions(att_ji,(0, 2, 1))
        return merge(
                [self.model(sent1), self.model(sent2)],
                mode=_outer,
                output_shape=(self.max_length, self.max_length))


class _SoftAlignment(object):
    def __init__(self, max_length, nr_hidden):
        self.max_length = max_length
        self.nr_hidden = nr_hidden

    def __call__(self, sentence, attention, transpose=False):
        def _normalize_attention(attmat):
            att = attmat[0]
            mat = attmat[1]
            if transpose:
                att = K.permute_dimensions(att,(0, 2, 1))
            # 3d softmax
            e = K.exp(att - K.max(att, axis=-1, keepdims=True))
            s = K.sum(e, axis=-1, keepdims=True)
            sm_att = e / s
            return K.batch_dot(sm_att, mat)
        return merge([attention, sentence], mode=_normalize_attention,
                      output_shape=(self.max_length, self.nr_hidden)) # Shape: (i, n)


class _Comparison(object):
    def __init__(self, words, nr_hidden, L2=0.0, dropout=0.0):
        self.words = words
        self.model = Sequential()
        self.model.add(Dropout(dropout, input_shape=(nr_hidden*2,)))
        self.model.add(Dense(nr_hidden, name='compare1',
            init='he_normal', W_regularizer=l2(L2)))
        self.model.add(Activation('relu'))
        self.model.add(Dropout(dropout))
        self.model.add(Dense(nr_hidden, name='compare2',
                        W_regularizer=l2(L2), init='he_normal'))
        self.model.add(Activation('relu'))
        self.model = TimeDistributed(self.model)

    def __call__(self, sent, align, **kwargs):
        result = self.model(merge([sent, align], mode='concat')) # Shape: (i, n)
        avged = GlobalAveragePooling1D()(result) # , mask=self.words)
        maxed = GlobalMaxPooling1D()(result) # , mask=self.words)
        merged = merge([avged, maxed])
        result = BatchNormalization()(merged)
        return result


class _Entailment(object):
    def __init__(self, nr_hidden, nr_out, dropout=0.0, L2=0.0):
        self.model = Sequential()
        self.model.add(Dropout(dropout, input_shape=(nr_hidden*2,)))
        self.model.add(Dense(nr_hidden, name='entail1',
            init='he_normal', W_regularizer=l2(L2)))
        self.model.add(Activation('relu'))
        self.model.add(Dropout(dropout))
        self.model.add(Dense(nr_hidden, name='entail2',
            init='he_normal', W_regularizer=l2(L2)))
        self.model.add(Activation('relu'))
        self.model.add(Dense(nr_out, name='entail_out', activation='softmax',
                        W_regularizer=l2(L2), init='zero'))

    def __call__(self, feats1, feats2):
        features = merge([feats1, feats2], mode='concat')
        return self.model(features)


class _GlobalSumPooling1D(Layer):
    '''Global sum pooling operation for temporal data.

    # Input shape
        3D tensor with shape: `(samples, steps, features)`.

    # Output shape
        2D tensor with shape: `(samples, features)`.
    '''
    def __init__(self, **kwargs):
        super(_GlobalSumPooling1D, self).__init__(**kwargs)
        self.input_spec = [InputSpec(ndim=3)]

    def get_output_shape_for(self, input_shape):
        return (input_shape[0], input_shape[2])

    def call(self, x, mask=None):
        if mask is not None:
            return K.sum(x * K.clip(mask, 0, 1), axis=1)
        else:
            return K.sum(x, axis=1)


def test_build_model():
    vectors = numpy.ndarray((100, 8), dtype='float32')
    shape = (10, 16, 3)
    settings = {'lr': 0.001, 'dropout': 0.2, 'gru_encode':True}
    model = build_model(vectors, shape, settings)


def test_fit_model():

    def _generate_X(nr_example, length, nr_vector):
        X1 = numpy.ndarray((nr_example, length), dtype='int32')
        X1 *= X1 < nr_vector
        X1 *= 0 <= X1
        X2 = numpy.ndarray((nr_example, length), dtype='int32')
        X2 *= X2 < nr_vector
        X2 *= 0 <= X2
        return [X1, X2]

    def _generate_Y(nr_example, nr_class):
        ys = numpy.zeros((nr_example, nr_class), dtype='int32')
        for i in range(nr_example):
            ys[i, i % nr_class] = 1
        return ys

    vectors = numpy.ndarray((100, 8), dtype='float32')
    shape = (10, 16, 3)
    settings = {'lr': 0.001, 'dropout': 0.2, 'gru_encode':True}
    model = build_model(vectors, shape, settings)

    train_X = _generate_X(20, shape[0], vectors.shape[0])
    train_Y = _generate_Y(20, shape[2])
    dev_X = _generate_X(15, shape[0], vectors.shape[0])
    dev_Y = _generate_Y(15, shape[2])

    model.fit(train_X, train_Y, validation_data=(dev_X, dev_Y), nb_epoch=5,
              batch_size=4)


__all__ = [build_model]
