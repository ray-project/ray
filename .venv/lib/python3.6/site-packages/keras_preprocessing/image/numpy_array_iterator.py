"""Utilities for real-time data augmentation on image data.
"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import warnings
import numpy as np

from .iterator import Iterator
from .utils import array_to_img


class NumpyArrayIterator(Iterator):
    """Iterator yielding data from a Numpy array.

    # Arguments
        x: Numpy array of input data or tuple.
            If tuple, the second elements is either
            another numpy array or a list of numpy arrays,
            each of which gets passed
            through as an output without any modifications.
        y: Numpy array of targets data.
        image_data_generator: Instance of `ImageDataGenerator`
            to use for random transformations and normalization.
        batch_size: Integer, size of a batch.
        shuffle: Boolean, whether to shuffle the data between epochs.
        sample_weight: Numpy array of sample weights.
        seed: Random seed for data shuffling.
        data_format: String, one of `channels_first`, `channels_last`.
        save_to_dir: Optional directory where to save the pictures
            being yielded, in a viewable format. This is useful
            for visualizing the random transformations being
            applied, for debugging purposes.
        save_prefix: String prefix to use for saving sample
            images (if `save_to_dir` is set).
        save_format: Format to use for saving sample images
            (if `save_to_dir` is set).
        subset: Subset of data (`"training"` or `"validation"`) if
            validation_split is set in ImageDataGenerator.
        dtype: Dtype to use for the generated arrays.
    """

    def __init__(self,
                 x,
                 y,
                 image_data_generator,
                 batch_size=32,
                 shuffle=False,
                 sample_weight=None,
                 seed=None,
                 data_format='channels_last',
                 save_to_dir=None,
                 save_prefix='',
                 save_format='png',
                 subset=None,
                 dtype='float32'):
        self.dtype = dtype
        if (type(x) is tuple) or (type(x) is list):
            if type(x[1]) is not list:
                x_misc = [np.asarray(x[1])]
            else:
                x_misc = [np.asarray(xx) for xx in x[1]]
            x = x[0]
            for xx in x_misc:
                if len(x) != len(xx):
                    raise ValueError(
                        'All of the arrays in `x` '
                        'should have the same length. '
                        'Found a pair with: len(x[0]) = %s, len(x[?]) = %s' %
                        (len(x), len(xx)))
        else:
            x_misc = []

        if y is not None and len(x) != len(y):
            raise ValueError('`x` (images tensor) and `y` (labels) '
                             'should have the same length. '
                             'Found: x.shape = %s, y.shape = %s' %
                             (np.asarray(x).shape, np.asarray(y).shape))
        if sample_weight is not None and len(x) != len(sample_weight):
            raise ValueError('`x` (images tensor) and `sample_weight` '
                             'should have the same length. '
                             'Found: x.shape = %s, sample_weight.shape = %s' %
                             (np.asarray(x).shape, np.asarray(sample_weight).shape))
        if subset is not None:
            if subset not in {'training', 'validation'}:
                raise ValueError('Invalid subset name:', subset,
                                 '; expected "training" or "validation".')
            split_idx = int(len(x) * image_data_generator._validation_split)

            if (y is not None and not
                np.array_equal(np.unique(y[:split_idx]),
                               np.unique(y[split_idx:]))):
                raise ValueError('Training and validation subsets '
                                 'have different number of classes after '
                                 'the split. If your numpy arrays are '
                                 'sorted by the label, you might want '
                                 'to shuffle them.')

            if subset == 'validation':
                x = x[:split_idx]
                x_misc = [np.asarray(xx[:split_idx]) for xx in x_misc]
                if y is not None:
                    y = y[:split_idx]
            else:
                x = x[split_idx:]
                x_misc = [np.asarray(xx[split_idx:]) for xx in x_misc]
                if y is not None:
                    y = y[split_idx:]

        self.x = np.asarray(x, dtype=self.dtype)
        self.x_misc = x_misc
        if self.x.ndim != 4:
            raise ValueError('Input data in `NumpyArrayIterator` '
                             'should have rank 4. You passed an array '
                             'with shape', self.x.shape)
        channels_axis = 3 if data_format == 'channels_last' else 1
        if self.x.shape[channels_axis] not in {1, 3, 4}:
            warnings.warn('NumpyArrayIterator is set to use the '
                          'data format convention "' + data_format + '" '
                          '(channels on axis ' + str(channels_axis) +
                          '), i.e. expected either 1, 3, or 4 '
                          'channels on axis ' + str(channels_axis) + '. '
                          'However, it was passed an array with shape ' +
                          str(self.x.shape) + ' (' +
                          str(self.x.shape[channels_axis]) + ' channels).')
        if y is not None:
            self.y = np.asarray(y)
        else:
            self.y = None
        if sample_weight is not None:
            self.sample_weight = np.asarray(sample_weight)
        else:
            self.sample_weight = None
        self.image_data_generator = image_data_generator
        self.data_format = data_format
        self.save_to_dir = save_to_dir
        self.save_prefix = save_prefix
        self.save_format = save_format
        super(NumpyArrayIterator, self).__init__(x.shape[0],
                                                 batch_size,
                                                 shuffle,
                                                 seed)

    def _get_batches_of_transformed_samples(self, index_array):
        batch_x = np.zeros(tuple([len(index_array)] + list(self.x.shape)[1:]),
                           dtype=self.dtype)
        for i, j in enumerate(index_array):
            x = self.x[j]
            params = self.image_data_generator.get_random_transform(x.shape)
            x = self.image_data_generator.apply_transform(
                x.astype(self.dtype), params)
            x = self.image_data_generator.standardize(x)
            batch_x[i] = x

        if self.save_to_dir:
            for i, j in enumerate(index_array):
                img = array_to_img(batch_x[i], self.data_format, scale=True)
                fname = '{prefix}_{index}_{hash}.{format}'.format(
                    prefix=self.save_prefix,
                    index=j,
                    hash=np.random.randint(1e4),
                    format=self.save_format)
                img.save(os.path.join(self.save_to_dir, fname))
        batch_x_miscs = [xx[index_array] for xx in self.x_misc]
        output = (batch_x if batch_x_miscs == []
                  else [batch_x] + batch_x_miscs,)
        if self.y is None:
            return output[0]
        output += (self.y[index_array],)
        if self.sample_weight is not None:
            output += (self.sample_weight[index_array],)
        return output
