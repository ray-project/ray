from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import pandas as pd

from pandas.core.dtypes.common import is_list_like


class Index(object):

    def __init__(self, idx, pandas_type, name=None, names=None):
        self.idx = idx
        self.pandas_type = pandas_type
        self.name = name
        self.names = names

    def __getitem__(self, item):
        return self.idx[item]

    def __len__(self):
        return len(self.idx)

    def _set_names(self, values, level=None):
        if len(values) != 1:
            raise ValueError('Length of new names must be 1, got %d' %
                             len(values))
        self.name = values[0]

    def set_names(self, names, level=None, inplace=False):
        """
        Set new names on index. Defaults to returning new index.
        Parameters
        ----------
        names : str or sequence
            name(s) to set
        level : int, level name, or sequence of int/level names (default None)
            If the index is a MultiIndex (hierarchical), level(s) to set (None
            for all levels).  Otherwise level must be None
        inplace : bool
            if True, mutates in place
        Returns
        -------
        new index (of same type and class...etc) [if inplace, returns None]
        Examples
        --------
        >>> Index([1, 2, 3, 4]).set_names('foo')
        Int64Index([1, 2, 3, 4], dtype='int64')
        >>> Index([1, 2, 3, 4]).set_names(['foo'])
        Int64Index([1, 2, 3, 4], dtype='int64')
        >>> idx = MultiIndex.from_tuples([(1, u'one'), (1, u'two'),
                                          (2, u'one'), (2, u'two')],
                                          names=['foo', 'bar'])
        >>> idx.set_names(['baz', 'quz'])
        MultiIndex(levels=[[1, 2], [u'one', u'two']],
                   labels=[[0, 0, 1, 1], [0, 1, 0, 1]],
                   names=[u'baz', u'quz'])
        >>> idx.set_names('baz', level=0)
        MultiIndex(levels=[[1, 2], [u'one', u'two']],
                   labels=[[0, 0, 1, 1], [0, 1, 0, 1]],
                   names=[u'baz', u'bar'])
        """

        if level is not None and not is_list_like(level) and is_list_like(
                names):
            raise TypeError("Names must be a string")

        if not is_list_like(names):
            names = [names]
        if level is not None and not is_list_like(level):
            level = [level]

        if inplace:
            idx = self
        else:
            idx = self.copy()
        idx._set_names(names, level=level)
        if not inplace:
            return idx

    def copy(self, deep=True):
        """
        Make a copy of this objects data.
        Parameters
        ----------
        deep : boolean or string, default True
            Make a deep copy, including a copy of the data and the indices.
            With ``deep=False`` neither the indices or the data are copied.
            Note that when ``deep=True`` data is copied, actual python objects
            will not be copied recursively, only the reference to the object.
            This is in contrast to ``copy.deepcopy`` in the Standard Library,
            which recursively copies object data.
        Returns
        -------
        copy : type of caller
        """
        # TODO: Implement deep copy vs. shallow copy
        return Index(self.idx.copy(), self.pandas_type, name=self.name,
                     names=self.names.copy())

    @classmethod
    def to_pandas(cls, index):
        """Convert a Ray Index object to a Pandas Index object.

        Args:
            index (ray.Index): A Ray Index object.

        Returns:
            A pandas Index object.
        """
        k = sorted(index.idx, key=index.idx.__getitem__)
        if index.pandas_type is pd.RangeIndex:
            return pd.RangeIndex(min(k), max(k)+1, name=index.name,
                                 names=index.names)
        else:
            return pd.Index(k, name=index.name, names=index.names)

    @classmethod
    def from_pandas(cls, pd_index, lengths):
        """Convert a Pandas Index object to a Ray Index object.

        Args:
            pd_index (pd.Index): A Pandas Index object.
            lengths ([int]): A list of lengths for the partitions.

        Returns:
            A Ray Index object.
        """
        dest_indices = [(i, j)
                        for i in range(len(lengths))
                        for j in range(lengths[i])]
        if len(pd_index) != len(dest_indices):
            raise ValueError(
              "Length of index given does not match current dataframe")

        return Index(
          {pd_index[i]: dest_indices[i] for i in range(len(dest_indices))},
          type(pd_index), name=pd_index.name, names=pd_index.names)
