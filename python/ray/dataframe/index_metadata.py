import pandas as pd
import numpy as np
import ray

from .utils import (
    _build_row_lengths,
    _build_col_widths,
    _build_coord_df)

from pandas.core.indexing import convert_to_index_sliceable


class _IndexMetadata(object):
    """Wrapper for Pandas indexes in Ray DataFrames. Handles all of the
    metadata specific to the axis of partition (setting indexes,
    calculating the index within partition of a value, etc.). This
    implementation assumes the underlying index lies across multiple
    partitions.

    IMPORTANT NOTE: Currently all operations, as implemented, are inplace.
    """

    def __init__(self, dfs=None, index=None, axis=0, lengths_oid=None,
                 coord_df_oid=None):
        """Inits a IndexMetadata from Ray DataFrame partitions

        Args:
            dfs ([ObjectID]): ObjectIDs of dataframe partitions
            index (pd.Index): Index of the Ray DataFrame.
            axis: Axis of partition (0=row partitions, 1=column partitions)

        Returns:
            A IndexMetadata backed by the specified pd.Index, partitioned off
            specified partitions
        """
        assert (lengths_oid is None) == (coord_df_oid is None), \
            "Must pass both or neither of lengths_oid and coord_df_oid"

        if dfs is not None and lengths_oid is None:
            if axis == 0:
                lengths_oid = _build_row_lengths.remote(dfs)
            else:
                lengths_oid = _build_col_widths.remote(dfs)
            coord_df_oid = _build_coord_df.remote(lengths_oid, index)

        self._coord_df = coord_df_oid
        self._lengths = lengths_oid
        self._index_cache = index
        self._new_index = False

    def _get__coord_df(self):
        if isinstance(self._coord_df_cache, ray.local_scheduler.ObjectID):
            self._coord_df_cache = ray.get(self._coord_df_cache)
        if self._new_index:
            self._coord_df_cache.index = self.index
        return self._coord_df_cache

    def _set__coord_df(self, coord_df):
        # Sometimes we set the _IndexMetadata's coord_df outside of the
        # constructor, generally using fxns like squeeze(), drop(), etc.
        # This produces a modified index, so we need to reflec the change on
        # the index cache
        if not isinstance(coord_df, ray.local_scheduler.ObjectID):
            self._index_cache = coord_df.index
        self._coord_df_cache = coord_df

    _coord_df = property(_get__coord_df, _set__coord_df)

    def _get__lengths(self):
        if isinstance(self._lengths_cache, ray.local_scheduler.ObjectID) or \
            (isinstance(self._lengths_cache, list) and
             isinstance(self._lengths_cache[0], ray.local_scheduler.ObjectID)):
            self._lengths_cache = ray.get(self._lengths_cache)
        return self._lengths_cache

    def _set__lengths(self, lengths):
        self._lengths_cache = lengths

    _lengths = property(_get__lengths, _set__lengths)

    def _get_index(self):
        """Get the index wrapped by this IndexDF.

        Returns:
            The index wrapped by this IndexDF
        """
        if self._index_cache is None:
            self._index_cache = pd.RangeIndex(len(self))
        print(self._index_cache)
        return self._index_cache

    def _set_index(self, new_index):
        """Set the index wrapped by this IndexDF.

        Args:
            new_index: The new index to wrap
        """
        new_index = pd.DataFrame(index=new_index).index
        assert len(new_index) == len(self)
        self._index_cache = new_index
        self._new_index = True

    index = property(_get_index, _set_index)

    def coords_of(self, key):
        """Returns the coordinates (partition, index_within_partition) of the
        provided key in the index. Can be called on its own or implicitly
        through __getitem__

        Args:
            key:
                item to get coordinates of. Can also be a tuple of item
                and {"partition", "index_within_partition"} if caller only
                needs one of the coordinates

        Returns:
            Pandas object with the keys specified. If key is a single object
            it will be a pd.Series with items `partition` and
            `index_within_partition`, and if key is a slice or if the key is
            duplicate it will be a pd.DataFrame with said items as columns.
        """
        return self._coord_df.loc[key]

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        # TODO: Find out what this does, and write a docstring
        assignments_df = self._coord_df.groupby(by=by, axis=axis, level=level,
                                                as_index=as_index, sort=sort,
                                                group_keys=group_keys,
                                                squeeze=squeeze, **kwargs)\
            .apply(lambda x: x[:])
        return assignments_df

    def partition_series(self, partition):
        return self[self._coord_df['partition'] == partition,
                    'index_within_partition']

    def __len__(self):
        return sum(self._lengths)

    def reset_partition_coords(self, partitions=None):
        partitions = np.array(partitions)

        for partition in partitions:
            partition_mask = (self._coord_df['partition'] == partition)
            # Since we are replacing columns with RangeIndex inside the
            # partition, we have to make sure that our reference to it is
            # updated as well.
            try:
                self._coord_df.loc[partition_mask,
                                   'index_within_partition'] = [
                    p for p in range(sum(partition_mask))]
            except ValueError:
                # Copy the arrow sealed dataframe so we can mutate it.
                # We only do this the first time we try to mutate the sealed.
                self._coord_df = self._coord_df.copy()
                self._coord_df.loc[partition_mask,
                                   'index_within_partition'] = [
                    p for p in range(sum(partition_mask))]

    def insert(self, key, loc=None, partition=None,
               index_within_partition=None):
        """Inserts a key at a certain location in the index, or a certain coord
        in a partition. Called with either `loc` or `partition` and
        `index_within_partition`. If called with both, `loc` will be used.

        Args:
            key: item to insert into index
            loc: location to insert into index
            partition: partition to insert into
            index_within_partition: index within partition to insert into

        Returns:
            DataFrame with coordinates of insert
        """
        # Perform insert on a specific partition
        # Determine which partition to place it in, and where in that partition
        if loc is not None:
            cum_lens = np.cumsum(self._lengths)
            partition = np.digitize(loc, cum_lens[:-1])
            if partition >= len(cum_lens):
                if loc > cum_lens[-1]:
                    raise IndexError("index {0} is out of bounds".format(loc))
                else:
                    index_within_partition = self._lengths[-1]
            else:
                first_in_partition = \
                        np.asscalar(np.concatenate(([0], cum_lens))[partition])
                index_within_partition = loc - first_in_partition

        # TODO: Stop-gap solution until we begin passing IndexMetadatas
        return partition, index_within_partition

        # Generate new index
        new_index = self.index.insert(loc, key)

        # Shift indices in partition where we inserted column
        idx_locs = (self._coord_df.partition == partition) & \
                   (self._coord_df.index_within_partition ==
                    index_within_partition)
        # TODO: Determine why self._coord_df{,_cache} are read-only
        _coord_df_copy = self._coord_df.copy()
        _coord_df_copy.loc[idx_locs, 'index_within_partition'] += 1

        # TODO: Determine if there's a better way to do a row-index insert in
        # pandas, because this is very annoying/unsure of efficiency
        # Create new coord entry to insert
        coord_to_insert = pd.DataFrame(
                {'partition': partition,
                 'index_within_partition': index_within_partition},
                index=[key])

        # Insert into cached RangeIndex, and order by new column index
        self._coord_df = _coord_df_copy.append(coord_to_insert).loc[new_index]

        # Return inserted coordinate for callee
        return coord_to_insert

    def squeeze(self, partition, index_within_partition):
        self._coord_df = self._coord_df.copy()

        partition_mask = self._coord_df.partition == partition
        index_within_partition_mask = \
            self._coord_df.index_within_partition > index_within_partition
        self._coord_df.loc[partition_mask & index_within_partition_mask,
                           'index_within_partition'] -= 1

    def copy(self):
        # TODO: Investigate copy-on-write wrapper for metadata objects
        coord_df_copy = self._coord_df_cache
        if not isinstance(self._coord_df_cache, ray.local_scheduler.ObjectID):
            coord_df_copy = self._coord_df_cache.copy()

        lengths_copy = self._lengths_cache
        if not isinstance(self._lengths_cache, ray.local_scheduler.ObjectID):
            lengths_copy = self._lengths_cache.copy()

        index_copy = self._index_cache
        if self._index_cache is not None:
            index_copy = self._index_cache.copy()

        return _IndexMetadata(index=index_copy,
                              coord_df_oid=coord_df_copy,
                              lengths_oid=lengths_copy)

    def __getitem__(self, key):
        """Returns the coordinates (partition, index_within_partition) of the
        provided key in the index. Essentially just an alias for
        `_IndexMetadata.coords_of` that allows for slice passing, since
        slices cannot be passed with slice notation other than through
        `__getitem__` calls.

        Args:
            key:
                item to get coordinates of. Can also be a tuple of item
                and {"partition", "index_within_partition"} if caller only
                needs one of the coordinates

        Returns:
            Pandas object with the keys specified. If key is a single object
            it will be a pd.Series with items `partition` and
            `index_within_partition`, and if key is a slice or if the key is
            duplicate it will be a pd.DataFrame with said items as columns.
        """
        return self.coords_of(key)

    def first_valid_index(self):
        return self._coord_df.first_valid_index()

    def last_valid_index(self):
        return self._coord_df.last_valid_index()

    def drop(self, labels, errors='raise'):
        """Drop the specified labels from the IndexMetadata

        Args:
            labels (scalar or list-like):
                The labels to drop
            errors ('raise' or 'ignore'):
                If 'ignore', suppress errors for when labels don't exist

        Returns:
            DataFrame with coordinates of dropped labels
        """
        dropped = self.coords_of(labels)
        self._coord_df = self._coord_df.drop(labels, errors=errors)
        return dropped

    def rename_index(self, mapper):
        """Rename the index.

        Args:
            mapper: name to rename the index as
        """
        self._coord_df = self._coord_df.rename_axis(mapper, axis=0)

    def convert_to_index_sliceable(self, key):
        """Converts and performs error checking on the passed slice

        Args:
            key: slice to convert and check
        """
        return convert_to_index_sliceable(self._coord_df, key)
