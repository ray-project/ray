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

    WARNING: Currently, the `_lengths` item is the source of truth for an
    _IndexMetadata object, since it is easy to manage, and that the coord_df
    item may be deprecated in the future. As such, it is _very_ important that
    any functions that mutate the coord_df splits in anyway first modify the
    lengths. Otherwise bad things might happen!
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

        self._lengths = lengths_oid
        self._coord_df = coord_df_oid
        self._index_cache = index
        self._cached_index = False

    def _get__lengths(self):
        if isinstance(self._lengths_cache, ray.ObjectID) or \
            (isinstance(self._lengths_cache, list) and
             isinstance(self._lengths_cache[0], ray.ObjectID)):
            self._lengths_cache = ray.get(self._lengths_cache)
        return self._lengths_cache

    def _set__lengths(self, lengths):
        self._lengths_cache = lengths

    _lengths = property(_get__lengths, _set__lengths)

    def _get__coord_df(self):
        """Get the coordinate dataframe wrapped by this _IndexMetadata.

        Since we may have had an index set before our coord_df was
        materialized, we'll have to apply it to the newly materialized df
        """
        if isinstance(self._coord_df_cache, ray.ObjectID):
            self._coord_df_cache = ray.get(self._coord_df_cache)
        if self._cached_index:
            self._coord_df_cache.index = self._index_cache
            self._cached_index = False
        return self._coord_df_cache

    def _set__coord_df(self, coord_df):
        """Set the coordinate dataframe wrapped by this _IndexMetadata.

        Sometimes we set the _IndexMetadata's coord_df outside of the
        constructor, generally using fxns like drop(). This produces a modified
        index, so we need to reflect the change on the index cache.

        If the set _IndexMetadata is an OID instead (due to a copy or whatever
        reason), we fall back relying on `_index_cache`.
        """
        if not isinstance(coord_df, ray.ObjectID):
            self._index_cache = coord_df.index
        self._coord_df_cache = coord_df

    _coord_df = property(_get__coord_df, _set__coord_df)

    def _get_index(self):
        """Get the index wrapped by this _IndexMetadata.

        The only time `self._index_cache` would be None is in a newly created
        _IndexMetadata object without a specified `index` parameter (See the
        _IndexMetadata constructor for more details)
        """
        if isinstance(self._coord_df_cache, ray.ObjectID):
            return self._index_cache
        else:
            return self._coord_df_cache.index

    def _set_index(self, new_index):
        """Set the index wrapped by this _IndexMetadata.

        It is important to always set `_index_cache` even if the coord_df is
        materialized due to the possibility that it is set to an OID later on.
        This design is more straightforward than caching indexes on setting the
        coord_df to an OID due to the possibility of an OID-to-OID change.
        """
        new_index = pd.DataFrame(index=new_index).index
        assert len(new_index) == len(self)

        self._index_cache = new_index
        if isinstance(self._coord_df_cache, ray.ObjectID):
            self._cached_index = True
        else:
            self._coord_df_cache.index = new_index

    index = property(_get_index, _set_index)

    def _get_index_cache(self):
        """Get the cached Index object, which may sometimes be an OID.

        This will ray.get the Index object out of the Ray store lazily, such
        that it is not grabbed until it is needed in the driver. This layer of
        abstraction is important for allowing this object to be instantiated
        with a remote Index object.

        Returns:
            The Index object in _index_cache.
        """
        if self._index_cache_validator is None:
            self._index_cache_validator = pd.RangeIndex(len(self))
        elif isinstance(self._index_cache_validator,
                        ray.ObjectID):
            self._index_cache_validator = ray.get(self._index_cache_validator)

        return self._index_cache_validator

    def _set_index_cache(self, new_index):
        """Sets the new index cache.

        Args:
            new_index: The Index to set the _index_cache to.
        """
        self._index_cache_validator = new_index

    # _index_cache_validator is an extra layer of abstraction to allow the
    # cache to accept ObjectIDs and ray.get them when needed.
    _index_cache = property(_get_index_cache, _set_index_cache)

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

    def get_global_indices(self, partition, index_within_partition_list):
        total = 0
        for i in range(partition):
            total += self._lengths[i]

        return [total + i for i in index_within_partition_list]

    def squeeze(self, partition, index_within_partition):
        """Prepare a single coordinate for removal by "squeezing" the
        subsequent coordinates "up" one index within that partition. To be used
        with "_IndexMetadata.drop" for when all the "squeezed" coordinates are
        dropped in batch. Note that this function doesn't actually mutate the
        coord_df.
        """
        self._coord_df = self._coord_df.copy()

        partition_mask = self._coord_df.partition == partition
        index_within_partition_mask = \
            self._coord_df.index_within_partition > index_within_partition
        self._coord_df.loc[partition_mask & index_within_partition_mask,
                           'index_within_partition'] -= 1

    def copy(self):
        # TODO: Investigate copy-on-write wrapper for metadata objects
        coord_df_copy = self._coord_df_cache
        if not isinstance(self._coord_df_cache, ray.ObjectID):
            coord_df_copy = self._coord_df_cache.copy()

        lengths_copy = self._lengths_cache
        if not isinstance(self._lengths_cache, ray.ObjectID):
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

        # Update first lengths to prevent possible length inconsistencies
        if isinstance(dropped, pd.DataFrame):
            drop_per_part = dropped.groupby(["partition"]).size()\
                    .reindex(index=pd.RangeIndex(len(self._lengths)),
                             fill_value=0)
        elif isinstance(dropped, pd.Series):
            drop_per_part = np.zeros_like(self._lengths)
            drop_per_part[dropped["partition"]] = 1
        else:
            raise AssertionError("Unrecognized result from `coords_of`")
        self._lengths = self._lengths - drop_per_part

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

    def get_partition(self, partition_id):
        """Return a view of coord_df where partition = partition_id
        """
        return self._coord_df[self._coord_df.partition == partition_id]

    def sorted_index(self):
        return (self._coord_df
                    .sort_values(['partition', 'index_within_partition'])
                    .index)
