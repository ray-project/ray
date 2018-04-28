""" Indexing class layout as follows:
    _Location_Indexer_Base is the parent class for Loc and iLoc Indexer
    Base class is responsible for triage.
    Child class is responsible for item lookup and write.
"""
import pandas as pd
import numpy as np
import ray
from warnings import warn

from pandas.api.types import (is_scalar, is_list_like, is_bool)
from pandas.core.dtypes.common import is_integer

from .utils import (_blocks_to_col, _get_nan_block_id, extractor)
from .index_metadata import _IndexMetadata


def is_slice(x):
    return isinstance(x, slice)


def is_2d(x):
    return is_list_like(x) or is_slice(x)


def is_tuple(x):
    return isinstance(x, tuple)


class _Location_Indexer_Base():
    """Base class for location indexer like loc and iloc
    """

    def __init__(self, ray_df):
        self.df = ray_df
        self.col_coord_df = ray_df._col_metadata._coord_df
        self.row_coord_df = ray_df._row_metadata._coord_df
        self.block_oids = ray_df._block_partitions

    def __getitem__(self, key):
        # Pathological Case: df.loc[1:,]
        if is_tuple(key) and len(key) == 1:
            key = key[0]

        # The one argument case is equivalent to full slice in 2nd dim.
        # iloc[3] == iloc[3, :] for a dataframe
        if not isinstance(key, tuple):
            return self._triage_getitem(key, slice(None))

        return self._triage_getitem(*key)

    def _triage_getitem(self, row_loc, col_loc):
        # iloc[1,2] returns a scaler
        if is_scalar(row_loc) and is_scalar(col_loc):
            return self._get_scaler(row_loc, col_loc)

        # iloc[1,:] returns a row series: row 1
        if is_scalar(row_loc) and is_2d(col_loc):
            return self._get_row_series(row_loc, col_loc)

        # iloc[:,1] returns a columns series: col 1
        if is_2d(row_loc) and is_scalar(col_loc):
            return self._get_col_series(row_loc, col_loc)

        if is_2d(row_loc) and is_2d(col_loc):
            return self._get_dataframe_view(row_loc, col_loc)

    def _get_scaler(self, row_loc, col_loc):
        pass

    def _get_col_series(self, row_loc, col_loc):
        pass

    def _get_row_series(self, row_loc, col_loc):
        pass

    def _get_dataframe_view(self, row_loc, col_loc):
        pass


def is_enlargement(locator, corrd_df):
    # Enlargement happens when you trying to locate using labels isn't in the
    # original index. In other words, enlargement == adding NaNs !
    if is_list_like(locator) and not is_slice(
            locator) and len(locator) > 0 and not is_bool(locator[0]):
        n_diff_elems = len(pd.Index(locator).difference(corrd_df.index))
        is_enlargement_boolean = n_diff_elems > 0
        if is_enlargement_boolean:
            warn(
                FutureWarning("""
Passing list-likes to .loc or [] with any missing label will raise
KeyError in the future, you can use .reindex() as an alternative.

See the documentation here:
http://pandas.pydata.org/pandas-docs/stable/indexing.html#deprecate-loc-reindex-listlike
            """))

        return is_enlargement_boolean
    return False


class _Loc_Indexer(_Location_Indexer_Base):
    """A indexer for ray_df.loc[] functionality"""

    def _get_scaler(self, row_loc, col_loc):
        row_loc_result = self.row_coord_df.loc[row_loc]
        if row_loc_result.ndim == 2:
            # We can facing duplicate index values
            return self._get_col_series(row_loc, col_loc)
        row_part, row_idx = row_loc_result
        col_part, col_idx = self.col_coord_df.loc[col_loc]
        chunk_oid = self.block_oids[row_part, col_part]
        result_oid = extractor.remote(chunk_oid, row_idx, col_idx)
        return ray.get(result_oid)

    # Series Helper
    def _get_series_blocks(self, row_loc, col_loc, primary_axis='row'):
        # primary_axis := axis where the locator is a scaler
        assert primary_axis in ['row', 'col']

        if primary_axis == 'row':
            row_loc = [row_loc]
        else:
            col_loc = [col_loc]

        # Have to do copy before we do groupby
        col_part_table = self.col_coord_df.loc[col_loc].copy()
        row_part_table = self.row_coord_df.loc[row_loc].copy()

        result_oids = []
        for row_part, row_partition_data in row_part_table.groupby(
                'partition'):
            for col_part, col_partition_data in col_part_table.groupby(
                    'partition'):
                block_oid = self.block_oids[row_part, col_part]
                row_idx = row_partition_data['index_within_partition']
                col_idx = col_partition_data['index_within_partition']

                if primary_axis == 'row':
                    row_idx = row_idx.iloc[0]
                else:
                    col_idx = col_idx.iloc[0]

                result_oid = extractor.remote(block_oid, row_idx, col_idx)
                result_oids.append(result_oid)

        if primary_axis == 'row':
            series_index = col_part_table.index
            series_name = row_loc[0]
        else:
            series_index = row_part_table.index
            series_name = col_loc[0]

        return result_oids, series_index, series_name

    def _post_process_series(self,
                             result_oids,
                             index,
                             name,
                             primary_axis='row'):
        series = ray.get(_blocks_to_col.remote(*result_oids))
        series.index = index
        series.name = name
        return series

    def _get_col_series(self, row_loc, col_loc):
        if is_enlargement(row_loc, self.row_coord_df):
            return self._get_col_series_enlarge(row_loc, col_loc)

        result_oids, index, name = self._get_series_blocks(
            row_loc, col_loc, primary_axis='col')
        col_series = self._post_process_series(
            result_oids, index, name, primary_axis='col')
        col_series = col_series.astype(self.df.dtypes[col_loc])

        return col_series

    def _compute_enlarge_labels(self, locator, axis='row'):
        assert axis in ['row', 'col']

        locator_as_index = pd.Index(locator)
        if axis == 'row':
            base_index = self.row_coord_df.index
        else:
            base_index = self.col_coord_df.index

        nan_labels = locator_as_index.difference(base_index)
        common_labels = locator_as_index.intersection(base_index)

        if len(common_labels) == 0:
            raise KeyError(
                'None of [{labels}] are in the [{base_index_name}]'.format(
                    labels=list(locator_as_index),
                    base_index_name=axis + ' index'))

        return nan_labels, common_labels

    def _get_col_series_enlarge(self, row_loc, col_loc):
        nan_labels, common_labels = self._compute_enlarge_labels(
            row_loc, axis='row')
        nan_series = pd.Series({name: np.NaN for name in nan_labels})

        col_series = self._get_col_series(common_labels, col_loc)
        col_series = pd.concat([col_series, nan_series])
        col_series = col_series.reindex(row_loc)

        return col_series

    def _get_row_series(self, row_loc, col_loc):
        if is_enlargement(col_loc, self.col_coord_df):
            return self._get_row_series_enlarge(row_loc, col_loc)

        result_oids, index, name = self._get_series_blocks(
            row_loc, col_loc, primary_axis='row')
        row_series = self._post_process_series(result_oids, index, name)

        return row_series

    def _get_row_series_enlarge(self, row_loc, col_loc):
        nan_labels, common_labels = self._compute_enlarge_labels(
            col_loc, axis='col')
        nan_series = pd.Series({name: np.NaN for name in nan_labels})

        row_series = self._get_row_series(row_loc, common_labels)
        row_series = pd.concat([row_series, nan_series])
        row_series = row_series.reindex(col_loc)

        return row_series

    def _enlarge_axis(self, locator, axis, row_meta, col_meta):
        """Add rows/columns to block partitions according to locator.

        Returns:
            metadata (_IndexMetadata)
        """
        assert axis in ['row', 'col']
        nan_labels, _ = self._compute_enlarge_labels(locator, axis=axis)
        n_nan_labels = len(nan_labels)
        blk_part_n_row, blk_part_n_col = self.block_oids.shape

        if axis == 'row':
            nan_blk_lens = col_meta._lengths
            nan_blks = np.array([[
                _get_nan_block_id(n_nan_labels, n_cols)
                for n_cols in nan_blk_lens
            ]])
        else:
            nan_blk_lens = row_meta._lengths
            nan_blks = np.array([[
                _get_nan_block_id(n_rows, n_nan_labels)
                for n_rows in nan_blk_lens
            ]]).T

        self.block_oids = np.concatenate(
            [self.block_oids, nan_blks], axis=0 if axis == 'row' else 1)

        nan_coord_df = pd.DataFrame(data=[{
            '': name,
            'partition': blk_part_n_row if axis == 'row' else blk_part_n_col,
            'index_within_partition': i
        } for name, i in zip(nan_labels, np.arange(n_nan_labels))]).set_index(
            '')

        coord_df = pd.concat([
            self.row_coord_df
            if axis == 'row' else self.col_coord_df, nan_coord_df
        ])
        coord_df = coord_df.loc[locator]

        lens = row_meta._lengths if axis == 'row' else col_meta._lengths
        lens = np.concatenate([lens, np.array([n_nan_labels])])

        row_metadata_view = _IndexMetadata(
            coord_df_oid=coord_df, lengths_oid=lens)

        return row_metadata_view

    def _get_dataframe_view(self, row_loc, col_loc):
        from .dataframe import DataFrame

        if is_enlargement(row_loc, self.row_coord_df):
            row_metadata_view = self._enlarge_axis(
                row_loc,
                axis='row',
                row_meta=self.df._row_metadata,
                col_meta=self.df._col_metadata)
        else:
            row_coord_df = self.row_coord_df.loc[row_loc]
            row_metadata_view = _IndexMetadata(
                coord_df_oid=row_coord_df,
                lengths_oid=self.df._row_metadata._lengths)

        if is_enlargement(col_loc, self.col_coord_df):
            col_metadata_view = self._enlarge_axis(
                col_loc,
                axis='col',
                row_meta=row_metadata_view,
                col_meta=self.df._col_metadata)
        else:
            col_coord_df = self.col_coord_df.loc[col_loc]
            col_metadata_view = _IndexMetadata(
                coord_df_oid=col_coord_df,
                lengths_oid=self.df._col_metadata._lengths)

        df_view = DataFrame(
            block_partitions=self.block_oids,
            row_metadata=row_metadata_view,
            col_metadata=col_metadata_view,
            index=row_metadata_view.index,
            columns=col_metadata_view.index,
            partial=True)

        return df_view


class _iLoc_Indexer(_Location_Indexer_Base):
    """A indexer for ray_df.iloc[] functionality"""

    # Note (simon)
    # This is implemented by pruning and slightly modifying code from Loc
    # Changes done:
    # - Find and Replace loc with iloc
    # - Make modificatino to series naming
    # - Prune enlargement related functions

    def _get_scaler(self, row_loc, col_loc):
        # Scaler access needs type checking.
        if not (is_integer(row_loc) and is_integer(col_loc)):
            raise ValueError("""
                             Location based indexing can only have
                             [integer, integer slice (START point is INCLUDED,
                             END point is EXCLUDED), listlike of integers,
                             boolean array] types""")
        row_loc_result = self.row_coord_df.iloc[row_loc]
        if row_loc_result.ndim == 2:
            # We can facing duplicate index values
            return self._get_col_series(row_loc, col_loc)
        row_part, row_idx = row_loc_result
        col_part, col_idx = self.col_coord_df.iloc[col_loc]
        chunk_oid = self.block_oids[row_part, col_part]
        result_oid = extractor.remote(chunk_oid, row_idx, col_idx)
        return ray.get(result_oid)

    # Series Helper
    def _get_series_blocks(self, row_loc, col_loc, primary_axis='row'):
        # primary_axis := axis where the locator is a scaler
        assert primary_axis in ['row', 'col']

        if primary_axis == 'row':
            row_loc = [row_loc]
        else:
            col_loc = [col_loc]

        # Must copy before groupby. Otherwise buffer isn't writable
        col_part_table = self.col_coord_df.iloc[col_loc].copy()
        row_part_table = self.row_coord_df.iloc[row_loc].copy()

        result_oids = []
        for row_part, row_partition_data in row_part_table.groupby(
                'partition'):
            for col_part, col_partition_data in col_part_table.groupby(
                    'partition'):
                block_oid = self.block_oids[row_part, col_part]
                row_idx = row_partition_data['index_within_partition']
                col_idx = col_partition_data['index_within_partition']

                if primary_axis == 'row':
                    row_idx = row_idx.iloc[0]
                else:
                    col_idx = col_idx.iloc[0]

                result_oid = extractor.remote(block_oid, row_idx, col_idx)
                result_oids.append(result_oid)

        if primary_axis == 'row':
            series_index = col_part_table.index
            series_name = row_part_table.index[0]
        else:
            series_index = row_part_table.index
            series_name = col_part_table.index[0]

        return result_oids, series_index, series_name

    def _post_process_series(self,
                             result_oids,
                             index,
                             name,
                             primary_axis='row'):
        series = ray.get(_blocks_to_col.remote(*result_oids))
        series.index = index
        series.name = name
        return series

    def _get_col_series(self, row_loc, col_loc):
        result_oids, index, name = self._get_series_blocks(
            row_loc, col_loc, primary_axis='col')
        col_series = self._post_process_series(
            result_oids, index, name, primary_axis='col')
        col_series = col_series.astype(self.df.dtypes[col_loc])

        return col_series

    def _get_row_series(self, row_loc, col_loc):
        result_oids, index, name = self._get_series_blocks(
            row_loc, col_loc, primary_axis='row')
        row_series = self._post_process_series(result_oids, index, name)

        return row_series

    def _get_dataframe_view(self, row_loc, col_loc):
        from .dataframe import DataFrame

        row_coord_df = self.row_coord_df.iloc[row_loc]
        row_metadata_view = _IndexMetadata(
            coord_df_oid=row_coord_df,
            lengths_oid=self.df._row_metadata._lengths)

        col_coord_df = self.col_coord_df.iloc[col_loc]
        col_metadata_view = _IndexMetadata(
            coord_df_oid=col_coord_df,
            lengths_oid=self.df._col_metadata._lengths)

        df_view = DataFrame(
            block_partitions=self.block_oids,
            row_metadata=row_metadata_view,
            col_metadata=col_metadata_view,
            index=row_metadata_view.index,
            columns=col_metadata_view.index,
            partial=True)

        return df_view
