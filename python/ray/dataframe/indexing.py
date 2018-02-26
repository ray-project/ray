import pandas as pd
import ray
from .dataframe import _deploy_func


def _row_tuples_to_dict(row_tuples):
    """A convenient helper function convert:
    [(partition, idx_in_partition)] to {partition: [idx_in_partition]}
    """
    d = {}
    for partition, idx_in_partition in row_tuples:
        if partition in d:
            d[partition].append(idx_in_partition)
        else:
            d[partition] = [idx_in_partition]
    return d


class _Location_Indexer_Base():
    """Base class for location indexer like loc and iloc
    This class abstract away commonly used method
    """

    def __init__(self, ray_df):
        self.df = ray_df

    def __getitem__(self, key):
        if not isinstance(key, tuple):
            # The one argument case is equivalent to full slice in 2nd dim.
            return self.locate_2d(key, slice(None))
        else:
            return self.locate_2d(*key)

    def _get_lookup_dict(self, ray_partition_idx):
        if ray_partition_idx.ndim == 1:  # Single row matched
            position = (ray_partition_idx['partition'],
                        ray_partition_idx['index_within_partition'])
            rows_to_lookup = [position]

        if ray_partition_idx.ndim == 2:  # Multiple rows matched
            rows_to_lookup = [(row['partition'], row['index_within_partition'])
                              for _, row in ray_partition_idx.iterrows()]

        lookup_dict = _row_tuples_to_dict(rows_to_lookup)
        return lookup_dict

    def locate_2d(self, row_label, col_label):
        pass

    def _map_partition(self, lookup_dict, col_lst, indexer='loc'):
        """Apply retrieval function to a lookup_dict
        in the form of {partition_id: [idx]}.

        Returns:
            retrieved_rows_remote: a list of object ids for pd_df
        """
        assert indexer in ['loc', 'iloc'], "indexer must be loc or iloc"

        if indexer == 'loc':
            def retrieve_func(
                df, idx_lst, col_label): return df.loc[idx_lst, col_label]
        elif indexer == 'iloc':
            def retrieve_func(
                df, idx_lst, col_idx): return df.iloc[idx_lst, col_idx]

        retrieved_rows_remote = []
        for partition, idx_to_lookup in lookup_dict.items():
            part_remote = _deploy_func.remote(
                retrieve_func, self.df._df[partition], idx_to_lookup, col_lst)
            retrieved_rows_remote.append(part_remote)
        return retrieved_rows_remote


class _Loc_Indexer(_Location_Indexer_Base):
    """A indexer for ray_df.loc[] functionality"""

    def locate_2d(self, row_label, col_label):
        index_loc = self.df._index.loc[row_label]
        lookup_dict = self._get_lookup_dict(index_loc)
        retrieved_rows_remote = self._map_partition(
            lookup_dict, col_label, indexer='loc')
        joined_df = pd.concat(ray.get(retrieved_rows_remote))

        if index_loc.ndim == 2:
            # The returned result need to be indexed series/df
            # Re-index is needed.
            joined_df.index = index_loc.index

        if isinstance(row_label, int) or isinstance(row_label, str):
            return joined_df.squeeze(axis=0)
        else:
            return joined_df


class _iLoc_Indexer(_Location_Indexer_Base):
    """A indexer for ray_df.iloc[] functionality"""

    def locate_2d(self, row_idx, col_idx):
        index_loc = self.df._index.iloc[row_idx]
        lookup_dict = self._get_lookup_dict(index_loc)
        retrieved_rows_remote = self._map_partition(
            lookup_dict, col_idx, indexer='iloc')
        joined_df = pd.concat(ray.get(retrieved_rows_remote))

        if index_loc.ndim == 2:
            # The returned result need to be indexed series/df
            # Re-index is needed.
            joined_df.index = index_loc.index

        if isinstance(row_idx, int) or isinstance(row_idx, str):
            return joined_df.squeeze(axis=0)
        else:
            return joined_df
