from __future__ import division

import pandas.util.testing as tm
import ray.dataframe as rdf

from pandas.util.testing import (
        _skip_if_no_lzma,
        assert_index_equal,
        assert_produces_warning,
        assert_raises_regex,
        assert_series_equal, # TODO: replace this once series are implemented
        getSeriesData, # TODO: replace this once series are implemented
        getTimeSeriesData, # TODO: replace this once series are implemented
        ensure_clean,
        raise_assert_detail)

from .. import get_npartitions, utils


def assert_almost_equal(left, right, check_exact=False,
                        check_dtype='equiv', check_less_precise=False,
                        **kwargs):
    if isinstance(left, rdf.DataFrame):
        return assert_frame_equal(left, right, check_exact=check_exact,
                                  check_dtype=check_dtype,
                                  check_less_precise=check_less_precise,
                                  **kwargs)
    return tm.assert_almost_equal(left, right, check_exact=check_exact,
                                  check_dtype=check_dtype,
                                  check_less_precise=check_less_precise,
                                  **kwargs)


def assert_frame_equal(left, right, check_dtype=True,
                       check_index_type='equiv',
                       check_column_type='equiv',
                       check_frame_type=True,
                       check_less_precise=False,
                       check_names=True,
                       by_blocks=False,
                       check_exact=False,
                       check_datetimelike_compat=False,
                       check_categorical=True,
                       check_like=False,
                       obj='DataFrame'):

    # instance validation
    tm._check_isinstance(left, right, rdf.DataFrame)

    if check_frame_type:
        # ToDo: There are some tests using rhs is SparseDataFrame
        # lhs is DataFrame. Should use assert_class_equal in future
        assert isinstance(left, type(right))
        # assert_class_equal(left, right, obj=obj)

    # shape comparison
    if left.shape != right.shape:
        raise_assert_detail(obj,
                            'DataFrame shape mismatch',
                            '{shape!r}'.format(shape=left.shape),
                            '{shape!r}'.format(shape=right.shape))

    if check_like:
        left, right = left.reindex_like(right), right

    # index comparison
    assert_index_equal(left.index, right.index, exact=check_index_type,
                       check_names=check_names,
                       check_less_precise=check_less_precise,
                       check_exact=check_exact,
                       check_categorical=check_categorical,
                       obj='{obj}.index'.format(obj=obj))

    # column comparison
    assert_index_equal(left.columns, right.columns, exact=check_column_type,
                       check_names=check_names,
                       check_less_precise=check_less_precise,
                       check_exact=check_exact,
                       check_categorical=check_categorical,
                       obj='{obj}.columns'.format(obj=obj))

    # compare by blocks
    if by_blocks:
        raise NotImplementedError("Comparing by blocks is not supported yet")

        rblocks = right._to_dict_of_blocks()
        lblocks = left._to_dict_of_blocks()
        for dtype in list(set(list(lblocks.keys()) + list(rblocks.keys()))):
            assert dtype in lblocks
            assert dtype in rblocks
            assert_frame_equal(lblocks[dtype], rblocks[dtype],
                               check_dtype=check_dtype, obj='DataFrame.blocks')

    # compare by rows
    else:
        assert all(left.columns == right.columns)
        for (_, left_row), (__, right_row) in zip(left.iterrows(),
                                                  right.iterrows()):
            assert_series_equal(left_row, right_row)


def makeCustomDataframe(nrows, ncols, c_idx_names=True, r_idx_names=True,
                        c_idx_nlevels=1, r_idx_nlevels=1, data_gen_f=None,
                        c_ndupe_l=None, r_ndupe_l=None, dtype=None,
                        c_idx_type=None, r_idx_type=None):
    pd_df = tm.makeCustomDataframe(nrows, ncols, c_idx_names=c_idx_names,
                                   r_idx_names=r_idx_names,
                                   c_idx_nlevels=c_idx_nlevels,
                                   r_idx_nlevels=r_idx_nlevels,
                                   data_gen_f=data_gen_f, c_ndupe_l=c_ndupe_l,
                                   r_ndupe_l=r_ndupe_l, dtype=dtype,
                                   c_idx_type=c_idx_type,
                                   r_idx_type=r_idx_type)
    return utils.from_pandas(pd_df, get_npartitions())
