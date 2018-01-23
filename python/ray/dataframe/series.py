from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import numpy as np


def na_op():
    """Pandas uses a similar function to handle na values.
    """
    raise NotImplementedError("Not Yet implemented.")


class Series(object):

    @property
    def T(self):
        raise NotImplementedError("Not Yet implemented.")

    def __abs__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __add__(self, right, name='__add__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __and__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __array__(self, result=None):
        raise NotImplementedError("Not Yet implemented.")

    def __array_prepare__(self, result, context=None):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def __array_priority__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __array_wrap__(self, result, context=None):
        raise NotImplementedError("Not Yet implemented.")

    def __bool__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __bytes__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __class__(self, data=None, index=None, dtype=None, name=None,
                  copy=False, fastpath=False):
        raise NotImplementedError("Not Yet implemented.")

    def __contains__(self, key):
        raise NotImplementedError("Not Yet implemented.")

    def __copy__(self, deep=True):
        raise NotImplementedError("Not Yet implemented.")

    def __deepcopy__(self, memo=None):
        raise NotImplementedError("Not Yet implemented.")

    def __delitem__(self, key):
        raise NotImplementedError("Not Yet implemented.")

    def __dir__(self):
        return list(type(self).__dict__.keys())

    def __div__(self, right, name='__truediv__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __divmod__(self, right, name='__divmod__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def __doc__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __eq__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __finalize__(self, other, method=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def __float__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __floordiv__(self, right, name='__floordiv__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __ge__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __getitem__(self, key):
        raise NotImplementedError("Not Yet implemented.")

    def __getstate__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __gt__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __iadd__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __imul__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __int__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __invert__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __ipow__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __isub__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __iter__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __itruediv__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __le__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __len__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __long__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __lt__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __mod__(self, right, name='__mod__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __mul__(self, right, name='__mul__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __ne__(self, other, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def __neg__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __nonzero__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __or__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def __pow__(self, right, name='__pow__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __repr__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __round__(self, decimals=0):
        raise NotImplementedError("Not Yet implemented.")

    def __setitem__(self, key, value):
        raise NotImplementedError("Not Yet implemented.")

    def __setstate__(self, state):
        raise NotImplementedError("Not Yet implemented.")

    def __sizeof__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __str__(self):
        raise NotImplementedError("Not Yet implemented.")

    def __sub__(self, right, name='__sub__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __truediv__(self, right, name='__truediv__', na_op=na_op):
        raise NotImplementedError("Not Yet implemented.")

    def __xor__(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def abs(self):
        raise NotImplementedError("Not Yet implemented.")

    def add(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def add_prefix(self, prefix):
        raise NotImplementedError("Not Yet implemented.")

    def add_suffix(self, suffix):
        raise NotImplementedError("Not Yet implemented.")

    def agg(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def aggregate(self, func, axis=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def align(self, other, join='outer', axis=None, level=None, copy=True,
              fill_value=None, method=None, limit=None, fill_axis=0,
              broadcast_axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def all(self, axis=None, bool_only=None, skipna=None, level=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def any(self, axis=None, bool_only=None, skipna=None, level=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def append(self, to_append, ignore_index=False, verify_integrity=False):
        raise NotImplementedError("Not Yet implemented.")

    def apply(self, func, convert_dtype=True, args=(), **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def argmax(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def argmin(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def argsort(self, axis=0, kind='quicksort', order=None):
        raise NotImplementedError("Not Yet implemented.")

    def as_blocks(self, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def as_matrix(self, columns=None):
        raise NotImplementedError("Not Yet implemented.")

    def asfreq(self, freq, method=None, how=None, normalize=False,
               fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def asof(self, where, subset=None):
        raise NotImplementedError("Not Yet implemented.")

    def astype(self, dtype, copy=True, errors='raise', **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def at(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def at_time(self, time, asof=False):
        raise NotImplementedError("Not Yet implemented.")

    def autocorr(self, lag=1):
        raise NotImplementedError("Not Yet implemented.")

    def between(self, left, right, inclusive=True):
        raise NotImplementedError("Not Yet implemented.")

    def between_time(self, start_time, end_time, include_start=True,
                     include_end=True):
        raise NotImplementedError("Not Yet implemented.")

    def bfill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError("Not Yet implemented.")

    def bool(self):
        raise NotImplementedError("Not Yet implemented.")

    def clip(self, lower=None, upper=None, axis=None, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def clip_lower(self, threshold, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def clip_upper(self, threshold, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def combine(self, other, func, fill_value=np.nan):
        raise NotImplementedError("Not Yet implemented.")

    def combine_first(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def compound(self, axis=None, skipna=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def compress(self, condition, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def consolidate(self, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def convert_objects(self, convert_dates=True, convert_numeric=False,
                        convert_timedeltas=True, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def copy(self, deep=True):
        raise NotImplementedError("Not Yet implemented.")

    def corr(self, other, method='pearson', min_periods=None):
        raise NotImplementedError("Not Yet implemented.")

    def count(self, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def cov(self, other, min_periods=None):
        raise NotImplementedError("Not Yet implemented.")

    def cummax(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cummin(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cumprod(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def cumsum(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def describe(self, percentiles=None, include=None, exclude=None):
        raise NotImplementedError("Not Yet implemented.")

    def diff(self, periods=1):
        raise NotImplementedError("Not Yet implemented.")

    def div(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def divide(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def dot(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def drop(self, labels, axis=0, level=None, inplace=False, errors='raise'):
        raise NotImplementedError("Not Yet implemented.")

    def drop_duplicates(self, keep='first', inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def dropna(self, axis=0, inplace=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def duplicated(self, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def eq(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def equals(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def ewm(self, com=None, span=None, halflife=None, alpha=None,
            min_periods=0, freq=None, adjust=True, ignore_na=False, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def expanding(self, min_periods=1, freq=None, center=False, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def factorize(self, sort=False, na_sentinel=-1):
        raise NotImplementedError("Not Yet implemented.")

    def ffill(self, axis=None, inplace=False, limit=None, downcast=None):
        raise NotImplementedError("Not Yet implemented.")

    def fillna(self, value=None, method=None, axis=None, inplace=False,
               limit=None, downcast=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def filter(self, items=None, like=None, regex=None, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def first(self, offset):
        raise NotImplementedError("Not Yet implemented.")

    def first_valid_index(self):
        raise NotImplementedError("Not Yet implemented.")

    def floordiv(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def from_array(self, arr, index=None, name=None, dtype=None, copy=False,
                   fastpath=False):
        raise NotImplementedError("Not Yet implemented.")

    def from_csv(self, path, sep=',', parse_dates=True, header=None,
                 index_col=0, encoding=None, infer_datetime_format=False):
        raise NotImplementedError("Not Yet implemented.")

    def ge(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def get(self, key, default=None):
        raise NotImplementedError("Not Yet implemented.")

    def get_dtype_counts(self):
        raise NotImplementedError("Not Yet implemented.")

    def get_ftype_counts(self):
        raise NotImplementedError("Not Yet implemented.")

    def get_value(self, label, takeable=False):
        raise NotImplementedError("Not Yet implemented.")

    def get_values(self):
        raise NotImplementedError("Not Yet implemented.")

    def groupby(self, by=None, axis=0, level=None, as_index=True, sort=True,
                group_keys=True, squeeze=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def gt(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def head(self, n=5):
        raise NotImplementedError("Not Yet implemented.")

    def hist(self, by=None, ax=None, grid=True, xlabelsize=None, xrot=None,
             ylabelsize=None, yrot=None, figsize=None, bins=10, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def iat(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def idxmax(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def idxmin(self, axis=None, skipna=True, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def iloc(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def interpolate(self, method='linear', axis=0, limit=None, inplace=False,
                    limit_direction='forward', downcast=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def isin(self, values):
        raise NotImplementedError("Not Yet implemented.")

    def isnull(self):
        raise NotImplementedError("Not Yet implemented.")

    def item(self):
        raise NotImplementedError("Not Yet implemented.")

    def items(self):
        raise NotImplementedError("Not Yet implemented.")

    def iteritems(self):
        raise NotImplementedError("Not Yet implemented.")

    def ix(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def keys(self):
        raise NotImplementedError("Not Yet implemented.")

    def kurt(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def kurtosis(self, axis=None, skipna=None, level=None, numeric_only=None,
                 **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def last(self, offset):
        raise NotImplementedError("Not Yet implemented.")

    def last_valid_index(self):
        raise NotImplementedError("Not Yet implemented.")

    def le(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def loc(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def lt(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def mad(self, axis=None, skipna=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def map(self, arg, na_action=None):
        raise NotImplementedError("Not Yet implemented.")

    def mask(self, cond, other=np.nan, inplace=False, axis=None, level=None,
             try_cast=False, raise_on_error=True):
        raise NotImplementedError("Not Yet implemented.")

    def max(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def mean(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def median(self, axis=None, skipna=None, level=None, numeric_only=None,
               **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def memory_usage(self, index=True, deep=False):
        raise NotImplementedError("Not Yet implemented.")

    def min(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def mod(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def mode(self):
        raise NotImplementedError("Not Yet implemented.")

    def mul(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def multiply(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def ne(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def nlargest(self, n=5, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def nonzero(self):
        raise NotImplementedError("Not Yet implemented.")

    def notnull(self):
        raise NotImplementedError("Not Yet implemented.")

    def nsmallest(self, n=5, keep='first'):
        raise NotImplementedError("Not Yet implemented.")

    def nunique(self, dropna=True):
        raise NotImplementedError("Not Yet implemented.")

    def pct_change(self, periods=1, fill_method='pad', limit=None, freq=None,
                   **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def pipe(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def plot(self, kind='line', ax=None, figsize=None, use_index=True,
             title=None, grid=None, legend=False, style=None, logx=False,
             logy=False, loglog=False, xticks=None, yticks=None, xlim=None,
             ylim=None, rot=None, fontsize=None, colormap=None, table=False,
             yerr=None, xerr=None, label=None, secondary_y=False, **kwds):
        raise NotImplementedError("Not Yet implemented.")

    def pop(self, item):
        raise NotImplementedError("Not Yet implemented.")

    def pow(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def prod(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def product(self, axis=None, skipna=None, level=None, numeric_only=None,
                **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def ptp(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def put(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def quantile(self, q=0.5, interpolation='linear'):
        raise NotImplementedError("Not Yet implemented.")

    def radd(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rank(self, axis=0, method='average', numeric_only=None,
             na_option='keep', ascending=True, pct=False):
        raise NotImplementedError("Not Yet implemented.")

    def ravel(self, order='C'):
        raise NotImplementedError("Not Yet implemented.")

    def rdiv(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def reindex(self, index=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def reindex_axis(self, labels, axis=0, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def reindex_like(self, other, method=None, copy=True, limit=None,
                     tolerance=None):
        raise NotImplementedError("Not Yet implemented.")

    def rename(self, index=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def rename_axis(self, mapper, axis=0, copy=True, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def reorder_levels(self, order):
        raise NotImplementedError("Not Yet implemented.")

    def repeat(self, repeats, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def replace(self, to_replace=None, value=None, inplace=False, limit=None,
                regex=False, method='pad', axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def resample(self, rule, how=None, axis=0, fill_method=None, closed=None,
                 label=None, convention='start', kind=None, loffset=None,
                 limit=None, base=0, on=None, level=None):
        raise NotImplementedError("Not Yet implemented.")

    def reset_index(self, level=None, drop=False, name=None, inplace=False):
        raise NotImplementedError("Not Yet implemented.")

    def reshape(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def rfloordiv(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rmod(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rmul(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rolling(self, window, min_periods=None, freq=None, center=False,
                win_type=None, on=None, axis=0, closed=None):
        raise NotImplementedError("Not Yet implemented.")

    def round(self, decimals=0, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def rpow(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rsub(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def rtruediv(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def sample(self, n=None, frac=None, replace=False, weights=None,
               random_state=None, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def searchsorted(self, value, side='left', sorter=None):
        raise NotImplementedError("Not Yet implemented.")

    def select(self, crit, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def sem(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def set_axis(self, axis, labels):
        raise NotImplementedError("Not Yet implemented.")

    def set_value(self, label, value, takeable=False):
        raise NotImplementedError("Not Yet implemented.")

    def shift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def skew(self, axis=None, skipna=None, level=None, numeric_only=None,
             **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def slice_shift(self, periods=1, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def sort_index(self, axis=0, level=None, ascending=True, inplace=False,
                   kind='quicksort', na_position='last', sort_remaining=True):
        raise NotImplementedError("Not Yet implemented.")

    def sort_values(self, axis=0, ascending=True, inplace=False,
                    kind='quicksort', na_position='last'):
        raise NotImplementedError("Not Yet implemented.")

    def sortlevel(self, level=0, ascending=True, sort_remaining=True):
        raise NotImplementedError("Not Yet implemented.")

    def squeeze(self, axis=None):
        raise NotImplementedError("Not Yet implemented.")

    def std(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def sub(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def subtract(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def sum(self, axis=None, skipna=None, level=None, numeric_only=None,
            **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def swapaxes(self, axis1, axis2, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def swaplevel(self, i=-2, j=-1, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def tail(self, n=5):
        raise NotImplementedError("Not Yet implemented.")

    def take(self, indices, axis=0, convert=True, is_copy=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_clipboard(self, excel=None, sep=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_csv(self, path=None, index=True, sep=',', na_rep='',
               float_format=None, header=False, index_label=None, mode='w',
               encoding=None, date_format=None, decimal='.'):
        raise NotImplementedError("Not Yet implemented.")

    def to_dense(self):
        raise NotImplementedError("Not Yet implemented.")

    def to_dict(self):
        raise NotImplementedError("Not Yet implemented.")

    def to_excel(self, excel_writer, sheet_name='Sheet1', na_rep='',
                 float_format=None, columns=None, header=True, index=True,
                 index_label=None, startrow=0, startcol=0, engine=None,
                 merge_cells=True, encoding=None, inf_rep='inf',
                 verbose=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_frame(self, name=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_hdf(self, path_or_buf, key, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_json(self, path_or_buf=None, orient=None, date_format=None,
                double_precision=10, force_ascii=True, date_unit='ms',
                default_handler=None, lines=False):
        raise NotImplementedError("Not Yet implemented.")

    def to_latex(self, buf=None, columns=None, col_space=None, header=True,
                 index=True, na_rep='NaN', formatters=None, float_format=None,
                 sparsify=None, index_names=True, bold_rows=False,
                 column_format=None, longtable=None, escape=None,
                 encoding=None, decimal='.', multicolumn=None,
                 multicolumn_format=None, multirow=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_msgpack(self, path_or_buf=None, encoding='utf-8', **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def to_period(self, freq=None, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_pickle(self, path, compression='infer'):
        raise NotImplementedError("Not Yet implemented.")

    def to_sparse(self, kind='block', fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_sql(self, name, con, flavor=None, schema=None, if_exists='fail',
               index=True, index_label=None, chunksize=None, dtype=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_string(self, buf=None, na_rep='NaN', float_format=None,
                  header=True, index=True, length=False, dtype=False,
                  name=False, max_rows=None):
        raise NotImplementedError("Not Yet implemented.")

    def to_timestamp(self, freq=None, how='start', copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def to_xarray(self):
        raise NotImplementedError("Not Yet implemented.")

    def tolist(self):
        raise NotImplementedError("Not Yet implemented.")

    def transform(self, func, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def transpose(self, *args, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def truediv(self, other, level=None, fill_value=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def truncate(self, before=None, after=None, axis=None, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def tshift(self, periods=1, freq=None, axis=0):
        raise NotImplementedError("Not Yet implemented.")

    def tz_convert(self, tz, axis=0, level=None, copy=True):
        raise NotImplementedError("Not Yet implemented.")

    def tz_localize(self, tz, axis=0, level=None, copy=True,
                    ambiguous='raise'):
        raise NotImplementedError("Not Yet implemented.")

    def unique(self):
        raise NotImplementedError("Not Yet implemented.")

    def unstack(self, level=-1, fill_value=None):
        raise NotImplementedError("Not Yet implemented.")

    def update(self, other):
        raise NotImplementedError("Not Yet implemented.")

    def valid(self, inplace=False, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def value_counts(self, normalize=False, sort=True, ascending=False,
                     bins=None, dropna=True):
        raise NotImplementedError("Not Yet implemented.")

    def var(self, axis=None, skipna=None, level=None, ddof=1,
            numeric_only=None, **kwargs):
        raise NotImplementedError("Not Yet implemented.")

    def view(self, dtype=None):
        raise NotImplementedError("Not Yet implemented.")

    def where(self, cond, other=np.nan, inplace=False, axis=None, level=None,
              try_cast=False, raise_on_error=True):
        raise NotImplementedError("Not Yet implemented.")

    def xs(key, axis=0, level=None, drop_level=True):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def asobject(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def axes(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def base(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def blocks(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def data(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def dtype(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def dtypes(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def empty(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def flags(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def ftype(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def ftypes(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def hasnans(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def imag(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def index(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_copy(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_monotonic(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_monotonic_decreasing(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_monotonic_increasing(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def is_unique(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def itemsize(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def name(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def nbytes(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def ndim(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def real(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def shape(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def size(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def strides(self):
        raise NotImplementedError("Not Yet implemented.")

    @property
    def values(self):
        raise NotImplementedError("Not Yet implemented.")
