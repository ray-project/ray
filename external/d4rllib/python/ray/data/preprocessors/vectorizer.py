from collections import Counter
from typing import Callable, List, Optional

import pandas as pd

from ray.data import Dataset
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors.utils import simple_hash, simple_split_tokenizer
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class HashingVectorizer(Preprocessor):
    """Count the frequency of tokens using the
    `hashing trick <https://en.wikipedia.org/wiki/Feature_hashing>`_.

    This preprocessors creates a list column for each input column. For each row,
    the list contains the frequency counts of tokens (for CountVectorizer) or hash values
    (for HashingVectorizer). For HashingVectorizer, the list will have length
    ``num_features``. If ``num_features`` is large enough relative to the size of your
    vocabulary, then each index approximately corresponds to the frequency of a unique
    token.

    :class:`HashingVectorizer` is memory efficient and quick to pickle. However, given a
    transformed column, you can't know which tokens correspond to it. This might make it
    hard to determine which tokens are important to your model.

    .. note::

        This preprocessor transforms each input column to a
        `document-term matrix <https://en.wikipedia.org/wiki/Document-term_matrix>`_.

        A document-term matrix is a table that describes the frequency of tokens in a
        collection of documents. For example, the strings `"I like Python"` and `"I
        dislike Python"` might have the document-term matrix below:

        .. code-block::

                    corpus_I  corpus_Python  corpus_dislike  corpus_like
                0         1              1               1            0
                1         1              1               0            1

        To generate the matrix, you typically map each token to a unique index. For
        example:

        .. code-block::

                        token  index
                0        I      0
                1   Python      1
                2  dislike      2
                3     like      3

        The problem with this approach is that memory use scales linearly with the size
        of your vocabulary. :class:`HashingVectorizer` circumvents this problem by
        computing indices with a hash function:
        :math:`\\texttt{index} = hash(\\texttt{token})`.

    .. warning::
        Sparse matrices aren't currently supported. If you use a large ``num_features``,
        this preprocessor might behave poorly.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import HashingVectorizer
        >>>
        >>> df = pd.DataFrame({
        ...     "corpus": [
        ...         "Jimmy likes volleyball",
        ...         "Bob likes volleyball too",
        ...         "Bob also likes fruit jerky"
        ...     ]
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> vectorizer = HashingVectorizer(["corpus"], num_features=8)
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
                             corpus
        0  [1, 0, 1, 0, 0, 0, 0, 1]
        1  [1, 0, 1, 0, 0, 0, 1, 1]
        2  [0, 0, 1, 1, 0, 2, 1, 0]

        :class:`HashingVectorizer` can also be used in append mode by providing the
        name of the output_columns that should hold the encoded values.

        >>> vectorizer = HashingVectorizer(["corpus"], num_features=8, output_columns=["corpus_hashed"])
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
                               corpus             corpus_hashed
        0      Jimmy likes volleyball  [1, 0, 1, 0, 0, 0, 0, 1]
        1    Bob likes volleyball too  [1, 0, 1, 0, 0, 0, 1, 1]
        2  Bob also likes fruit jerky  [0, 0, 1, 1, 0, 2, 1, 0]

    Args:
        columns: The columns to separately tokenize and count.
        num_features: The number of features used to represent the vocabulary. You
            should choose a value large enough to prevent hash collisions between
            distinct tokens.
        tokenization_fn: The function used to generate tokens. This function
            should accept a string as input and return a list of tokens as
            output. If unspecified, the tokenizer uses a function equivalent to
            ``lambda s: s.split(" ")``.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.

    .. seealso::

        :class:`CountVectorizer`
            Another method for counting token frequencies. Unlike :class:`HashingVectorizer`,
            :class:`CountVectorizer` creates a feature for each unique token. This
            enables you to compute the inverse transformation.

        :class:`FeatureHasher`
            This preprocessor is similar to :class:`HashingVectorizer`, except it expects
            a table describing token frequencies. In contrast,
            :class:`FeatureHasher` expects a column containing documents.
    """  # noqa: E501

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        num_features: int,
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
        *,
        output_columns: Optional[List[str]] = None,
    ):
        self.columns = columns
        self.num_features = num_features
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    def _transform_pandas(self, df: pd.DataFrame):
        def hash_count(tokens: List[str]) -> Counter:
            hashed_tokens = [simple_hash(token, self.num_features) for token in tokens]
            return Counter(hashed_tokens)

        for col, output_col in zip(self.columns, self.output_columns):
            tokenized = df[col].map(self.tokenization_fn)
            hashed = tokenized.map(hash_count)
            # Create a list to store the hash columns
            hash_columns = []
            for i in range(self.num_features):
                series = hashed.map(lambda counts: counts[i])
                series.name = f"hash_{i}"
                hash_columns.append(series)
            # Concatenate all hash columns into a single list column
            df[output_col] = pd.concat(hash_columns, axis=1).values.tolist()

        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"num_features={self.num_features!r}, tokenization_fn={fn_name}, "
            f"output_columns={self.output_columns!r})"
        )


@PublicAPI(stability="alpha")
class CountVectorizer(Preprocessor):
    """Count the frequency of tokens in a column of strings.

    :class:`CountVectorizer` operates on columns that contain strings. For example:

    .. code-block::

                        corpus
        0    I dislike Python
        1       I like Python

    This preprocessor creates a list column for each input column. Each list contains
    the frequency counts of tokens in order of their first appearance. For example:

    .. code-block::

                    corpus
        0    [1, 1, 1, 0]  # Counts for [I, dislike, Python, like]
        1    [1, 0, 1, 1]  # Counts for [I, dislike, Python, like]

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> from ray.data.preprocessors import CountVectorizer
        >>>
        >>> df = pd.DataFrame({
        ...     "corpus": [
        ...         "Jimmy likes volleyball",
        ...         "Bob likes volleyball too",
        ...         "Bob also likes fruit jerky"
        ...     ]
        ... })
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP
        >>>
        >>> vectorizer = CountVectorizer(["corpus"])
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
                             corpus
        0  [1, 0, 1, 1, 0, 0, 0, 0]
        1  [1, 1, 1, 0, 0, 0, 0, 1]
        2  [1, 1, 0, 0, 1, 1, 1, 0]

        You can limit the number of tokens in the vocabulary with ``max_features``.

        >>> vectorizer = CountVectorizer(["corpus"], max_features=3)
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
              corpus
        0  [1, 0, 1]
        1  [1, 1, 1]
        2  [1, 1, 0]

        :class:`CountVectorizer` can also be used in append mode by providing the
        name of the output_columns that should hold the encoded values.

        >>> vectorizer = CountVectorizer(["corpus"], output_columns=["corpus_counts"])
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
                               corpus             corpus_counts
        0      Jimmy likes volleyball  [1, 0, 1, 1, 0, 0, 0, 0]
        1    Bob likes volleyball too  [1, 1, 1, 0, 0, 0, 0, 1]
        2  Bob also likes fruit jerky  [1, 1, 0, 0, 1, 1, 1, 0]

    Args:
        columns: The columns to separately tokenize and count.
        tokenization_fn: The function used to generate tokens. This function
            should accept a string as input and return a list of tokens as
            output. If unspecified, the tokenizer uses a function equivalent to
            ``lambda s: s.split(" ")``.
        max_features: The maximum number of tokens to encode in the transformed
            dataset. If specified, only the most frequent tokens are encoded.
        output_columns: The names of the transformed columns. If None, the transformed
            columns will be the same as the input columns. If not None, the length of
            ``output_columns`` must match the length of ``columns``, othwerwise an error
            will be raised.
    """  # noqa: E501

    def __init__(
        self,
        columns: List[str],
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
        max_features: Optional[int] = None,
        *,
        output_columns: Optional[List[str]] = None,
    ):
        self.columns = columns
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer
        self.max_features = max_features
        self.output_columns = Preprocessor._derive_and_validate_output_columns(
            columns, output_columns
        )

    def _fit(self, dataset: Dataset) -> Preprocessor:
        def get_pd_value_counts(df: pd.DataFrame) -> List[Counter]:
            def get_token_counts(col):
                token_series = df[col].apply(self.tokenization_fn)
                tokens = token_series.sum()
                return Counter(tokens)

            return {col: [get_token_counts(col)] for col in self.columns}

        value_counts = dataset.map_batches(get_pd_value_counts, batch_format="pandas")
        total_counts = {col: Counter() for col in self.columns}
        for batch in value_counts.iter_batches(batch_size=None):
            for col, counters in batch.items():
                for counter in counters:
                    total_counts[col].update(counter)

        def most_common(counter: Counter, n: int):
            return Counter(dict(counter.most_common(n)))

        top_counts = [
            most_common(counter, self.max_features) for counter in total_counts.values()
        ]

        self.stats_ = {
            f"token_counts({col})": counts
            for (col, counts) in zip(self.columns, top_counts)
        }

        return self

    def _transform_pandas(self, df: pd.DataFrame):
        result_columns = []
        for col, output_col in zip(self.columns, self.output_columns):
            token_counts = self.stats_[f"token_counts({col})"]
            sorted_tokens = [token for (token, count) in token_counts.most_common()]
            tokenized = df[col].map(self.tokenization_fn).map(Counter)

            # Create a list to store token frequencies
            token_columns = []
            for token in sorted_tokens:
                series = tokenized.map(lambda val: val[token])
                series.name = token
                token_columns.append(series)

            # Concatenate all token columns into a single list column
            if token_columns:
                df[output_col] = pd.concat(token_columns, axis=1).values.tolist()
            else:
                df[output_col] = [[]] * len(df)
            result_columns.append(output_col)

        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"tokenization_fn={fn_name}, max_features={self.max_features!r}, "
            f"output_columns={self.output_columns!r})"
        )
