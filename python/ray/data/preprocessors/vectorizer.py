from collections import Counter
from typing import List, Callable, Optional

import pandas as pd

from ray.data import Datastream
from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors.utils import simple_split_tokenizer, simple_hash
from ray.util.annotations import PublicAPI


@PublicAPI(stability="alpha")
class HashingVectorizer(Preprocessor):
    """Count the frequency of tokens using the
    `hashing trick <https://en.wikipedia.org/wiki/Feature_hashing>`_.

    This preprocessors creates ``num_features`` columns named like
    ``hash_{column_name}_{index}``. If ``num_features`` is large enough relative to
    the size of your vocabulary, then each column approximately corresponds to the
    frequency of a unique token.

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
           hash_corpus_0  hash_corpus_1  hash_corpus_2  hash_corpus_3  hash_corpus_4  hash_corpus_5  hash_corpus_6  hash_corpus_7
        0              1              0              1              0              0              0              0              1
        1              1              0              1              0              0              0              1              1
        2              0              0              1              1              0              2              1              0

    Args:
        columns: The columns to separately tokenize and count.
        num_features: The number of features used to represent the vocabulary. You
            should choose a value large enough to prevent hash collisions between
            distinct tokens.
        tokenization_fn: The function used to generate tokens. This function
            should accept a string as input and return a list of tokens as
            output. If unspecified, the tokenizer uses a function equivalent to
            ``lambda s: s.split(" ")``.

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
    ):
        self.columns = columns
        # TODO(matt): Set default number of features.
        # This likely requires sparse matrix support to avoid explosion of columns.
        self.num_features = num_features
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer

    def _transform_pandas(self, df: pd.DataFrame):
        # TODO(matt): Use sparse matrix for efficiency.

        def hash_count(tokens: List[str]) -> Counter:
            hashed_tokens = [simple_hash(token, self.num_features) for token in tokens]
            return Counter(hashed_tokens)

        for col in self.columns:
            tokenized = df[col].map(self.tokenization_fn)
            hashed = tokenized.map(hash_count)
            for i in range(self.num_features):
                df[f"hash_{col}_{i}"] = hashed.map(lambda counts: counts[i])

        # Drop original columns.
        df.drop(columns=self.columns, inplace=True)
        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"num_features={self.num_features!r}, tokenization_fn={fn_name})"
        )


@PublicAPI(stability="alpha")
class CountVectorizer(Preprocessor):
    """Count the frequency of tokens in a column of strings.

    :class:`CountVectorizer` operates on columns that contain strings. For example:

    .. code-block::

                        corpus
        0    I dislike Python
        1       I like Python

    This preprocessors creates a column named like ``{column}_{token}`` for each
    unique token. These columns represent the frequency of token ``{token}`` in
    column ``{column}``. For example:

    .. code-block::

            corpus_I  corpus_Python  corpus_dislike  corpus_like
        0         1              1               1            0
        1         1              1               0            1

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
           corpus_likes  corpus_volleyball  corpus_Bob  corpus_Jimmy  corpus_too  corpus_also  corpus_fruit  corpus_jerky
        0             1                  1           0             1           0            0             0             0
        1             1                  1           1             0           1            0             0             0
        2             1                  0           1             0           0            1             1             1

        You can limit the number of tokens in the vocabulary with ``max_features``.

        >>> vectorizer = CountVectorizer(["corpus"], max_features=3)
        >>> vectorizer.fit_transform(ds).to_pandas()  # doctest: +SKIP
           corpus_likes  corpus_volleyball  corpus_Bob
        0             1                  1           0
        1             1                  1           1
        2             1                  0           1

    Args:
        columns: The columns to separately tokenize and count.
        tokenization_fn: The function used to generate tokens. This function
            should accept a string as input and return a list of tokens as
            output. If unspecified, the tokenizer uses a function equivalent to
            ``lambda s: s.split(" ")``.
        max_features: The maximum number of tokens to encode in the transformed
            datastream. If specified, only the most frequent tokens are encoded.

    """  # noqa: E501

    def __init__(
        self,
        columns: List[str],
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
        max_features: Optional[int] = None,
    ):
        # TODO(matt): Add fit_transform to avoid recomputing tokenization step.
        self.columns = columns
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer
        self.max_features = max_features

    def _fit(self, datastream: Datastream) -> Preprocessor:
        def get_pd_value_counts(df: pd.DataFrame) -> List[Counter]:
            def get_token_counts(col):
                token_series = df[col].apply(self.tokenization_fn)
                tokens = token_series.sum()
                return Counter(tokens)

            return {col: [get_token_counts(col)] for col in self.columns}

        value_counts = datastream.map_batches(
            get_pd_value_counts, batch_format="pandas"
        )
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

        to_concat = []
        for col in self.columns:
            token_counts = self.stats_[f"token_counts({col})"]
            sorted_tokens = [token for (token, count) in token_counts.most_common()]
            tokenized = df[col].map(self.tokenization_fn).map(Counter)
            for token in sorted_tokens:
                series = tokenized.map(lambda val: val[token])
                series.name = f"{col}_{token}"
                to_concat.append(series)

        df = pd.concat(to_concat, axis=1)
        return df

    def __repr__(self):
        fn_name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"tokenization_fn={fn_name}, max_features={self.max_features!r})"
        )
