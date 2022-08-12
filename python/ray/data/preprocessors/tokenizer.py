from typing import List, Callable, Optional

import pandas as pd

from ray.data.preprocessor import Preprocessor
from ray.data.preprocessors.utils import simple_split_tokenizer


class Tokenizer(Preprocessor):
    """Replace each string with a list of tokens.

    Examples:
        >>> import pandas as pd
        >>> import ray
        >>> df = pd.DataFrame({"text": ["Hello, world!", "foo bar\\nbaz"]})
        >>> ds = ray.data.from_pandas(df)  # doctest: +SKIP

        The default ``tokenization_fn`` delimits strings using the space character.

        >>> from ray.data.preprocessors import Tokenizer
        >>> tokenizer = Tokenizer(columns=["text"])
        >>> tokenizer.transform(ds).to_pandas()  # doctest: +SKIP
                       text
        0  [Hello,, world!]
        1   [foo, bar\\nbaz]

        If the default logic isn't adequate for your use case, you can specify a
        custom ``tokenization_fn``.

        >>> import string
        >>> def tokenization_fn(s):
        ...     for character in string.punctuation:
        ...         s = s.replace(character, "")
        ...     return s.split()
        >>> tokenizer = Tokenizer(columns=["text"], tokenization_fn=tokenization_fn)
        >>> tokenizer.transform(ds).to_pandas()  # doctest: +SKIP
                      text
        0   [Hello, world]
        1  [foo, bar, baz]

    Args:
        columns: The columns to tokenize.
        tokenization_fn: The function used to generate tokens. This function
            should accept a string as input and return a list of tokens as
            output. If unspecified, the tokenizer uses a function equivalent to
            ``lambda s: s.split(" ")``.
    """

    _is_fittable = False

    def __init__(
        self,
        columns: List[str],
        tokenization_fn: Optional[Callable[[str], List[str]]] = None,
    ):
        self.columns = columns
        # TODO(matt): Add a more robust default tokenizer.
        self.tokenization_fn = tokenization_fn or simple_split_tokenizer

    def _transform_pandas(self, df: pd.DataFrame):
        def column_tokenizer(s: pd.Series):
            return s.map(self.tokenization_fn)

        df.loc[:, self.columns] = df.loc[:, self.columns].transform(column_tokenizer)
        return df

    def __repr__(self):
        name = getattr(self.tokenization_fn, "__name__", self.tokenization_fn)
        return (
            f"{self.__class__.__name__}(columns={self.columns!r}, "
            f"tokenization_fn={name})"
        )
