from typing import Iterable

import pandas as pd


class SourceShard:
    @property
    def shard_id(self) -> int:
        raise NotImplementedError

    def __iter__(self) -> Iterable[pd.DataFrame]:
        raise NotImplementedError

    def __str__(self):
        return repr(self)

    def __repr__(self):
        return f"SourceShard[{self.shard_id}]"
