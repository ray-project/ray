from typing import Optional, Tuple, Union
from ray.data import Dataset


def train_test_split(
    dataset: Dataset,
    test_size: Union[int, float],
    *,
    shuffle: bool = False,
    seed: Optional[int] = None,
) -> Tuple[Dataset, Dataset]:
    """Split a Dataset into train and test subsets.

    Example:
        .. code-block:: python

            import ray
            from ray.ml import train_test_split

            ds = ray.data.range(8)
            train, test = train_test_split(ds, test_size=0.25)
            print(train.take())  # [0, 1, 2, 3, 4, 5]
            print(test.take())  # [6, 7]

    Args:
        dataset: Dataset to split.
        test_size: If float, should be between 0.0 and 1.0 and represent the proportion
            of the dataset to include in the test split. If int, represents the
            absolute number of test samples. The train split will always be the
            compliment of the test split.
        shuffle: Whether or not to globally shuffle the dataset before splitting.
            Defaults to False. This may be a very expensive operation with large
            datasets.
        seed: Fix the random seed to use for shuffle, otherwise one will be chosen
            based on system randomness. Ignored if ``shuffle=False``.

    Returns:
        Train and test subsets as two Datasets.
    """
    if shuffle:
        dataset = dataset.random_shuffle(seed=seed)

    if not isinstance(test_size, (int, float)):
        raise TypeError(f"`test_size` must be int or float got {type(test_size)}.")
    if isinstance(test_size, float):
        if test_size <= 0 or test_size >= 1:
            raise ValueError(
                "If `test_size` is a float, it must be bigger than 0 and smaller than "
                f"1. Got {test_size}."
            )
        return dataset.split_proportionately([1 - test_size])
    else:
        dataset_length = dataset.count()
        if test_size <= 0 or test_size >= dataset_length:
            raise ValueError(
                "If `test_size` is an int, it must be bigger than 0 and smaller than "
                f"the size of the dataset ({dataset_length}). Got {test_size}."
            )
        return dataset.split_at_indices([dataset_length - test_size])
