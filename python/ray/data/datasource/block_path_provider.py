from ray.util.annotations import Deprecated


@Deprecated
class BlockWritePathProvider:
    """Abstract callable that provides concrete output paths when writing
    dataset blocks.

    Current subclasses:
        DefaultBlockWritePathProvider
    """

    def __init__(self) -> None:
        raise DeprecationWarning(
            "`BlockWritePathProvider` has been deprecated in favor of "
            "`FilenameProvider`. For more information, see "
            "https://docs.ray.io/en/master/data/api/doc/ray.data.datasource.FilenameProvider.html",  # noqa: E501
        )


@Deprecated
class DefaultBlockWritePathProvider(BlockWritePathProvider):
    """Default block write path provider implementation that writes each
    dataset block out to a file of the form:
    {base_path}/{dataset_uuid}_{task_index}_{block_index}.{file_format}
    """
