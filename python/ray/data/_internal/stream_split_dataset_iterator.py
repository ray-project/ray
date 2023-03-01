

class StreamSplitDatasetIterator(DatasetIterator):
    def __init__(
        self,
        coord_actor: ray.actor.ActorHandle,
        output_split_idx: int,
    ):
        self._wrapped_dataset = _DatasetSplit(coord_actor, output_split_idx)

    def iter_batches(
        self,
        *,
        prefetch_blocks: int = 0,
        batch_size: int = 256,
        batch_format: Literal["default", "numpy", "pandas"] = "default",
        drop_last: bool = False,
        local_shuffle_buffer_size: Optional[int] = None,
        local_shuffle_seed: Optional[int] = None,
    ) -> Iterator[DataBatch]:

        block_iterator = self._gen_blocks()

        yield from batch_block_refs(
            block_iterator,
            stats=None,
            prefetch_blocks=prefetch_blocks,
            batch_size=batch_size,
            batch_format=batch_format,
            drop_last=drop_last,
            collate_fn=_collate_fn,
            shuffle_buffer_min_size=local_shuffle_buffer_size,
            shuffle_seed=local_shuffle_seed,
        )

    def _gen_blocks(self) -> Iterator[ObjectRef[Block]]:
        raise NotImplementedError
