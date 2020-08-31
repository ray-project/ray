from ray.util.iter import ParallelIterator, from_iterators


class Dataset():
    """A simple Dataset abstraction for RaySGD.

    This dataset is designed to work with RaySGD trainers (currently just
    Torch) to provide support for streaming large external datasets, and built
    in sharding.

    .. code-block:: python

        def to_mat(x):
            return torch.tensor([[x]]).float()


        data = [i * 0.001 for i in range(1000)]
        p_iter = iter.from_items(data, num_shards=1, repeat=True)
        dataset = Dataset(
            p_iter,
            batch_size=32,
            max_concurrency=1,
            download_func=lambda x: (to_mat(x), to_mat(x)))

        trainer = TorchTrainer(
            model_creator=model_creator,
            data_creator=None,
            optimizer_creator=optimizer_creator,
            loss_creator=torch.nn.MSELoss,
            num_workers=5,
        )

        for i in range(10):
            # Train for another epoch using the dataset
            trainer.train(dataset=dataset, num_steps=200)

        model = trainer.get_model()
        print("f(0.5)=", float(model(to_mat(0.5))[0][0]))

    Args:
        data (iterable[U] or ParallelIterator[U]): Any existing python
            iterable (or iterator), or an existing parallel iterator
            to use.
        batch_size (int): The batch size for training/inference (default 32).
        download_func (U -> (S, Y)): A function which returns two values, the
            input and the label (default is the identity function).
        max_concurrency (int): The maximum number of concurrent calls to the
            download function. See ParallelIterator::for_each for details.
        transform (S -> X): A final transformation to be applied to the *input
            only*. This is guaranteed to run on the same worker that training
            will occur on.
    """

    def __init__(self,
                 data,
                 batch_size=32,
                 download_func=None,
                 max_concurrency=0,
                 transform=None):
        par_iter = None
        if isinstance(data, ParallelIterator):
            par_iter = data.repartition(1)
        else:
            par_iter = from_iterators([data], repeat=True)
        if download_func:
            par_iter = par_iter.for_each(
                download_func, max_concurrency=max_concurrency)
        self.iter = par_iter.batch(batch_size)

        self.batch_size = batch_size
        self.max_concurrency = max_concurrency
        self.transform = transform

    def set_num_shards(self, num_shards):
        """
        Reshards the iterator if necessary.
        """
        if num_shards != self.iter.num_shards():
            print("Setting num shards", num_shards)
            self.iter = self.iter.repartition(num_shards)

    def get_shard(self, i):
        """
        Returns a single, iterable shard.
        """
        assert i < self.iter.num_shards(), \
            "Trying to get shard {} but there are only {} shards." + \
            "Are you sure you called set_num_shards already?".format(
                i, self.iter.num_shards()
            )

        return self.iter.get_shard(i)
