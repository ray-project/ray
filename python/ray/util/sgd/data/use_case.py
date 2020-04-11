


train_list = [(np.arange(10), 1), (np.arange(10), 2)]

dataset = Dataset(train_list)

trainer = Trainer(model_creator=..., data_creator=None)

trainer.train(dataset)


-----------------------------------------------------------

class S3Dataset(Dataset):
    def __init__(self, **kwargs):
        super().__init__(self, download_func=s3_download, **kwargs)

train_paths = ["s3://dat1.dat", "s3://dat2.dat"]

dataset = S3Dataset(train_paths)

trainer = Trainer(model_creator=..., data_creator=None)

trainer.train(dataset)

-----------------------------------------------------------

train_paths = ["ftp://dat1.dat", "ftp://dat2.dat"]

dataset = FileDataset(train_paths)

dataset = Dataset(train_paths, download_func=ftp_download)


------------------------------------------------------------

exist_iter = ParallelIterator.from_items(["s3://blah", "s3://blasdf"]).foreach(ASyncRead()).flatten().filter()

dataset = Dataset(exist_iter)

trainer = Trainer(model_creator=..., data_creator=None)

trainer.train(dataset)
add_actor()
trainer.train(dataset)


class Dataset():

    def __init__(iterable, download_func=None, parallelism=0):
        par_iter = None
        if isinstance(iterable, ParalellIterator):
            par_iter = iterable
        else:
            par_iter = ParallelIterator.from_items(iterable)

        if download_func:
            par_iter.foreach(ASyncRead(download_func))

