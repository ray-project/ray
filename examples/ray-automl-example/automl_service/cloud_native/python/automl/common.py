
class WorkerTask:
    def __init__(self,
                model,
                df,
                train_indices,
                test_indices,
                label_column,
                metrics,
                freq,
                ):
        self.model = model
        self.df = df
        self.train_indices = train_indices
        self.test_indices = test_indices
        self.label_column = label_column
        self.metrics = metrics
        self.freq = freq
