from tqdm import tqdm


class TqdmReporter:
    def __init__(self, handler):
        self.handler = handler

    def on_epoch_begin(self, info, training_op):
        if training_op.world_rank != 0:
            return

        self.handler.handle_setup(len(training_op.train_loader))

    def on_batch_end(self, batch_info, metrics, training_op):
        if training_op.world_rank != 0:
            return

        pbar_metrics = {}
        if "train_loss" in metrics:
            pbar_metrics["loss"] = metrics["train_loss"]

        self.handler.handle_logs({
            "batch_idx": batch_info["batch_idx"],
            "pbar_metrics": pbar_metrics
        })


class TqdmHandler:
    def __init__(self):
        self.batch_pbar = None

        self.setup = {"new_data": False, "data": None}
        self.logs = {"new_data": False, "data": None}

    def create_reporter(self):
        return TqdmReporter(self)

    def handle_setup(self, loader_len):
        n = self.num_steps
        if n is None:
            n = loader_len

        desc = ""
        if self.train_info is not None and "epoch_idx" in self.train_info:
            if "num_epochs" in self.train_info:
                desc = "{}/{}e".format(self.train_info["epoch_idx"] + 1,
                                       self.train_info["num_epochs"])
            else:
                desc = "{}e".format(self.train_info["epoch_idx"] + 1)

        self.batch_pbar = tqdm(total=n, desc=desc, unit="batch", leave=False)

    def handle_logs(self, logs):
        self.batch_pbar.n = logs["batch_idx"] + 1
        self.batch_pbar.set_postfix(logs["pbar_metrics"])

    def record_train_info(self, info, num_steps):
        self.train_info = info
        self.num_steps = num_steps
