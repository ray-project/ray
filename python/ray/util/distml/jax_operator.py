"""TODO(Runhui): JAX Operator"""

class TrainingOperator:

    def __init__(self):
        pass

    def setup():
        """Function that needs to be override by users."""
        pass

    def register(self, *, model, optimizer, critierion):
        """Register a few critical information about the model to operator."""
        model = ...
        optimizer = ...
        self._register_model(model)
        self._register_optimizer(optimizer)

    def _register_model(self, model):
        pass

    def _register_optimizer(self, optimizer):
        pass

    def register_data(self, *, train_loader=None, validation_loader=None):

        self.train_loader = train_loader
        self.validation_loader = validation_loader


    def train_epoch(self, iterator, info):
        if not self.train_dataloader:
            raise RuntimeError("Train dataloader hasn't been set.")

        for idx, batch in enumerate(self.train_dataloader):
            self.opt_state = self.update(self.steps,
                                            self.opt_state,
                                            batch)

        params = self.get_params(self.opt_state)
        # train_acc = self.accuracy(params, self.train_dataloader)
        test_acc = self.accuracy(params, self.test_dataloader)
        test_time = time.time() - test_start_time
        print("Epoch {} in {:0.2f} sec, test time {:0.2f} sec."
            .format(epoch, epoch_time, test_time))
        # print("Training set accuracy {}".format(train_acc))
        print("Test set accuracy {}".format(test_acc))

        if not hasattr(self, "model"):
            raise RuntimeError("Either set self.model in setup function or "
                               "override this method to implement a custom "
                               "training loop.")
        if not hasattr(self, "optimizer"):
            raise RuntimeError("Either set self.optimizer in setup function "
                               "or override this method to implement a custom "
                               "training loop.")
        if not hasattr(self, "criterion"):
            raise RuntimeError("Either set self.criterion in setup function "
                               "or override this method to implement a custom "
                               "training loop.")
        model = self.model
        optimizer = self.optimizer
        criterion = self.criterion
        # unpack features into list to support multiple inputs model
        *features, target = batch
        # Create non_blocking tensors for distributed training
        if self.use_gpu:
            features = [
                feature.cuda(non_blocking=True) for feature in features
            ]
            target = target.cuda(non_blocking=True)

        # Compute output.
        output = model(*features)
        loss = criterion(output, target)


        # Call step of optimizer to update model params.
        with self.timers.record("apply"):
            optimizer.step()

        return {"train_loss": loss.item(), NUM_SAMPLES: features[0].size(0)}


if __name__ == "__main__":
    class Dataloader:
        def __init__(self, data, target, batch_size=128, shuffle=False):
            '''
            data: shape(width, height, channel, num)
            target: shape(num, num_classes)
            '''
            self.data = data
            self.target = target
            self.batch_size = batch_size
            num_data = self.target.shape[0]
            num_complete_batches, leftover = divmod(num_data, batch_size)
            self.num_batches = num_complete_batches + bool(leftover)
            self.shuffle = shuffle

        def synth_batches(self):
            num_imgs = self.target.shape[0]
            rng = npr.RandomState(np.random.randint(10))
            perm = rng.permutation(num_imgs) if self.shuffle else np.arange(num_imgs)
            for i in range(self.num_batches):
                batch_idx = perm[i * self.batch_size:(i + 1) * self.batch_size]
                img_batch = self.data[:, :, :, batch_idx]
                label_batch = self.target[batch_idx]
                yield img_batch, label_batch

        def __iter__(self):
            return self.synth_batches()


    class MyTrainingOperator(TrainingOperator):
        def setup(self, config):
            model = nn.Linear(1, 1)
            optimizer = torch.optim.SGD(
                model.parameters(), lr=config.get("lr", 1e-4))
            loss = torch.nn.MSELoss()

            batch_size = config["batch_size"]
            train_data, val_data = LinearDataset(2, 5), LinearDataset(2, 5)
            train_target, val_target = LinearDataset(2, 5), LinearDataset(2, 5)
            train_loader = Dataloader(train_data, train_target, batch_size=batch_size, shuffle=True)
            val_loader = Dataloader(val_data, val_target, batch_size=batch_size)

            self.model, self.optimizer = self.register(
                models=model,
                optimizers=optimizer,
                criterion=loss)

            self.register_data(
                train_loader=train_loader,
                validation_loader=val_loader)