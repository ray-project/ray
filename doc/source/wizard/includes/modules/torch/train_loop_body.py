from ray.train.torch import TorchCheckpoint

lr = 1e-3
epochs = 10
device = ray.train.torch.get_device()

# Prepare DDP Model, optimizer, and loss function
model = build_model()

model = ray.train.torch.prepare_model(model)

optimizer = optim.Adam(model.parameters(), lr=lr)
criterion = nn.CrossEntropyLoss()


def evaluate(logits, labels):
    _, preds = torch.max(logits, 1)
    corrects = torch.sum(preds == labels).item()
    return corrects


# Start training loops
for epoch in range(epochs):
    # Each epoch has a training and validation phase
    for phase in ["train", "valid"]:
        if phase == "train":
            model.train()  # Set model to training mode
        else:
            model.eval()  # Set model to evaluate mode

        running_loss = 0.0
        running_corrects = 0

        # Create a dataset iterator for the shard on the current worker

        num_batches = 0
        for batch in datasets[phase]:
            inputs = batch["image"].to(device)
            labels = batch["label"].to(device)

            # zero the parameter gradients
            optimizer.zero_grad()

            # forward
            with torch.set_grad_enabled(phase == "train"):
                # Get model outputs and calculate loss
                outputs = model(inputs)
                loss = criterion(outputs, labels)

                # backward + optimize only if in training phase
                if phase == "train":
                    loss.backward()
                    optimizer.step()

            # calculate statistics
            running_loss += loss.item() * inputs.size(0)
            running_corrects += evaluate(outputs, labels)
            num_batches += 1

        epoch_loss = running_loss / num_batches
        epoch_acc = running_corrects / num_batches

        if session.get_world_rank() == 0:
            print(
                "Epoch {}-{} Loss: {:.4f} Acc: {:.4f}".format(
                    epoch, phase, epoch_loss, epoch_acc
                )
            )

        # Report metrics and checkpoint every epoch
        if phase == "valid":
            checkpoint = TorchCheckpoint.from_dict(
                {
                    "epoch": epoch,
                    "model": model.module.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                }
            )
            session.report(
                metrics={"loss": epoch_loss, "acc": epoch_acc},
                checkpoint=checkpoint,
            )
