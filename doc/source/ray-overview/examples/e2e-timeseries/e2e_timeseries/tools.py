def adjust_learning_rate(optimizer, epoch, config: dict):
    if config["lradj"] == "type1":
        lr_adjust = {epoch: config["learning_rate"] * (0.5 ** ((epoch - 1) // 1))}
    elif config["lradj"] == "type2":
        lr_adjust = {2: 5e-5, 4: 1e-5, 6: 5e-6, 8: 1e-6, 10: 5e-7, 15: 1e-7, 20: 5e-8}
    elif config["lradj"] == "3":
        lr_adjust = {
            epoch: (
                config["learning_rate"] if epoch < 10 else config["learning_rate"] * 0.1
            )
        }
    elif config["lradj"] == "4":
        lr_adjust = {
            epoch: (
                config["learning_rate"] if epoch < 15 else config["learning_rate"] * 0.1
            )
        }
    elif config["lradj"] == "5":
        lr_adjust = {
            epoch: (
                config["learning_rate"] if epoch < 25 else config["learning_rate"] * 0.1
            )
        }
    elif config["lradj"] == "6":
        lr_adjust = {
            epoch: (
                config["learning_rate"] if epoch < 5 else config["learning_rate"] * 0.1
            )
        }
    else:
        print(
            f"Warning: learning rate adjustment type '{config['lradj']}' not recognized. Learning rate not adjusted."
        )
        return

    if epoch in lr_adjust:
        lr = lr_adjust[epoch]
        for param_group in optimizer.param_groups:
            param_group["lr"] = lr
        print("Updating learning rate to {}".format(lr))
