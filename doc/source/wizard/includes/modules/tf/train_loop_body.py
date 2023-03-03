from ray.air.integrations.keras import ReportCheckpointCallback

epochs = 10

report_metrics_and_checkpoint_callback = ReportCheckpointCallback(
    report_metrics_on="epoch_end", checkpoint_on="epoch_end"
)
model.fit(
    datasets["train"],
    epochs=epochs,
    callbacks=[report_metrics_and_checkpoint_callback],
    verbose=(0 if session.get_world_rank() != 0 else 2),
)

eval_result = model.evaluate(datasets["valid"], return_dict=True, verbose=0)
test_loss = eval_result["loss"]
test_accuracy = eval_result["accuracy"]
if session.get_world_rank() == 0:
    print(
        f"Final Test Loss: {test_loss:.4f}, "
        f"Final Test Accuracy: {test_accuracy:.4f}"
    )
