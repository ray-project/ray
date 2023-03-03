strategy = tf.distribute.MultiWorkerMirroredStrategy()
with strategy.scope():
    loss_object = tf.keras.losses.SparseCategoricalCrossentropy(from_logits=True)
    optimizer = tf.keras.optimizers.Adam()
    model = build_model()
    model.compile(optimizer=optimizer, loss=loss_object, metrics=["accuracy"])
