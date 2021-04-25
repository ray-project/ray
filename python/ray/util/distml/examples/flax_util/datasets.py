from .input_pipeline import create_pretrain_dataset
import jax.numpy as jnp

def make_wiki_train_loader(batch_size=8):
    inputs = [f"/data3/huangrunhui/proj2/tmp_ray/ray/python/ray/util/distml/examples/flax_util/sample_data_tfrecord/bert-pretrain-shard{i}.tfrecord" for i in range(16)]

    train_dataset = create_pretrain_dataset(inputs, 
                                            seq_length=128, 
                                            max_predictions_per_seq=20, 
                                            batch_size=batch_size,
                                            is_training=True,
                                            use_next_sentence_label=True)
    return train_dataset


def tf2numpy(batch):
    new_batch = [dict()]
    for key in batch[0].keys():
        try:
            new_batch[0][key] = batch[0][key].numpy()
        except AttributeError:
            new_batch[0][key] = jnp.asarray(batch[0][key])
    try:
        new_batch.append(batch[1].numpy())
    except AttributeError:
        new_batch.append(jnp.asarray(batch[1]))
    del batch
    return new_batch


if __name__ == "__main__":
    train_dataset = get_wiki_train_dataset()

    for batch in train_dataset:
        # print(batch)
        print(convert2numpy(batch))
        break
    # iterator = iter(train_dataset)
    # print(next(iterator))