from .input_pipeline import create_pretrain_dataset


def make_wiki_train_loader(batch_size=8):
    inputs = [f"./flax_util/sample_data_tfrecord/bert-pretrain-shard{i}.tfrecord" for i in range(16)]

    train_dataset = create_pretrain_dataset(inputs, 
                                            seq_length=128, 
                                            max_predictions_per_seq=20, 
                                            batch_size=8,
                                            is_training=True,
                                            use_next_sentence_label=True)
    return train_dataset


if __name__ == "__main__":

    def convert2numpy(batch):
        new_batch = [dict()]
        for key in batch[0].keys():
            new_batch[0][key] = batch[0][key].numpy()
        new_batch.append(batch[1].numpy())
        del batch
        return new_batch


    train_dataset = get_wiki_train_dataset()

    for batch in train_dataset:
        # print(batch)
        print(convert2numpy(batch))
        break
    # iterator = iter(train_dataset)
    # print(next(iterator))