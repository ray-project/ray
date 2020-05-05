import random


# Mix in replay to a stream of experiences.
class MixInReplay:
    def __init__(self, num_slots, replay_proportion):
        if replay_proportion > 0 and num_slots == 0:
            raise ValueError(
                "You must set num_slots > 0 if replay_proportion > 0.")
        self.num_slots = num_slots
        self.replay_proportion = replay_proportion
        self.replay_batches = []
        self.replay_index = 0

    def __call__(self, sample_batch):
        output_batches = [sample_batch]
        if self.num_slots <= 0:
            return output_batches  # Replay is disabled.

        # Put in replay buffer if enabled.
        if len(self.replay_batches) < self.num_slots:
            self.replay_batches.append(sample_batch)
        else:
            self.replay_batches[self.replay_index] = sample_batch
            self.replay_index += 1
            self.replay_index %= self.num_slots

        # Replay with some probability.
        f = self.replay_proportion
        while random.random() < f:
            f -= 1
            replay_batch = random.choice(self.replay_batches)
            output_batches.append(replay_batch)

        return output_batches
