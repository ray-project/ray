

class PolicyV2:
    def __init__(self, observation_space, action_space, config,
                 worker_idx=0):
        self.local_optimizers  # dict[optimID] -> LocalOptimizer
        self.models  # dict[modelID] -> nn.Module or tf.keras.Model
        self.loaded_batches  # list -> SampleBatch (already on device)

        self.devices  # list of CPU/GPU devices; if > 1: Each model will be tower-copied n times to the different devices

    def get_action(self, input_dict, explore=True):
        pass

    def log_likelihood(self, input_dict, action):
        pass

    def postprocess_trajectory(self, batch):
        pass

    def loss(self, input_dict):
        pass

    def train(self, batch=None, loaded_batch_idx=None):
        pass

    def load_batch(self, batch, idx=0):
        pass

    def compute_gradients(self):
        pass

    def apply_gradients(self):
        pass

    def get_state(self):
        pass

    def set_state(self, state):
        pass
