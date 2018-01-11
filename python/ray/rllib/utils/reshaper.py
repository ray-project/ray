import numpy as np
import tensorflow as tf

class Reshaper(object):
    """
    This class keeps track of where in the flattened observation space we should be slicing and what the
    new shapes should be
    """
    # TODO(ev) support discrete action spaces
    def __init__(self, env_space):
        self.shapes = []
        self.slice_positions = []
        self.env_space = env_space
        if isinstance(env_space, list):
            for space in env_space:
                # Handle both gym arrays and just lists of inputs length
                if hasattr(space, "shape"):
                    arr_shape = np.asarray(space.shape)
                else:
                    arr_shape = space
                self.shapes.append(arr_shape)
                if len(self.slice_positions) == 0:
                    self.slice_positions.append(np.product(arr_shape))
                else:
                    self.slice_positions.append(np.product(arr_shape) + self.slice_positions[-1])
        else:
            self.shapes.append(np.asarray(env_space.shape))
            self.slice_positions.append(np.product(env_space.shape))


    def get_flat_shape(self):
        return self.slice_positions[-1]


    def get_slice_lengths(self):
        diffed_list = np.diff(self.slice_positions).tolist()
        diffed_list.insert(0, self.slice_positions[0])
        return np.asarray(diffed_list).astype(int)


    # def get_flat_box(self):
    #     lows = []
    #     highs = []
    #     if isinstance(self.env_space, list):
    #         for i in range(len(self.env_space)):
    #             lows += self.env_space[i].low.tolist()
    #             highs += self.env_space[i].high.tolist()
    #         return gym.spaces.Box(np.asarray(lows), np.asarray(highs))
    #     else:
    #         return gym.spaces.Box(self.env_space.low, self.env_space.high)


    def split_tensor(self, tensor, axis=-1):
        # FIXME (ev) brittle. Should instead use information about distributions to scale appropriately
        slice_rescale = int(tensor.shape.as_list()[axis] / int(np.sum(self.get_slice_lengths())))
        return tf.split(tensor, slice_rescale*self.get_slice_lengths(), axis=axis)


    def split_number(self, number):
        slice_rescale = int(number / int(np.sum(self.get_slice_lengths())))
        import ipdb; ipdb.set_trace()
        return slice_rescale*self.get_slice_lengths()


    def split_agents(self, tensor, axis=-1):
        return tf.split(tensor)