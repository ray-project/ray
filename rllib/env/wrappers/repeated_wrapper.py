import gymnasium as gym
from gymnasium.spaces import Discrete, Box, Dict
from ray.rllib.utils.spaces.repeated import Repeated
import numpy as np
import torch
from ray.rllib.utils.annotations import PublicAPI


@PublicAPI
class ObsVectorizationWrapper(gym.ObservationWrapper):
    """
    Works around the crash currently associated with Repeated action spaces by mapping them to vectors. Includes a method for mapping them to the original space for encoding or other processing, either as batches or as individual observations. See examples/catalogs/attention_encoder.py for usage.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        original_obs_space = self.observation_space
        self.observation_space = gym.spaces.Box(
            0.0,
            1.0,
            shape=(ObsVectorizationWrapper.obs_length(self.observation_space),),
            dtype=np.float32,
        )
        self.observation_space.original = original_obs_space

    #
    def observation(self, observation):
        return self.__class__.serialize_obs(
            observation, self.observation_space.original
        )

    @classmethod
    def serialize_obs(cls, obs, obs_s):
        """Convert a Dict or Repeated space into a [0, 1] vector with all relevant information"""
        if type(obs_s) == Repeated:
            cs = obs_s.child_space
            csl = cls.obs_length(cs)
            ml = obs_s.max_len
            l = ml * (csl + 1)
            v = np.zeros(l, dtype=np.float32)
            for o, sp, ix in zip(obs, range(0, l, csl), range(0, ml)):
                v[ix] = 1
                v[sp + ml : sp + ml + csl] = cls.serialize_obs(o, cs)
            return v
        elif type(obs_s) == Dict:
            vl = []
            cs = obs_s.spaces
            for s in sorted(cs.keys()):
                vl.append(cls.serialize_obs(obs[s], cs[s]))
            return np.concatenate(vl)
        elif type(obs_s) == Box:
            return (obs - obs_s.low) / (obs_s.high - obs_s.low)
        elif type(obs_s) == Discrete:
            x = np.zeros(obs_s.n, dtype=np.float32)
            x[obs] = 1
            return x

    @classmethod
    def restore_obs(cls, vec, obs_s):
        """Convert an observation vector back into its original format."""
        if len(vec.shape) == 2:  # Handle batched observations
            batch_list = []
            for v in vec:
                batch_list.append(cls.restore_obs(v, obs_s))
            return batch_list
        if type(obs_s) == Repeated:
            rl = []
            cs = obs_s.child_space
            csl = cls.obs_length(cs)
            ml = obs_s.max_len
            l = ml * (csl + 1)
            for sp, ix in zip(range(ml, l + ml, csl), range(0, ml)):
                if vec[ix] == 1:
                    rl.append(cls.restore_obs(vec[sp : sp + csl], cs))
                else:
                    break
            return rl
        elif type(obs_s) == Dict:
            d = {}
            cs = obs_s.spaces
            sp2 = 0
            for s in sorted(cs.keys()):
                d[s] = cls.restore_obs(vec[sp2:], cs[s])
                sp2 += cls.obs_length(cs[s])
            return d
        elif type(obs_s) == Box:
            vec = vec[: obs_s.shape[0]]
            h, l = obs_s.high, obs_s.low
            if type(vec) == torch.Tensor:
                h, l = torch.tensor(h).to(vec.device), torch.tensor(l).to(vec.device)
            return vec * (h - l) + l
        elif type(obs_s) == Discrete:
            return vec[: obs_s.n].argmax()

    @classmethod
    def restore_obs_batch(cls, vec, obs_s):
        """
        Convert an observation vector back into its original format.
        This version of the function integrates Repeated spaces more neatly with attention/LSTM-based models, preserving the batch structure and supplementing it with a mask to indicate items' presence or absence.
        """
        if len(vec.shape) == 1:
            vec = np.expand_dims(vec, 0)
        if type(obs_s) == Repeated:
            rl = []
            cs = obs_s.child_space
            csl = cls.obs_length(cs)
            ml = obs_s.max_len
            l = ml * (csl + 1)
            mask = vec[:, :ml]
            for sp in range(ml, l, csl):
                rl.append(cls.restore_obs_batch(vec[:, sp : sp + csl], cs))
            return (rl, mask)
        elif type(obs_s) == Dict:
            d = {}
            cs = obs_s.spaces
            sp2 = 0
            for s in sorted(cs.keys()):
                d[s] = cls.restore_obs_batch(vec[:, sp2:], cs[s])
                sp2 += cls.obs_length(cs[s])
            return d
        elif type(obs_s) == Box:
            vec = vec[:, : obs_s.shape[0]]
            h, l = obs_s.high, obs_s.low
            if type(vec) == torch.Tensor:
                h, l = torch.tensor(h).to(vec.device), torch.tensor(l).to(vec.device)
            return vec * (h - l) + l
        elif type(obs_s) == Discrete:
            return vec[:, : obs_s.n].argmax(axis=1)

    @classmethod
    def obs_length(cls, obs_s):
        """Get the length of a Space's vector representation"""
        if type(obs_s) == Repeated:
            # +1 accounts for the indicator variable that tells you an item exists.
            return obs_s.max_len * (cls.obs_length(obs_s.child_space) + 1)
        elif type(obs_s) == Dict:
            l = 0
            for s in obs_s.spaces.values():
                l += cls.obs_length(s)
            return l
        elif type(obs_s) == Box:
            return obs_s.shape[0]
        elif type(obs_s) == Discrete:
            return obs_s.n
