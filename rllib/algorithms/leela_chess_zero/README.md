# LeelaChessZero implementation for Ray/RLlib
## Notes

This code implements a multi-player LeelaChessZero agent, the Leela Chess Zero’s neural network is largely based on the DeepMind’s AlphaGo Zero and AlphaZero architecture. There are however some changes. It should be trained in a competition with multiple versions of its past self.

The code for LeelaChessZero only supports the PyTorch framework.
It assumes that the environment is a MultiAgent Chess environment, that has a discrete action space and returns an observation as a dictionary with two keys:

 - `obs` that contains an observation under either the form of a state vector or an image
 - `action_mask` that contains a mask over the legal actions
 
 It should also implement a `get_state`and a `set_state` function, used in the MCTS implementation.
 
 The model used in AlphaZero trainer should extend `TorchModelV2` and implement the method `compute_priors_and_value`. 
 
## Example on Chess



## References

- AlphaZero: https://arxiv.org/abs/1712.01815
- LeelaChessZero: https://lczero.org/dev/
