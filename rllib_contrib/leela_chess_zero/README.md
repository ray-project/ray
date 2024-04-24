# Leela Chess Zero 

[Leela Chess Zero](https://lczero.org/) Leela chess zero is an algorithm made to train agents on the Leela Chess Engine. The Leela Chess Zero’s neural network is largely based on the DeepMind’s AlphaGo Zero and AlphaZero architecture. There are however some changes. It should be trained in a competition with multiple versions of its past self.

The policy/model assumes that the environment is a MultiAgent Chess environment, that has a discrete action space and returns an observation as a dictionary with two keys:

 - `obs` that contains an observation under either the form of a state vector or an image
 - `action_mask` that contains a mask over the legal actions
 
 It should also implement a `get_state`and a `set_state` function, used in the MCTS implementation.
 
 The model used in AlphaZero trainer should extend `TorchModelV2` and implement the method `compute_priors_and_value`. 


## References

- AlphaZero: https://arxiv.org/abs/1712.01815
- LeelaChessZero: https://github.com/LeelaChessZero/lc0



## Installation

```
conda create -n rllib-leela-chess python=3.10
conda activate rllib-leela-chess
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[Leela Chess Zero Example]()