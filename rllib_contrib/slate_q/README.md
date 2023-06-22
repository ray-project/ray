# SlateQ (Asynchronous Advantage Actor-Critic)

[SlateQ](https://storage.googleapis.com/pub-tools-public-publication-data/pdf/9f91de1fa0ac351ecb12e4062a37afb896aa1463.pdf) is a model-free RL method that builds on top of DQN and generates recommendation slates for recommender system environments. Since these types of environments come with large combinatorial action spaces, SlateQ mitigates this by decomposing the Q-value into single-item Q-values and solves the decomposed objective via mixing integer programming and deep learning optimization. SlateQ can be evaluated on Google’s RecSim environment.


## Installation

```
conda create -n rllib-slateq python=3.10
conda activate rllib-slateq
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[SlateQ Example]()