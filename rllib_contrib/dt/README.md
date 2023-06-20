# Decision Transformer

[Decision Transformer](https://arxiv.org/abs/2106.01345) is an offline-rl algorithm that trains a transformer to generate
optimal actions based on desired returns, past states, and actions.


## Installation

```
conda create -n rllib-dt python=3.10
conda activate rllib-dt
pip install -r requirements.txt
pip install -e '.[development]'
```

## Usage

[DT Example]()