
- `Using fractional GPUs for training your model <https://github.com/ray-project/ray/blob/master/rllib/examples/gpus/fractional_gpus_per_learner.py>`__:
   If your model is small and easily fits on a single GPU and you want to therefore train
   other models alongside it to save time and cost, this script shows you how to set up
   your RLlib config with a fractional number of GPUs on the learner (model training)
   side.
