from __future__ import absolute_import, division, print_function, unicode_literals
import ray
import time
import ray.rllib.agents.a3c as a3c
from ray.rllib.models import ModelCatalog, Model
from ray.tune.registry import register_env
import gym
from tensorflow.keras import layers, Sequential
import CustomCartpole
import pickle
import os

class CartpoleModel(Model):
    def _build_layers_v2(self, input_dict, num_outputs, options):
        self.model = Sequential()
        self.model.add(layers.InputLayer(input_tensor=input_dict["obs"], input_shape=(4,)))
        self.model.add(layers.Dense(4, name='l1', activation='relu'))
        self.model.add(layers.Dense(10, name='l2', activation='relu'))
        self.model.add(layers.Dense(10, name='l3', activation='relu'))
        self.model.add(layers.Dense(10, name='l4', activation='relu'))
        self.model.add(layers.Dense(2, name='l5', activation='relu'))
        return self.model.get_layer("l5").output, self.model.get_layer("l4").output



ray.init()
ModelCatalog.register_custom_model("CartpoleModel", CartpoleModel)
CartpoleEnv = gym.make('CustomCartpole-v0')
register_env("CP", lambda _:CartpoleEnv)

trainer = a3c.A3CTrainer(env="CP", config={
    "model": {"custom_model": "CartpoleModel"},
    #"observation_filter": "MeanStdFilter",
    #"vf_share_layers": True,
}, logger_creator=lambda _:ray.tune.logger.NoopLogger({},None))

if os.path.isfile('weights.pickle'):
   weights = pickle.load(open("weights.pickle", "rb"))
   trainer.restore_from_object(weights)

if False:

    try:
        while True:
            rest=trainer.train()
            print(rest["episode_reward_mean"])
    except KeyboardInterrupt:
        weights=trainer.save_to_object()
        pickle.dump(weights, open('weights.pickle', 'wb'))
        print('Model saved')


obs=CartpoleEnv.reset()
cur_action = None
total_rev = 0
rew = None
info = None
done = False
while not done:
    cur_action = trainer.compute_action(obs,prev_action=cur_action,prev_reward=rew,info=info)
    obs, rew, done, info = CartpoleEnv.step(cur_action)
    total_rev += rew
    CartpoleEnv.render()
    time.sleep(0.05)
print(total_rev)

