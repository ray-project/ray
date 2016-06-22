# This code is copied and adapted from Andrej Karpathy's code for learning to
# play Pong https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5.

import ray
import numpy as np
import gym

env = gym.make("Pong-v0")
D = 80 * 80
gamma = 0.99 # discount factor for reward
def sigmoid(x):
  return 1.0 / (1.0 + np.exp(-x)) # sigmoid "squashing" function to interval [0,1]

def preprocess(I):
  """preprocess 210x160x3 uint8 frame into 6400 (80x80) 1D float vector"""
  I = I[35:195] # crop
  I = I[::2,::2,0] # downsample by factor of 2
  I[I == 144] = 0 # erase background (background type 1)
  I[I == 109] = 0 # erase background (background type 2)
  I[I != 0] = 1 # everything else (paddles, ball) just set to 1
  return I.astype(np.float).ravel()

def discount_rewards(r):
  """take 1D float array of rewards and compute discounted reward"""
  discounted_r = np.zeros_like(r)
  running_add = 0
  for t in reversed(xrange(0, r.size)):
    if r[t] != 0: running_add = 0 # reset the sum, since this was a game boundary (pong specific!)
    running_add = running_add * gamma + r[t]
    discounted_r[t] = running_add
  return discounted_r

def policy_forward(x, model):
  h = np.dot(model["W1"], x)
  h[h < 0] = 0 # ReLU nonlinearity
  logp = np.dot(model["W2"], h)
  p = sigmoid(logp)
  return p, h # return probability of taking action 2, and hidden state

def policy_backward(eph, epx, epdlogp, model):
  """backward pass. (eph is array of intermediate hidden states)"""
  dW2 = np.dot(eph.T, epdlogp).ravel()
  dh = np.outer(epdlogp, model["W2"])
  dh[eph <= 0] = 0 # backpro prelu
  dW1 = np.dot(dh.T, epx)
  return {"W1": dW1, "W2": dW2}

@ray.remote([dict], [tuple])
def compgrad(model):
  observation = env.reset()
  prev_x = None # used in computing the difference frame
  xs, hs, dlogps, drs = [], [], [], []
  reward_sum = 0
  done = False
  while not done:
    cur_x = preprocess(observation)
    x = cur_x - prev_x if prev_x is not None else np.zeros(D)
    prev_x = cur_x

    aprob, h = policy_forward(x,model)
    action = 2 if np.random.uniform() < aprob else 3 # roll the dice!

    xs.append(x) # observation
    hs.append(h) # hidden state
    y = 1 if action == 2 else 0 # a "fake label"
    dlogps.append(y - aprob) # grad that encourages the action that was taken to be taken (see http://cs231n.github.io/neural-networks-2/#losses if confused)

    observation, reward, done, info = env.step(action)
    reward_sum += reward

    drs.append(reward) # record reward (has to be done after we call step() to get reward for previous action)

  epx = np.vstack(xs)
  eph = np.vstack(hs)
  epdlogp = np.vstack(dlogps)
  epr = np.vstack(drs)
  xs, hs, dlogps, drs = [], [], [], [] # reset array memory

  # compute the discounted reward backwards through time
  discounted_epr = discount_rewards(epr)
  # standardize the rewards to be unit normal (helps control the gradient estimator variance)
  discounted_epr -= np.mean(discounted_epr)
  discounted_epr /= np.std(discounted_epr)
  epdlogp *= discounted_epr # modulate the gradient with advantage (PG magic happens right here.)
  return (policy_backward(eph, epx, epdlogp, model), reward_sum)
