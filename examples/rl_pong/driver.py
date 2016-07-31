# This code is copied and adapted from Andrej Karpathy's code for learning to
# play Pong https://gist.github.com/karpathy/a4166c7fe253700972fcbc77e4ea32c5.

import numpy as np
import cPickle as pickle
import ray

import gym

# hyperparameters
H = 200 # number of hidden layer neurons
batch_size = 10 # every how many episodes to do a param update?
learning_rate = 1e-4
gamma = 0.99 # discount factor for reward
decay_rate = 0.99 # decay factor for RMSProp leaky sum of grad^2
resume = False # resume from previous checkpoint?

D = 80 * 80 # input dimensionality: 80x80 grid

# Function for initializing the gym environment.
def env_initializer():
  return gym.make("Pong-v0")

# Function for reinitializing the gym environment in order to guarantee that
# the state of the game is reset after each remote task.
def env_reinitializer(env):
  env.reset()
  return env

# Create a reusable variable for the gym environment.
ray.reusables.env = ray.Reusable(env_initializer, env_reinitializer)

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

@ray.remote([dict], [dict, float])
def compute_gradient(model):
  env = ray.reusables.env
  observation = env.reset()
  prev_x = None # used in computing the difference frame
  xs, hs, dlogps, drs = [], [], [], []
  reward_sum = 0
  done = False
  while not done:
    cur_x = preprocess(observation)
    x = cur_x - prev_x if prev_x is not None else np.zeros(D)
    prev_x = cur_x

    aprob, h = policy_forward(x, model)
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
  return policy_backward(eph, epx, epdlogp, model), reward_sum

if __name__ == "__main__":
  ray.services.start_ray_local(num_workers=10)

  # Run the reinforcement learning
  running_reward = None
  batch_num = 1
  if resume:
    model = pickle.load(open("save.p", "rb"))
  else:
    model = {}
    model["W1"] = np.random.randn(H, D) / np.sqrt(D) # "Xavier" initialization
    model["W2"] = np.random.randn(H) / np.sqrt(H)
  grad_buffer = {k: np.zeros_like(v) for k, v in model.iteritems()} # update buffers that add up gradients over a batch
  rmsprop_cache = {k: np.zeros_like(v) for k, v in model.iteritems()} # rmsprop memory

  while True:
    model_ref = ray.put(model)
    grads, reward_sums = [], []
    # Launch tasks to compute gradients from multiple rollouts in parallel.
    for i in range(batch_size):
      grad_ref, reward_sum_ref = compute_gradient.remote(model_ref)
      grads.append(grad_ref)
      reward_sums.append(reward_sum_ref)
    for i in range(batch_size):
      grad = ray.get(grads[i])
      reward_sum = ray.get(reward_sums[i])
      for k in model: grad_buffer[k] += grad[k] # accumulate grad over batch
      running_reward = reward_sum if running_reward is None else running_reward * 0.99 + reward_sum * 0.01
      print "Batch {}. episode reward total was {}. running mean: {}".format(batch_num, reward_sum, running_reward)
    for k, v in model.iteritems():
      g = grad_buffer[k] # gradient
      rmsprop_cache[k] = decay_rate * rmsprop_cache[k] + (1 - decay_rate) * g ** 2
      model[k] += learning_rate * g / (np.sqrt(rmsprop_cache[k]) + 1e-5)
      grad_buffer[k] = np.zeros_like(v) # reset batch gradient buffer
    batch_num += 1
    if batch_num % 10 == 0: pickle.dump(model, open("save.p", "wb"))
