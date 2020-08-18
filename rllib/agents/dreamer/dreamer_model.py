import numpy as np
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.models.torch.recurrent_net import RecurrentNetwork
from ray.rllib.agents.dreamer.utils import Linear, Conv2d, ConvTranspose2d, GRUCell, TanhBijector
torch, nn = try_import_torch()
if torch:
  from torch import distributions as td


# Encoder, part of PlaNET
class ConvEncoder(nn.Module):
  def __init__(self, depth: int = 32, act=None, shape=[3, 64, 64]):
    super().__init__()
    self.act = act
    if not act:
      self.act = nn.ReLU
    self.depth = depth
    self.shape = shape

    init_channels = self.shape[0]
    self.layers = [
          Conv2d(init_channels, self.depth, 4, stride = 2),
          self.act(),
          Conv2d(self.depth, 2 * self.depth, 4, stride = 2),
          self.act(),
          Conv2d(2 * self.depth, 4 * self.depth, 4, stride = 2),
          self.act(),
          Conv2d(4 * self.depth, 8 * self.depth, 4, stride = 2),
          self.act(),
        ]
    self.model=nn.Sequential(*self.layers)

  def forward(self, x):
    # x is [batch, horizon, 3, 64, 64]
    # Flatten to [batch*horizon, 3, 64, 64]
    orig_shape = list(x.size())
    x = x.view(-1, *(orig_shape[-3:]))
    x = self.model(x)

    new_shape = orig_shape[:-3] + [32*self.depth]
    x = x.view(*new_shape)
    return x

class Reshape(nn.Module):
  def __init__(self, shape):
    super().__init__()
    self.shape = shape

  def forward(self, x):
    return x.view(*self.shape)

# Decoder, part of PlaNET
class ConvDecoder(nn.Module):
  def __init__(self, input_size, depth=32, act=None, shape=[3, 64, 64]):
    super().__init__()
    self.act = act
    if not act:
      self.act = nn.ReLU
    self.depth = depth
    self.shape = shape

    self.layers = [
          Linear(input_size , 32*self.depth),
          Reshape((-1, 32*self.depth, 1, 1)),
          ConvTranspose2d(32*self.depth, 4*self.depth, 5, stride = 2),
          self.act(),
          ConvTranspose2d(4*self.depth, 2*self.depth, 5, stride = 2),
          self.act(),
          ConvTranspose2d(2*self.depth, self.depth, 6, stride = 2),
          self.act(),
          ConvTranspose2d(self.depth, self.shape[0], 6, stride = 2),       
        ]
    self.model = nn.Sequential(*self.layers)

  def forward(self, x):
    # x is [batch, hor_length, input_size]
    orig_shape = list(x.size())
    x = self.model(x)

    reshape_size= orig_shape[:-1] + self.shape
    mean = x.view(*reshape_size)
    # Equivalent to making a multivariate diag
    return td.Independent(td.Normal(mean, 1), len(self.shape))


# Reward Model (PlaNET), and Value Function
class DenseDecoder(nn.Module):
  def __init__(self, input_size, output_size, layers, units, dist='normal', act=None):
    super().__init__()
    self.layrs = layers
    self.units = units
    self.act = act
    if not act:
      self.act = nn.ELU
    self.dist = dist
    self.input_size = input_size
    self.output_size = output_size
    self.layers = []
    cur_size = input_size
    for _ in range(self.layrs):
      self.layers.extend([Linear(cur_size, self.units), self.act()])
      cur_size = units
    self.layers.append(Linear(cur_size, output_size))
    self.model = nn.Sequential(*self.layers)

  def forward(self, x):
    x = self.model(x)
    if self.output_size==1:
      x = torch.squeeze(x)
    if self.dist == 'normal':
      output_dist = td.Normal(x, 1)
    elif self.dist == 'binary':
      output_dist = td.Bernoulli(logits=x)
    else:
      raise NotImplementedError("Distribution type not implemented!")
    return td.Independent(output_dist, 0)

# Represents dreamer policy
class ActionDecoder(nn.Module):
  def __init__(self, input_size, action_size, layers, units, dist='tanh_normal', act=None, min_std=1e-4, init_std=5.0, mean_scale=5.0):
    super().__init__()
    self.layrs = layers
    self.units = units
    self.dist = dist
    self.act = act
    if not act:
      self.act = nn.ReLU
    self.min_std = min_std 
    self.init_std = init_std
    self.mean_scale = mean_scale
    self.action_size = action_size

    self.layers = []
    self.softplus = nn.Softplus()

    # MLP Construction
    cur_size = input_size
    for _ in range(self.layrs):
      self.layers.extend([Linear(cur_size, self.units), self.act()])
      cur_size = self.units
    if self.dist == "tanh_normal":
      self.layers.append(Linear(cur_size, 2*action_size))
    elif self.dist == "onehot":
      self.layers.append(Linear(cur_size, action_size))
    self.model = nn.Sequential(*self.layers)

  # Returns distribution
  def forward(self, x):
    raw_init_std = np.log(np.exp(self.init_std) - 1)
    x = self.model(x)
    if self.dist=='tanh_normal':
      mean, std = torch.chunk(x, 2, dim=-1)
      mean = self.mean_scale * torch.tanh(mean/self.mean_scale)
      std = self.softplus(std + raw_init_std) + self.min_std
      dist = td.Normal(mean, std)
      transforms = [TanhBijector()]
      dist = td.transformed_distribution.TransformedDistribution(dist, transforms)
      dist = td.Independent(dist,1)
    elif self.dist=='onehot':
      dist = td.OneHotCategorical(logits=x)
      raise NotImplementedError("Atari not implemented yet!")
    return dist

# Represents TD model in PlaNET
class RSSM(nn.Module):
  def __init__(self, action_size, embed_size, stoch=30, deter=200, hidden=200, act=None):
    super().__init__()
    self.stoch_size = stoch
    self.deter_size = deter
    self.hidden_size = hidden
    self.act = act
    if act is None:
      self.act = nn.ELU

    self.obs1 = Linear(embed_size + deter, hidden)
    self.obs2 = Linear(hidden, 2*stoch)

    self.cell = GRUCell(self.hidden_size, hidden_size=self.deter_size)
    self.img1 = Linear(stoch + action_size, hidden)
    self.img2 = Linear(deter, hidden)
    self.img3 = Linear(hidden, 2*stoch)

    self.softplus = nn.Softplus

    self.device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))

  def get_initial_state(self, batch_size):
    # Mean, Std, Stochastic, Deterministic
    # Deterministic is the GRUCell's state
    return [
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.deter_size).to(self.device),
           ]

  def observe(self, embed, action, state=None):
    # Embed (output from encoder) [batch_size, batch_length, embed_size]
    # Action [batch_size, batch_length, action_size]
    # State (RNN hidden states)
    if state is None:
      state = self.get_initial_state(action.size()[0])

    embed = embed.permute(1,0,2)
    action = action.permute(1,0,2)

    priors = [[] for i in range(len(state))]
    posts = [[] for i in range(len(state))]
    last = (state, state)
    for index in range(len(action)):
      # Tuple of post and prior
      last = self.obs_step(last[0], action[index], embed[index])
      [o.append(l) for l,o in zip(last[0], posts)]
      [o.append(l) for l,o in zip(last[1], priors)]

    prior = [torch.stack(x, dim=0) for x in priors] 
    post = [torch.stack(x, dim=0) for x in posts]

    prior = [e.permute(1,0,2) for e in prior]
    post = [e.permute(1,0,2) for e in post]

    return post, prior

  def imagine(self, action, state=None):
    if state is None:
      state = self.get_initial_state(action.size()[0])

    action = action.permute(1,0,2)

    indices = range(len(action))
    priors = [[] for _ in range(len(state))]
    last = state
    for index in indices:
      last = self.img_step(last, action[index])
      [o.append(l) for l,o in zip(last, priors)]

    prior = [torch.stack(x,dim=0) for x in priors]
    prior = [e.permute(1,0,2) for e in prior]    
    return prior

  def get_feature(self, state):
    return torch.cat([state[2], state[3]], dim=-1)

  def get_dist(self, mean, std):
    return td.Normal(mean, std)

  def obs_step(self, prev_state, prev_action, embed):
    prior = self.img_step(prev_state, prev_action)
    x = torch.cat([prior[3], embed], dim=-1)
    x = self.obs1(x)
    x = self.act()(x)
    x = self.obs2(x)
    mean, std = torch.chunk(x, 2, dim=-1)
    std = self.softplus()(std) + 0.1
    stoch = self.get_dist(mean, std).rsample()
    post = [mean, std, stoch, prior[3]]
    return post, prior


  def img_step(self, prev_state, prev_action):
    x = torch.cat([prev_state[2], prev_action], dim=-1)
    x = self.img1(x)
    x = self.act()(x)
    deter = self.cell(x, prev_state[3])
    x = deter
    x = self.img2(x)
    x = self.act()(x)
    x = self.img3(x)
    mean, std = torch.chunk(x, 2, dim=-1)
    std = self.softplus()(std) + 0.1
    stoch = self.get_dist(mean, std).rsample()
    return [mean, std, stoch, deter]


# Represents all models in Dreamer, unifies them all into a single interface
class DreamerModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config,
                 name):
        super().__init__(obs_space, action_space, num_outputs,
                         model_config, name)

        nn.Module.__init__(self)
        self.depth = model_config["depth_size"]
        self.deter_size = model_config["deter_size"]
        self.stoch_size = model_config["stoch_size"]
        self.hidden_size = model_config["hidden_size"]

        self.action_size = action_space.shape[0]

        self.encoder = ConvEncoder(self.depth)
        self.decoder = ConvDecoder(self.stoch_size + self.deter_size, depth=self.depth)
        self.reward = DenseDecoder(self.stoch_size + self.deter_size, 1, 2, self.hidden_size)
        self.dynamics = RSSM(self.action_size, 32 * self.depth, stoch=self.stoch_size, deter=self.deter_size)
        self.actor = ActionDecoder(self.stoch_size + self.deter_size, self.action_size, 4, self.hidden_size)
        self.value = DenseDecoder(self.stoch_size + self.deter_size, 1, 3, self.hidden_size)
        self.state = None

        self.device = (torch.device("cuda")
              if torch.cuda.is_available() else torch.device("cpu"))

    def policy(self, obs, state, explore=True):
        if state is None:
          self.initial_state()
        else:
          self.state = state
        post = self.state[:4]
        action = self.state[4]

        embed = self.encoder(obs)
        post, _ = self.dynamics.obs_step(post, action, embed)
        feat = self.dynamics.get_feature(post)

        action_dist = self.actor(feat)
        if explore:
          action = action_dist.sample()
        else:
          action = action_dist.mean
        logp = action_dist.log_prob(action)

        self.state = post + [action]
        return action, logp, self.state

    def imagine_ahead(self, state, horizon):
        start = []
        for s in state:
          s = s.contiguous().detach()
          shpe = [-1] + list(s.size())[2:]
          start.append(s.view(*shpe))

        def next_state(state):
          feature = self.dynamics.get_feature(state).detach()
          action = self.actor(feature).rsample()
          next_state = self.dynamics.img_step(state, action)
          return next_state

        last = start
        outputs = [[] for i in range(len(start))]
        for _ in range(horizon):
          last = next_state(last)
          [o.append(l) for l,o in zip(last, outputs)]
        outputs = [torch.stack(x, dim=0) for x in outputs] 

        imag_feat = self.dynamics.get_feature(outputs)
        return imag_feat

    def get_initial_state(self):
        self.state = self.dynamics.get_initial_state(1) + [torch.zeros(1, self.action_space.shape[0]).to(self.device)]
        return self.state 

    def value_function(self):
        return None