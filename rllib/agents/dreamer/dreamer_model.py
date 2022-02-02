import numpy as np
from typing import Any, List, Tuple
from ray.rllib.models.torch.misc import Reshape
from ray.rllib.models.torch.torch_modelv2 import TorchModelV2
from ray.rllib.utils.framework import try_import_torch
from ray.rllib.utils.framework import TensorType

torch, nn = try_import_torch()
if torch:
    from torch import distributions as td
    from ray.rllib.agents.dreamer.utils import (
        Linear,
        Conv2d,
        ConvTranspose2d,
        GRUCell,
        TanhBijector,
    )

ActFunc = Any


# Encoder, part of PlaNET
class ConvEncoder(nn.Module):
    """Standard Convolutional Encoder for Dreamer. This encoder is used
    to encode images frm an enviornment into a latent state for the
    RSSM model in PlaNET.
    """

    def __init__(
        self, depth: int = 32, act: ActFunc = None, shape: Tuple[int] = (3, 64, 64)
    ):
        """Initializes Conv Encoder

        Args:
            depth (int): Number of channels in the first conv layer
            act (Any): Activation for Encoder, default ReLU
            shape (List): Shape of observation input
        """
        super().__init__()
        self.act = act
        if not act:
            self.act = nn.ReLU
        self.depth = depth
        self.shape = shape

        init_channels = self.shape[0]
        self.layers = [
            Conv2d(init_channels, self.depth, 4, stride=2),
            self.act(),
            Conv2d(self.depth, 2 * self.depth, 4, stride=2),
            self.act(),
            Conv2d(2 * self.depth, 4 * self.depth, 4, stride=2),
            self.act(),
            Conv2d(4 * self.depth, 8 * self.depth, 4, stride=2),
            self.act(),
        ]
        self.model = nn.Sequential(*self.layers)

    def forward(self, x):
        # Flatten to [batch*horizon, 3, 64, 64] in loss function
        orig_shape = list(x.size())
        x = x.view(-1, *(orig_shape[-3:]))
        x = self.model(x)

        new_shape = orig_shape[:-3] + [32 * self.depth]
        x = x.view(*new_shape)
        return x


# Decoder, part of PlaNET
class ConvDecoder(nn.Module):
    """Standard Convolutional Decoder for Dreamer.

    This decoder is used to decode images from the latent state generated
    by the transition dynamics model. This is used in calculating loss and
    logging gifs for imagined trajectories.
    """

    def __init__(
        self,
        input_size: int,
        depth: int = 32,
        act: ActFunc = None,
        shape: Tuple[int] = (3, 64, 64),
    ):
        """Initializes a ConvDecoder instance.

        Args:
            input_size (int): Input size, usually feature size output from
                RSSM.
            depth (int): Number of channels in the first conv layer
            act (Any): Activation for Encoder, default ReLU
            shape (List): Shape of observation input
        """
        super().__init__()
        self.act = act
        if not act:
            self.act = nn.ReLU
        self.depth = depth
        self.shape = shape

        self.layers = [
            Linear(input_size, 32 * self.depth),
            Reshape([-1, 32 * self.depth, 1, 1]),
            ConvTranspose2d(32 * self.depth, 4 * self.depth, 5, stride=2),
            self.act(),
            ConvTranspose2d(4 * self.depth, 2 * self.depth, 5, stride=2),
            self.act(),
            ConvTranspose2d(2 * self.depth, self.depth, 6, stride=2),
            self.act(),
            ConvTranspose2d(self.depth, self.shape[0], 6, stride=2),
        ]
        self.model = nn.Sequential(*self.layers)

    def forward(self, x):
        # x is [batch, hor_length, input_size]
        orig_shape = list(x.size())
        x = self.model(x)

        reshape_size = orig_shape[:-1] + list(self.shape)
        mean = x.view(*reshape_size)

        # Equivalent to making a multivariate diag
        return td.Independent(td.Normal(mean, 1), len(self.shape))


# Reward Model (PlaNET), and Value Function
class DenseDecoder(nn.Module):
    """FC network that outputs a distribution for calculating log_prob.

    Used later in DreamerLoss.
    """

    def __init__(
        self,
        input_size: int,
        output_size: int,
        layers: int,
        units: int,
        dist: str = "normal",
        act: ActFunc = None,
    ):
        """Initializes FC network

        Args:
            input_size (int): Input size to network
            output_size (int): Output size to network
            layers (int): Number of layers in network
            units (int): Size of the hidden layers
            dist (str): Output distribution, parameterized by FC output
                logits.
            act (Any): Activation function
        """
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
        if self.output_size == 1:
            x = torch.squeeze(x)
        if self.dist == "normal":
            output_dist = td.Normal(x, 1)
        elif self.dist == "binary":
            output_dist = td.Bernoulli(logits=x)
        else:
            raise NotImplementedError("Distribution type not implemented!")
        return td.Independent(output_dist, 0)


# Represents dreamer policy
class ActionDecoder(nn.Module):
    """ActionDecoder is the policy module in Dreamer.

    It outputs a distribution parameterized by mean and std, later to be
    transformed by a custom TanhBijector in utils.py for Dreamer.
    """

    def __init__(
        self,
        input_size: int,
        action_size: int,
        layers: int,
        units: int,
        dist: str = "tanh_normal",
        act: ActFunc = None,
        min_std: float = 1e-4,
        init_std: float = 5.0,
        mean_scale: float = 5.0,
    ):
        """Initializes Policy

        Args:
            input_size (int): Input size to network
            action_size (int): Action space size
            layers (int): Number of layers in network
            units (int): Size of the hidden layers
            dist (str): Output distribution, with tanh_normal implemented
            act (Any): Activation function
            min_std (float): Minimum std for output distribution
            init_std (float): Intitial std
            mean_scale (float): Augmenting mean output from FC network
        """
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
            self.layers.append(Linear(cur_size, 2 * action_size))
        elif self.dist == "onehot":
            self.layers.append(Linear(cur_size, action_size))
        self.model = nn.Sequential(*self.layers)

    # Returns distribution
    def forward(self, x):
        raw_init_std = np.log(np.exp(self.init_std) - 1)
        x = self.model(x)
        if self.dist == "tanh_normal":
            mean, std = torch.chunk(x, 2, dim=-1)
            mean = self.mean_scale * torch.tanh(mean / self.mean_scale)
            std = self.softplus(std + raw_init_std) + self.min_std
            dist = td.Normal(mean, std)
            transforms = [TanhBijector()]
            dist = td.transformed_distribution.TransformedDistribution(dist, transforms)
            dist = td.Independent(dist, 1)
        elif self.dist == "onehot":
            dist = td.OneHotCategorical(logits=x)
            raise NotImplementedError("Atari not implemented yet!")
        return dist


# Represents TD model in PlaNET
class RSSM(nn.Module):
    """RSSM is the core recurrent part of the PlaNET module. It consists of
    two networks, one (obs) to calculate posterior beliefs and states and
    the second (img) to calculate prior beliefs and states. The prior network
    takes in the previous state and action, while the posterior network takes
    in the previous state, action, and a latent embedding of the most recent
    observation.
    """

    def __init__(
        self,
        action_size: int,
        embed_size: int,
        stoch: int = 30,
        deter: int = 200,
        hidden: int = 200,
        act: ActFunc = None,
    ):
        """Initializes RSSM

        Args:
            action_size (int): Action space size
            embed_size (int): Size of ConvEncoder embedding
            stoch (int): Size of the distributional hidden state
            deter (int): Size of the deterministic hidden state
            hidden (int): General size of hidden layers
            act (Any): Activation function
        """
        super().__init__()
        self.stoch_size = stoch
        self.deter_size = deter
        self.hidden_size = hidden
        self.act = act
        if act is None:
            self.act = nn.ELU

        self.obs1 = Linear(embed_size + deter, hidden)
        self.obs2 = Linear(hidden, 2 * stoch)

        self.cell = GRUCell(self.hidden_size, hidden_size=self.deter_size)
        self.img1 = Linear(stoch + action_size, hidden)
        self.img2 = Linear(deter, hidden)
        self.img3 = Linear(hidden, 2 * stoch)

        self.softplus = nn.Softplus

        self.device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )

    def get_initial_state(self, batch_size: int) -> List[TensorType]:
        """Returns the inital state for the RSSM, which consists of mean,
        std for the stochastic state, the sampled stochastic hidden state
        (from mean, std), and the deterministic hidden state, which is
        pushed through the GRUCell.

        Args:
            batch_size (int): Batch size for initial state

        Returns:
            List of tensors
        """
        return [
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.stoch_size).to(self.device),
            torch.zeros(batch_size, self.deter_size).to(self.device),
        ]

    def observe(
        self, embed: TensorType, action: TensorType, state: List[TensorType] = None
    ) -> Tuple[List[TensorType], List[TensorType]]:
        """Returns the corresponding states from the embedding from ConvEncoder
        and actions. This is accomplished by rolling out the RNN from the
        starting state through each index of embed and action, saving all
        intermediate states between.

        Args:
            embed (TensorType): ConvEncoder embedding
            action (TensorType): Actions
            state (List[TensorType]): Initial state before rollout

        Returns:
            Posterior states and prior states (both List[TensorType])
        """
        if state is None:
            state = self.get_initial_state(action.size()[0])

        if embed.dim() <= 2:
            embed = torch.unsqueeze(embed, 1)

        if action.dim() <= 2:
            action = torch.unsqueeze(action, 1)

        embed = embed.permute(1, 0, 2)
        action = action.permute(1, 0, 2)

        priors = [[] for i in range(len(state))]
        posts = [[] for i in range(len(state))]
        last = (state, state)
        for index in range(len(action)):
            # Tuple of post and prior
            last = self.obs_step(last[0], action[index], embed[index])
            [o.append(s) for s, o in zip(last[0], posts)]
            [o.append(s) for s, o in zip(last[1], priors)]

        prior = [torch.stack(x, dim=0) for x in priors]
        post = [torch.stack(x, dim=0) for x in posts]

        prior = [e.permute(1, 0, 2) for e in prior]
        post = [e.permute(1, 0, 2) for e in post]

        return post, prior

    def imagine(
        self, action: TensorType, state: List[TensorType] = None
    ) -> List[TensorType]:
        """Imagines the trajectory starting from state through a list of actions.
        Similar to observe(), requires rolling out the RNN for each timestep.

        Args:
            action (TensorType): Actions
            state (List[TensorType]): Starting state before rollout

        Returns:
            Prior states
        """
        if state is None:
            state = self.get_initial_state(action.size()[0])

        action = action.permute(1, 0, 2)

        indices = range(len(action))
        priors = [[] for _ in range(len(state))]
        last = state
        for index in indices:
            last = self.img_step(last, action[index])
            [o.append(s) for s, o in zip(last, priors)]

        prior = [torch.stack(x, dim=0) for x in priors]
        prior = [e.permute(1, 0, 2) for e in prior]
        return prior

    def obs_step(
        self, prev_state: TensorType, prev_action: TensorType, embed: TensorType
    ) -> Tuple[List[TensorType], List[TensorType]]:
        """Runs through the posterior model and returns the posterior state

        Args:
            prev_state (TensorType): The previous state
            prev_action (TensorType): The previous action
            embed (TensorType): Embedding from ConvEncoder

        Returns:
            Post and Prior state
        """
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

    def img_step(
        self, prev_state: TensorType, prev_action: TensorType
    ) -> List[TensorType]:
        """Runs through the prior model and returns the prior state

        Args:
            prev_state (TensorType): The previous state
            prev_action (TensorType): The previous action

        Returns:
            Prior state
        """
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

    def get_feature(self, state: List[TensorType]) -> TensorType:
        # Constructs feature for input to reward, decoder, actor, critic
        return torch.cat([state[2], state[3]], dim=-1)

    def get_dist(self, mean: TensorType, std: TensorType) -> TensorType:
        return td.Normal(mean, std)


# Represents all models in Dreamer, unifies them all into a single interface
class DreamerModel(TorchModelV2, nn.Module):
    def __init__(self, obs_space, action_space, num_outputs, model_config, name):
        super().__init__(obs_space, action_space, num_outputs, model_config, name)

        nn.Module.__init__(self)
        self.depth = model_config["depth_size"]
        self.deter_size = model_config["deter_size"]
        self.stoch_size = model_config["stoch_size"]
        self.hidden_size = model_config["hidden_size"]

        self.action_size = action_space.shape[0]

        self.encoder = ConvEncoder(self.depth)
        self.decoder = ConvDecoder(self.stoch_size + self.deter_size, depth=self.depth)
        self.reward = DenseDecoder(
            self.stoch_size + self.deter_size, 1, 2, self.hidden_size
        )
        self.dynamics = RSSM(
            self.action_size,
            32 * self.depth,
            stoch=self.stoch_size,
            deter=self.deter_size,
        )
        self.actor = ActionDecoder(
            self.stoch_size + self.deter_size, self.action_size, 4, self.hidden_size
        )
        self.value = DenseDecoder(
            self.stoch_size + self.deter_size, 1, 3, self.hidden_size
        )
        self.state = None

        self.device = (
            torch.device("cuda") if torch.cuda.is_available() else torch.device("cpu")
        )

    def policy(
        self, obs: TensorType, state: List[TensorType], explore=True
    ) -> Tuple[TensorType, List[float], List[TensorType]]:
        """Returns the action. Runs through the encoder, recurrent model,
        and policy to obtain action.
        """
        if state is None:
            self.state = self.get_initial_state(batch_size=obs.shape[0])
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

    def imagine_ahead(self, state: List[TensorType], horizon: int) -> TensorType:
        """Given a batch of states, rolls out more state of length horizon."""
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
            [o.append(s) for s, o in zip(last, outputs)]
        outputs = [torch.stack(x, dim=0) for x in outputs]

        imag_feat = self.dynamics.get_feature(outputs)
        return imag_feat

    def get_initial_state(self) -> List[TensorType]:
        self.state = self.dynamics.get_initial_state(1) + [
            torch.zeros(1, self.action_space.shape[0]).to(self.device)
        ]
        return self.state

    def value_function(self) -> TensorType:
        return None
