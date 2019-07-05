from scipy.stats import genpareto
import numpy as np

import gym
from gym import spaces
from gym.utils import seeding


def flip(edge, np_random):
    return 1 if np_random.uniform() < edge else -1


class KellyCoinflipEnv(gym.Env):
    """The Kelly coinflip game is a simple gambling introduced by Haghani & Dewey 2016's
    'Rational Decision-Making Under Uncertainty: Observed Betting Patterns on a Biased
    Coin' (https://papers.ssrn.com/sol3/papers.cfm?abstract_id=2856963), to test human
    decision-making in a setting like that of the stock market: positive expected value
    but highly stochastic; they found many subjects performed badly, often going broke,
    even though optimal play would reach the maximum with ~95% probability. In the
    coinflip game, the player starts with $25.00 to gamble over 300 rounds; each round,
    they can bet anywhere up to their net worth (in penny increments), and then a coin is
    flipped; with P=0.6, the player wins twice what they bet, otherwise, they lose it.
    $250 is the maximum players are allowed to have. At the end of the 300 rounds, they
    keep whatever they have. The human subjects earned an average of $91; a simple use of
    the Kelly criterion (https://en.wikipedia.org/wiki/Kelly_criterion), giving a
    strategy of betting 20% until the cap is hit, would earn $240; a decision tree
    analysis shows that optimal play earns $246 (https://www.gwern.net/Coin-flip).

    The game short-circuits when either wealth = $0 (since one can never recover) or
    wealth = cap (trivial optimal play: one simply bets nothing thereafter).

    In this implementation, we default to the paper settings of $25, 60% odds, wealth cap
    of $250, and 300 rounds. To specify the action space in advance, we multiply the
    wealth cap (in dollars) by 100 (to allow for all penny bets); should one attempt to
    bet more money than one has, it is rounded down to one's net worth. (Alternately, a
    mistaken bet could end the episode immediately; it's not clear to me which version
    would be better.) For a harder version which randomizes the 3 key parameters, see the
    Generalized Kelly coinflip game."""
    metadata = {'render.modes': ['human']}

    def __init__(self, initial_wealth=25.0, edge=0.6, max_wealth=250.0, max_rounds=300):

        self.action_space = spaces.Discrete(int(max_wealth * 100))  # betting in penny
        # increments
        self.observation_space = spaces.Tuple((
            spaces.Box(0, max_wealth, [1], dtype=np.float32),  # (w,b)
            spaces.Discrete(max_rounds + 1)))
        self.reward_range = (0, max_wealth)
        self.edge = edge
        self.wealth = initial_wealth
        self.initial_wealth = initial_wealth
        self.max_rounds = max_rounds
        self.max_wealth = max_wealth
        self.np_random = None
        self.rounds = None
        self.seed()
        self.reset()

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        bet_in_dollars = min(action/100.0, self.wealth)  # action = desired bet in pennies
        self.rounds -= 1

        coinflip = flip(self.edge, self.np_random)
        self.wealth = min(self.max_wealth, self.wealth + coinflip * bet_in_dollars)

        done = self.wealth < 0.01 or self.wealth == self.max_wealth or not self.rounds
        reward = self.wealth if done else 0.0

        return self._get_obs(), reward, done, {}

    def _get_obs(self):
        return np.array([self.wealth]), self.rounds

    def reset(self):
        self.rounds = self.max_rounds
        self.wealth = self.initial_wealth
        return self._get_obs()

    def render(self, mode='human'):
        print("Current wealth: ", self.wealth, "; Rounds left: ", self.rounds)


class KellyCoinflipGeneralizedEnv(gym.Env):
    """The Generalized Kelly coinflip game is an extension by ArthurB & Gwern Branwen
    which expands the Kelly coinflip game MDP into a POMDP, where the 3 key parameters
    (edge, maximum wealth, and number of rounds) are unknown random variables drawn
    from 3 distributions: a Beta(7,3) for the coinflip edge 0-1, a N(300,25) the total
    number of rounds, and a Pareto(5,200) for the wealth cap. These distributions are
    chosen to be conjugate & easily updatable, to allow for inference (other choices
    like the geometric for number of rounds wouldn't make observations informative),
    and to loosely reflect what a human might expect in the original Kelly coinflip
    game given that the number of rounds wasn't strictly fixed and they weren't told
    the wealth cap until they neared it. With these particular distributions, the
    entire history of the game can be summarized into a few sufficient statistics of
    rounds-elapsed/wins/losses/max-wealth-ever-reached, from which the Bayes-optimal
    decision can (in theory) be made; to avoid all agents having to tediously track
    those sufficient statistics manually in the same way, the observation space is
    augmented from wealth/rounds-left (rounds-left is deleted because it is a hidden
    variable) to current-wealth/rounds-elapsed/wins/losses/maximum-observed-wealth.
    The simple Kelly coinflip game can easily be solved by calculating decision trees,
    but the Generalized Kelly coinflip game may be intractable (although the analysis
    for the edge case alone suggests that the Bayes-optimal value may be very close to
    what one would calculate using a decision tree for any specific case), and
    represents a good challenge for RL agents."""
    metadata = {'render.modes': ['human']}

    def __init__(self, initial_wealth=25.0, edge_prior_alpha=7, edge_prior_beta=3,
                 max_wealth_alpha=5.0, max_wealth_m=200.0, max_rounds_mean=300.0,
                 max_rounds_sd=25.0, reseed=True):
        # store the hyper-parameters for passing back into __init__() during resets so
        # the same hyper-parameters govern the next game's parameters, as the user
        # expects:
        # TODO: this is boilerplate, is there any more elegant way to do this?
        self.initial_wealth = float(initial_wealth)
        self.edge_prior_alpha = edge_prior_alpha
        self.edge_prior_beta = edge_prior_beta
        self.max_wealth_alpha = max_wealth_alpha
        self.max_wealth_m = max_wealth_m
        self.max_rounds_mean = max_rounds_mean
        self.max_rounds_sd = max_rounds_sd

        if reseed or not hasattr(self, 'np_random'):
            self.seed()

        # draw this game's set of parameters:
        edge = self.np_random.beta(edge_prior_alpha, edge_prior_beta)
        max_wealth = round(genpareto.rvs(max_wealth_alpha, max_wealth_m,
                                         random_state=self.np_random))
        max_rounds = int(round(self.np_random.normal(max_rounds_mean, max_rounds_sd)))

        # add an additional global variable which is the sufficient statistic for the
        # Pareto distribution on wealth cap; alpha doesn't update, but x_m does, and
        # simply is the highest wealth count we've seen to date:
        self.max_ever_wealth = float(self.initial_wealth)
        # for the coinflip edge, it is total wins/losses:
        self.wins = 0
        self.losses = 0
        # for the number of rounds, we need to remember how many rounds we've played:
        self.rounds_elapsed = 0

        # the rest proceeds as before:
        self.action_space = spaces.Discrete(int(max_wealth*100))
        self.observation_space = spaces.Tuple((
            spaces.Box(0, max_wealth, shape=[1], dtype=np.float32),  # current wealth
            spaces.Discrete(max_rounds+1),  # rounds elapsed
            spaces.Discrete(max_rounds+1),  # wins
            spaces.Discrete(max_rounds+1),  # losses
            spaces.Box(0, max_wealth, [1], dtype=np.float32)))  # maximum observed wealth
        self.reward_range = (0, max_wealth)
        self.edge = edge
        self.wealth = self.initial_wealth
        self.max_rounds = max_rounds
        self.rounds = self.max_rounds
        self.max_wealth = max_wealth

    def seed(self, seed=None):
        self.np_random, seed = seeding.np_random(seed)
        return [seed]

    def step(self, action):
        bet_in_dollars = min(action/100.0, self.wealth)

        self.rounds -= 1

        coinflip = flip(self.edge, self.np_random)
        self.wealth = min(self.max_wealth, self.wealth + coinflip * bet_in_dollars)
        self.rounds_elapsed += 1

        if coinflip:
            self.max_ever_wealth = max(self.wealth, self.max_ever_wealth)
            self.wins += 1
        else:
            self.losses += 1

        done = self.wealth < 0.01 or self.wealth == self.max_wealth or not self.rounds
        reward = self.max_wealth if done else 0.0

        return self._get_obs(), reward, done, {}

    def _get_obs(self):
        return (np.array([float(self.wealth)]), self.rounds_elapsed, self.wins,
                self.losses, np.array([float(self.max_ever_wealth)]))

    def reset(self):
        # re-init everything to draw new parameters etc, but preserve the RNG for
        # reproducibility and pass in the same hyper-parameters as originally specified:
        self.__init__(initial_wealth=self.initial_wealth,
                      edge_prior_alpha=self.edge_prior_alpha,
                      edge_prior_beta=self.edge_prior_beta,
                      max_wealth_alpha=self.max_wealth_alpha,
                      max_wealth_m=self.max_wealth_m,
                      max_rounds_mean=self.max_rounds_mean,
                      max_rounds_sd=self.max_rounds_sd,
                      reseed=False)
        return self._get_obs()

    def render(self, mode='human'):
        print("Current wealth: ", self.wealth, "; Rounds left: ", self.rounds,
              "; True edge: ", self.edge, "; True max wealth: ", self.max_wealth,
              "; True stopping time: ", self.max_rounds, "; Rounds left: ",
              self.max_rounds - self.rounds_elapsed)
