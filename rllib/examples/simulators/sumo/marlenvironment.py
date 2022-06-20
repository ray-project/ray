""" Example MARL Environment for RLLIB SUMO Utlis

    Author: Lara CODECA lara.codeca@gmail.com

    See:
        https://github.com/lcodeca/rllibsumoutils
        https://github.com/lcodeca/rllibsumodocker
    for further details.
"""

import collections
import logging
import os
import sys
from pprint import pformat

from numpy.random import RandomState

import gym
from ray.rllib.env import MultiAgentEnv

from ray.rllib.contrib.sumo.utils import SUMOUtils, sumo_default_config

# """ Import SUMO library """
if "SUMO_HOME" in os.environ:
    sys.path.append(os.path.join(os.environ["SUMO_HOME"], "tools"))
    # from traci.exceptions import TraCIException
    import traci.constants as tc
else:
    sys.exit("please declare environment variable 'SUMO_HOME'")

###############################################################################

logger = logging.getLogger(__name__)

###############################################################################


def env_creator(config):
    """Environment creator used in the environment registration."""
    logger.info("Environment creation: SUMOTestMultiAgentEnv")
    return SUMOTestMultiAgentEnv(config)


###############################################################################

MS_TO_KMH = 3.6


class SUMOSimulationWrapper(SUMOUtils):
    """A wrapper for the interaction with the SUMO simulation"""

    def _initialize_simulation(self):
        """Specific simulation initialization."""
        try:
            super()._initialize_simulation()
        except NotImplementedError:
            pass

    def _initialize_metrics(self):
        """Specific metrics initialization"""
        try:
            super()._initialize_metrics()
        except NotImplementedError:
            pass
        self.veh_subscriptions = dict()
        self.collisions = collections.defaultdict(int)

    def _default_step_action(self, agents):
        """Specific code to be executed in every simulation step"""
        try:
            super()._default_step_action(agents)
        except NotImplementedError:
            pass
        # get collisions
        collisions = self.traci_handler.simulation.getCollidingVehiclesIDList()
        logger.debug("Collisions: %s", pformat(collisions))
        for veh in collisions:
            self.collisions[veh] += 1
        # get subscriptions
        self.veh_subscriptions = self.traci_handler.vehicle.getAllSubscriptionResults()
        for veh, vals in self.veh_subscriptions.items():
            logger.debug("Subs: %s, %s", pformat(veh), pformat(vals))
        running = set()
        for agent in agents:
            if agent in self.veh_subscriptions:
                running.add(agent)
        if len(running) == 0:
            logger.info("All the agent left the simulation..")
            self.end_simulation()
        return True


###############################################################################


class SUMOAgent:
    """Agent implementation."""

    def __init__(self, agent, config):
        self.agent_id = agent
        self.config = config
        self.action_to_meaning = dict()
        for pos, action in enumerate(config["actions"]):
            self.action_to_meaning[pos] = config["actions"][action]
        logger.debug(
            "Agent '%s' configuration \n %s", self.agent_id, pformat(self.config)
        )

    def step(self, action, sumo_handler):
        """Implements the logic of each specific action passed as input."""
        logger.debug("Agent %s: action %d", self.agent_id, action)
        # Subscriptions EXAMPLE:
        #     {"agent_0": {64: 14.603468282230542, 104: None},
        #      "agent_1": {64: 12.922797055918513,
        #                  104: ("veh.19", 27.239870121802596)}}
        logger.debug(
            "Subscriptions: %s", pformat(sumo_handler.veh_subscriptions[self.agent_id])
        )
        previous_speed = sumo_handler.veh_subscriptions[self.agent_id][tc.VAR_SPEED]
        new_speed = previous_speed + self.action_to_meaning[action]
        logger.debug("Before %.2f", previous_speed)
        sumo_handler.traci_handler.vehicle.setSpeed(self.agent_id, new_speed)
        logger.debug("After %.2f", new_speed)
        return

    def reset(self, sumo_handler):
        """Resets the agent and return the observation."""
        route = "{}_rou".format(self.agent_id)
        # https://sumo.dlr.de/pydoc/traci._route.html#RouteDomain-add
        sumo_handler.traci_handler.route.add(route, ["road"])
        # insert the agent in the simulation
        # traci.vehicle.add(self, vehID, routeID, typeID="DEFAULT_VEHTYPE",
        #   depart=None, departLane="first", departPos="base", departSpeed="0",
        #   arrivalLane="current", arrivalPos="max", arrivalSpeed="current",
        #   fromTaz="", toTaz="", line="", personCapacity=0, personNumber=0)
        sumo_handler.traci_handler.vehicle.add(
            self.agent_id, route, departLane="best", departSpeed="max"
        )
        sumo_handler.traci_handler.vehicle.subscribeLeader(self.agent_id)
        sumo_handler.traci_handler.vehicle.subscribe(
            self.agent_id, varIDs=[tc.VAR_SPEED]
        )
        logger.info("Agent %s reset done.", self.agent_id)
        return self.agent_id, self.config["start"]


###############################################################################

DEFAULT_SCENARIO_CONFING = {
    "sumo_config": sumo_default_config(),
    "agent_rnd_order": True,
    "log_level": "WARN",
    "seed": 42,
    "misc": {
        "max_distance": 5000,  # [m]
    },
}

DEFAULT_AGENT_CONFING = {
    "origin": "road",
    "destination": "road",
    "start": 0,
    "actions": {  # increase/decrease the speed of:
        "acc": 1.0,  # [m/s]
        "none": 0.0,  # [m/s]
        "dec": -1.0,  # [m/s]
    },
    "max_speed": 130,  # km/h
}


class SUMOTestMultiAgentEnv(MultiAgentEnv):
    """
    A RLLIB environment for testing MARL environments with SUMO simulations.
    """

    def __init__(self, config):
        """Initialize the environment."""
        super(SUMOTestMultiAgentEnv, self).__init__()

        self._config = config

        # logging
        level = logging.getLevelName(config["scenario_config"]["log_level"])
        logger.setLevel(level)

        # SUMO Connector
        self.simulation = None

        # Random number generator
        self.rndgen = RandomState(config["scenario_config"]["seed"])

        # Agent initialization
        self.agents_init_list = dict()
        self.agents = dict()
        for agent, agent_config in self._config["agent_init"].items():
            self.agents[agent] = SUMOAgent(agent, agent_config)

        # Environment initialization
        self.resetted = True
        self.episodes = 0
        self.steps = 0

    def seed(self, seed):
        """Set the seed of a possible random number generator."""
        self.rndgen = RandomState(seed)

    def get_agents(self):
        """Returns a list of the agents."""
        return self.agents.keys()

    def __del__(self):
        logger.info("Environment destruction: SUMOTestMultiAgentEnv")
        if self.simulation:
            del self.simulation

    ###########################################################################
    # OBSERVATIONS

    def get_observation(self, agent):
        """
        Returns the observation of a given agent.
        See http://sumo.sourceforge.net/pydoc/traci._simulation.html
        """
        speed = 0
        distance = self._config["scenario_config"]["misc"]["max_distance"]
        if agent in self.simulation.veh_subscriptions:
            speed = round(
                self.simulation.veh_subscriptions[agent][tc.VAR_SPEED] * MS_TO_KMH
            )
            leader = self.simulation.veh_subscriptions[agent][tc.VAR_LEADER]
            if leader:  # compatible with traci
                veh, dist = leader
                if veh:
                    # compatible with libsumo
                    distance = round(dist)
        ret = [speed, distance]
        logger.debug("Agent %s --> Obs: %s", agent, pformat(ret))
        return ret

    def compute_observations(self, agents):
        """For each agent in the list, return the observation."""
        obs = dict()
        for agent in agents:
            obs[agent] = self.get_observation(agent)
        return obs

    ###########################################################################
    # REWARDS

    def get_reward(self, agent):
        """Return the reward for a given agent."""
        speed = self.agents[agent].config[
            "max_speed"
        ]  # if the agent is not in the subscriptions
        # and this function is called, the agent has
        # reached the end of the road
        if agent in self.simulation.veh_subscriptions:
            speed = round(
                self.simulation.veh_subscriptions[agent][tc.VAR_SPEED] * MS_TO_KMH
            )
        logger.debug("Agent %s --> Reward %d", agent, speed)
        return speed

    def compute_rewards(self, agents):
        """For each agent in the list, return the rewards."""
        rew = dict()
        for agent in agents:
            rew[agent] = self.get_reward(agent)
        return rew

    ###########################################################################
    # REST & LEARNING STEP

    def reset(self):
        """Resets the env and returns observations from ready agents."""
        self.resetted = True
        self.episodes += 1
        self.steps = 0

        # Reset the SUMO simulation
        if self.simulation:
            del self.simulation

        self.simulation = SUMOSimulationWrapper(
            self._config["scenario_config"]["sumo_config"]
        )

        # Reset the agents
        waiting_agents = list()
        for agent in self.agents.values():
            agent_id, start = agent.reset(self.simulation)
            waiting_agents.append((start, agent_id))
        waiting_agents.sort()

        # Move the simulation forward
        starting_time = waiting_agents[0][0]
        self.simulation.fast_forward(starting_time)
        self.simulation._default_step_action(
            self.agents.keys()
        )  # hack to retrieve the subs

        # Observations
        initial_obs = self.compute_observations(self.agents.keys())

        return initial_obs

    def step(self, action_dict):
        """
        Returns observations from ready agents.

        The returns are dicts mapping from agent_id strings to values. The
        number of agents in the env can vary over time.

        Returns
        -------
            obs: New observations for each ready agent.
            rewards: Reward values for each ready agent. If the
                episode is just started, the value will be None.
            dones: Done values for each ready agent. The special key
                "__all__" (required) is used to indicate env termination.
            infos: Optional info values for each agent id.
        """
        self.resetted = False
        self.steps += 1
        logger.debug(
            "====> [SUMOTestMultiAgentEnv:step] Episode: %d - Step: %d <====",
            self.episodes,
            self.steps,
        )
        dones = {}
        dones["__all__"] = False

        shuffled_agents = sorted(
            action_dict.keys()
        )  # it may seem not smar to sort something that
        # may need to be shuffled afterwards, but it
        # is a matter of consistency instead of using
        # whatever insertion order was used in the dict
        if self._config["scenario_config"]["agent_rnd_order"]:
            # randomize the agent order to minimize SUMO's
            # insertion queues impact
            logger.debug("Shuffling the order of the agents.")
            self.rndgen.shuffle(shuffled_agents)  # in-place shuffle

        # Take action
        for agent in shuffled_agents:
            self.agents[agent].step(action_dict[agent], self.simulation)

        logger.debug("Before SUMO")
        ongoing_simulation = self.simulation.step(
            until_end=False, agents=set(action_dict.keys())
        )
        logger.debug("After SUMO")

        # end of the episode
        if not ongoing_simulation:
            logger.info("Reached the end of the SUMO simulation.")
            dones["__all__"] = True

        obs, rewards, infos = {}, {}, {}

        for agent in action_dict:
            # check for collisions
            if self.simulation.collisions[agent] > 0:
                # punish the agent and remove it from the simulation
                dones[agent] = True
                obs[agent] = [0, 0]
                rewards[agent] = -self.agents[agent].config["max_speed"]
                # infos[agent] = "Collision"
                self.simulation.traci_handler.remove(agent, reason=tc.REMOVE_VAPORIZED)
            else:
                dones[agent] = agent not in self.simulation.veh_subscriptions
                obs[agent] = self.get_observation(agent)
                rewards[agent] = self.get_reward(agent)
                # infos[agent] = ""

        logger.debug("Observations: %s", pformat(obs))
        logger.debug("Rewards: %s", pformat(rewards))
        logger.debug("Dones: %s", pformat(dones))
        logger.debug("Info: %s", pformat(infos))
        logger.debug("========================================================")
        return obs, rewards, dones, infos

    ###########################################################################
    # ACTIONS & OBSERATIONS SPACE

    def get_action_space_size(self, agent):
        """Returns the size of the action space."""
        return len(self.agents[agent].config["actions"])

    def get_action_space(self, agent):
        """Returns the action space."""
        return gym.spaces.Discrete(self.get_action_space_size(agent))

    def get_set_of_actions(self, agent):
        """Returns the set of possible actions for an agent."""
        return set(range(self.get_action_space_size(agent)))

    def get_obs_space_size(self, agent):
        """Returns the size of the observation space."""
        return (self.agents[agent].config["max_speed"] + 1) * (
            self._config["scenario_config"]["misc"]["max_distance"] + 1
        )

    def get_obs_space(self, agent):
        """Returns the observation space."""
        return gym.spaces.MultiDiscrete(
            [
                self.agents[agent].config["max_speed"] + 1,
                self._config["scenario_config"]["misc"]["max_distance"] + 1,
            ]
        )
