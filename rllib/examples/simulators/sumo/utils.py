""" RLLIB SUMO Utils - SUMO Connector Wrapper

    Author: Lara CODECA lara.codeca@gmail.com

    See:
        https://github.com/lcodeca/rllibsumoutils
        https://github.com/lcodeca/rllibsumodocker
    for further details.
"""

import collections
from copy import deepcopy
import logging
import os
from pprint import pformat
import sys

from lxml import etree

from ray.rllib.examples.simulators.sumo.connector import SUMOConnector, DEFAULT_CONFIG

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


def sumo_default_config():
    """Return the default configuration for the SUMO Connector."""
    return deepcopy(DEFAULT_CONFIG)


###############################################################################


class SUMOUtils(SUMOConnector):
    """
    A wrapper for the interaction with the SUMO simulation that adds
    functionalities.
    """

    def _initialize_metrics(self):
        """Specific metrics initialization"""
        # Default TripInfo file metrics
        self.tripinfo = collections.defaultdict(dict)
        self.personinfo = collections.defaultdict(dict)

    ###########################################################################
    # TRIPINFO FILE

    def process_tripinfo_file(self):
        """
        Closes the TraCI connections, then reads and process the tripinfo
        data. It requires "tripinfo_xml_file" and "tripinfo_xml_schema"
        configuration parametes set.
        """

        if "tripinfo_keyword" not in self._config:
            raise Exception(
                "Function process_tripinfo_file requires the parameter "
                "'tripinfo_keyword' set.",
                self._config,
            )

        if "tripinfo_xml_schema" not in self._config:
            raise Exception(
                "Function process_tripinfo_file requires the parameter "
                "'tripinfo_xml_schema' set.",
                self._config,
            )

        # Make sure that the simulation is finished and the tripinfo file is
        # written.
        self.end_simulation()

        # Reset the data structures.
        self.tripinfo = collections.defaultdict(dict)
        self.personinfo = collections.defaultdict(dict)

        schema = etree.XMLSchema(file=self._config["tripinfo_xml_schema"])
        parser = etree.XMLParser(schema=schema)
        tripinfo_file = "{}{}".format(
            self._sumo_output_prefix, self._config["tripinfo_keyword"]
        )
        tree = etree.parse(tripinfo_file, parser)

        logger.info("Processing %s tripinfo file.", tripinfo_file)
        for element in tree.getroot():
            if element.tag == "tripinfo":
                self.tripinfo[element.attrib["id"]] = dict(element.attrib)
            elif element.tag == "personinfo":
                self.personinfo[element.attrib["id"]] = dict(element.attrib)
                stages = []
                for stage in element:
                    stages.append([stage.tag, dict(stage.attrib)])
                self.personinfo[element.attrib["id"]]["stages"] = stages
            else:
                raise Exception("Unrecognized element in the tripinfo file.")
        logger.debug("TRIPINFO: \n%s", pformat(self.tripinfo))
        logger.debug("PERSONINFO: \n%s", pformat(self.personinfo))

    def get_timeloss(self, entity, default=float("NaN")):
        """Returns the timeLoss computed by SUMO for the given entity."""

        if entity in self.tripinfo:
            logger.debug("TRIPINFO for %s", entity)
            if "timeLoss" in self.tripinfo[entity]:
                logger.debug("timeLoss %s", self.tripinfo[entity]["timeLoss"])
                return float(self.tripinfo[entity]["timeLoss"])
            logger.debug("timeLoss not found.")
            return default
        elif entity in self.personinfo:
            logger.debug("PERSONINFO for %s", entity)
            logger.debug("%s", pformat(self.personinfo[entity]))
            time_loss, ts_found = 0.0, False
            for _, stage in self.personinfo[entity]["stages"]:
                if "timeLoss" in stage:
                    logger.debug("timeLoss %s", stage["timeLoss"])
                    time_loss += float(stage["timeLoss"])
                    ts_found = True
            if not ts_found:
                logger.debug("timeLoss not found.")
                return default
            if time_loss <= 0:
                logger.debug("ERROR: timeLoss is %.2f", time_loss)
                return default
            logger.debug("total timeLoss %.2f", time_loss)
            return time_loss
        else:
            logger.debug("Entity %s not found.", entity)
        return default

    def get_depart(self, entity, default=float("NaN")):
        """
        Returns the departure recorded by SUMO for the given entity.

        The functions process_tripinfo_file() needs to be called in advance
        to initialize the data structures required.

        If the entity does not exist or does not have the value, it returns
        the default value.
        """
        if entity in self.tripinfo:
            logger.debug("TRIPINFO for %s", entity)
            if "depart" in self.tripinfo[entity]:
                logger.debug("depart %s", self.tripinfo[entity]["depart"])
                return float(self.tripinfo[entity]["depart"])
            logger.debug("depart not found.")
        elif entity in self.personinfo:
            logger.debug("PERSONINFO for %s", entity)
            logger.debug("%s", pformat(self.personinfo[entity]))
            if "depart" in self.personinfo[entity]:
                logger.debug("depart %s", self.personinfo[entity]["depart"])
                return float(self.personinfo[entity]["depart"])
            logger.debug("depart not found.")
        else:
            logger.debug("Entity %s not found.", entity)
        return default

    def get_duration(self, entity, default=float("NaN")):
        """
        Returns the duration computed by SUMO for the given entity.

        The functions process_tripinfo_file() needs to be called in advance
        to initialize the data structures required.

        If the entity does not exist or does not have the value, it returns
        the default value.
        """
        if entity in self.tripinfo:
            logger.debug("TRIPINFO for %s", entity)
            if "duration" in self.tripinfo[entity]:
                logger.debug("duration %s", self.tripinfo[entity]["duration"])
                return float(self.tripinfo[entity]["duration"])
            logger.debug("duration not found.")
        elif entity in self.personinfo:
            logger.debug("PERSONINFO for %s", entity)
            logger.debug("%s", pformat(self.personinfo[entity]))
            if "depart" in self.personinfo[entity]:
                depart = float(self.personinfo[entity]["depart"])
                arrival = depart
                for _, stage in self.personinfo[entity]["stages"]:
                    if "arrival" in stage:
                        arrival = float(stage["arrival"])
                duration = arrival - depart
                if duration > 0:
                    logger.debug("duration %d", duration)
                    return duration
            logger.debug("duration impossible to compute.")
        else:
            logger.debug("Entity %s not found.", entity)
        return default

    def get_arrival(self, entity, default=float("NaN")):
        """
        Returns the arrival computed by SUMO for the given entity.

        The functions process_tripinfo_file() needs to be called in advance
        to initialize the data structures required.

        If the entity does not exist or does not have the value, it returns
        the default value.
        """
        if entity in self.tripinfo:
            logger.debug("TRIPINFO for %s", entity)
            if "arrival" in self.tripinfo[entity]:
                logger.debug("arrival %s", self.tripinfo[entity]["arrival"])
                return float(self.tripinfo[entity]["arrival"])
            logger.debug("arrival not found.")
            return default
        elif entity in self.personinfo:
            logger.debug("PERSONINFO for %s", entity)
            arrival, arrival_found = 0.0, False
            for _, stage in self.personinfo[entity]["stages"]:
                if "arrival" in stage:
                    logger.debug("arrival %s", stage["arrival"])
                    arrival = float(stage["arrival"])
                    arrival_found = True
            if not arrival_found:
                logger.debug("arrival not found.")
                return default
            if arrival <= 0:
                logger.debug("ERROR: arrival is %.2f", arrival)
                return default
            logger.debug("total arrival %.2f", arrival)
            return arrival
        else:
            logger.debug("Entity %s not found.", entity)
        return default

    def get_global_travel_time(self):
        """
        Returns the global travel time computed from SUMO tripinfo data.

        The functions process_tripinfo_file() needs to be called in advance
        to initialize the data structures required.
        """
        gtt = 0
        for entity in self.tripinfo:
            gtt += self.get_duration(entity, default=0.0)
        for entity in self.personinfo:
            gtt += self.get_duration(entity, default=0.0)
        return gtt

    ###########################################################################
    # ROUTING

    @staticmethod
    def get_mode_parameters(mode):
        """
        Return the correst TraCI parameters for the requested mode.
        See: https://sumo.dlr.de/docs/TraCI/Simulation_Value_Retrieval.html
                    #command_0x87_find_intermodal_route

        Param: mode, String.
        Returns: _mode, _ptype, _vtype
        """
        if mode == "public":
            return "public", "", ""
        if mode == "bicycle":
            return "bicycle", "", "bicycle"
        if mode == "walk":
            return "", "pedestrian", ""
        return "car", "", mode  # (but car is not always necessary, and it may
        #  creates unusable alternatives)

    def is_valid_route(self, mode, route):
        """
        Handle findRoute and findIntermodalRoute results.

        Params:
            mode, String.
            route, return value of findRoute or findIntermodalRoute.
        """
        if route is None:
            # traci failed
            return False
        _mode, _ptype, _vtype = self.get_mode_parameters(mode)
        if not isinstance(route, (list, tuple)):
            # only for findRoute
            if len(route.edges) >= 2:
                return True
        elif _mode == "public":
            for stage in route:
                if stage.line:
                    return True
        elif _mode in ("car", "bicycle"):
            for stage in route:
                if stage.type == tc.STAGE_DRIVING and len(stage.edges) >= 2:
                    return True
        else:
            for stage in route:
                if len(stage.edges) >= 2:
                    return True
        return False

    @staticmethod
    def cost_from_route(route):
        """
        Compute the route cost.
        Params:
            route, return value of findRoute or findIntermodalRoute.
        """
        cost = 0.0
        for stage in route:
            cost += stage.cost
        return cost

    @staticmethod
    def travel_time_from_route(route):
        """
        Compute the route estimated travel time.
        Params:
            route, return value of findRoute or findIntermodalRoute.
        """
        ett = 0.0
        for stage in route:
            ett += stage.estimatedTime
        return ett
