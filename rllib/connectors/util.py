from typing import Dict

from ray.rllib.connectors.connector import Connector, get_connector


def get_connectors_from_cfg(config: dict) -> Dict[str, Connector]:
    return {k: get_connector(*v) for k, v in config.items()}
