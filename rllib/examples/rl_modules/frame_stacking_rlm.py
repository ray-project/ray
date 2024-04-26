msg = """
This script has been taken out of RLlib b/c:
- This script used `ViewRequirements` ("Trajectory View API") to set up the RLModule,
however, this API will not be part of the new API stack.
Instead, you can use RLlib's built-in ConnectorV2 for frame stacking (or write a custom
ConnectorV2). Take a look at this example script here, which shows how you can do frame-
stacking with RLlib's new ConnectorV2 API.

`ray.rllib.examples.connectors.frame_stacking.py`
"""

raise NotImplementedError(msg)
