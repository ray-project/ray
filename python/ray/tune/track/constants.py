"""
Both locally and remotely the directory structure is as follows:

project-directory/
  METADATA_FOLDER/
    trialprefix_uuid_param_map.json
    trialprefix_uuid_result.json
    ... other trials
  trailprefix_uuid/
    ... trial artifacts
  ... other trial artifact folders

Where the param map is a single json containing the trial uuid
and configuration parameters and, the result.json is a json
list file (i.e., not valid json, but valid json on each line),
and the artifacts folder contains the artifacts as supplied by
the user.
"""
import os

METADATA_FOLDER = "trials"
CONFIG_SUFFIX = "param_map.json"
RESULT_SUFFIX = "result.json"
