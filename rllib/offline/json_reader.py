import glob
import json
import logging
import numpy as np
import os
from pathlib import Path
import random
import re
import tree  # pip install dm_tree
from typing import List, Optional, TYPE_CHECKING, Union
from urllib.parse import urlparse
import zipfile

try:
    from smart_open import smart_open
except ImportError:
    smart_open = None

from ray.rllib.offline.input_reader import InputReader
from ray.rllib.offline.io_context import IOContext
from ray.rllib.policy.policy import Policy
from ray.rllib.policy.sample_batch import (
    DEFAULT_POLICY_ID,
    MultiAgentBatch,
    SampleBatch,
)
from ray.rllib.utils.annotations import override, PublicAPI, DeveloperAPI
from ray.rllib.utils.compression import unpack_if_needed
from ray.rllib.utils.spaces.space_utils import clip_action, normalize_action
from ray.rllib.utils.typing import Any, FileType, SampleBatchType

if TYPE_CHECKING:
    from ray.rllib.evaluation import RolloutWorker

logger = logging.getLogger(__name__)

WINDOWS_DRIVES = [chr(i) for i in range(ord("c"), ord("z") + 1)]


def _adjust_obs_actions_for_policy(json_data: dict, policy: Policy) -> dict:
    """Handle nested action/observation spaces for policies.

    Translates nested lists/dicts from the json into proper
    np.ndarrays, according to the (nested) observation- and action-
    spaces of the given policy.

    Providing nested lists w/o this preprocessing step would
    confuse a SampleBatch constructor.
    """
    for k, v in json_data.items():
        data_col = (
            policy.view_requirements[k].data_col
            if k in policy.view_requirements
            else ""
        )
        if policy.config.get("_disable_action_flattening") and (
            k == SampleBatch.ACTIONS
            or data_col == SampleBatch.ACTIONS
            or k == SampleBatch.PREV_ACTIONS
            or data_col == SampleBatch.PREV_ACTIONS
        ):
            json_data[k] = tree.map_structure_up_to(
                policy.action_space_struct,
                lambda comp: np.array(comp),
                json_data[k],
                check_types=False,
            )
        elif policy.config.get("_disable_preprocessor_api") and (
            k == SampleBatch.OBS
            or data_col == SampleBatch.OBS
            or k == SampleBatch.NEXT_OBS
            or data_col == SampleBatch.NEXT_OBS
        ):
            json_data[k] = tree.map_structure_up_to(
                policy.observation_space_struct,
                lambda comp: np.array(comp),
                json_data[k],
                check_types=False,
            )
    return json_data


@DeveloperAPI
def from_json_data(json_data: Any, worker: Optional["RolloutWorker"]):
    # Try to infer the SampleBatchType (SampleBatch or MultiAgentBatch).
    if "type" in json_data:
        data_type = json_data.pop("type")
    else:
        raise ValueError("JSON record missing 'type' field")

    if data_type == "SampleBatch":
        if worker is not None and len(worker.policy_map) != 1:
            raise ValueError(
                "Found single-agent SampleBatch in input file, but our "
                "PolicyMap contains more than 1 policy!"
            )
        for k, v in json_data.items():
            json_data[k] = unpack_if_needed(v)
        if worker is not None:
            policy = next(iter(worker.policy_map.values()))
            json_data = _adjust_obs_actions_for_policy(json_data, policy)
        return SampleBatch(json_data)
    elif data_type == "MultiAgentBatch":
        policy_batches = {}
        for policy_id, policy_batch in json_data["policy_batches"].items():
            inner = {}
            for k, v in policy_batch.items():
                inner[k] = unpack_if_needed(v)
            if worker is not None:
                policy = worker.policy_map[policy_id]
                inner = _adjust_obs_actions_for_policy(inner, policy)
            policy_batches[policy_id] = SampleBatch(inner)
        return MultiAgentBatch(policy_batches, json_data["count"])
    else:
        raise ValueError(
            "Type field must be one of ['SampleBatch', 'MultiAgentBatch']", data_type
        )


# TODO(jungong) : use DatasetReader to back JsonReader, so we reduce
#     codebase complexity without losing existing functionality.
@PublicAPI
class JsonReader(InputReader):
    """Reader object that loads experiences from JSON file chunks.

    The input files will be read from in random order.
    """

    @PublicAPI
    def __init__(
        self, inputs: Union[str, List[str]], ioctx: Optional[IOContext] = None
    ):
        """Initializes a JsonReader instance.

        Args:
            inputs: Either a glob expression for files, e.g. `/tmp/**/*.json`,
                or a list of single file paths or URIs, e.g.,
                ["s3://bucket/file.json", "s3://bucket/file2.json"].
            ioctx: Current IO context object or None.
        """
        logger.info(
            "You are using JSONReader. It is recommended to use "
            + "DatasetReader instead for better sharding support."
        )

        self.ioctx = ioctx or IOContext()
        self.default_policy = self.policy_map = None
        if self.ioctx.worker is not None:
            self.policy_map = self.ioctx.worker.policy_map
            self.default_policy = self.policy_map.get(DEFAULT_POLICY_ID)

        if isinstance(inputs, str):
            inputs = os.path.abspath(os.path.expanduser(inputs))
            if os.path.isdir(inputs):
                inputs = [os.path.join(inputs, "*.json"), os.path.join(inputs, "*.zip")]
                logger.warning(f"Treating input directory as glob patterns: {inputs}")
            else:
                inputs = [inputs]

            if any(urlparse(i).scheme not in [""] + WINDOWS_DRIVES for i in inputs):
                raise ValueError(
                    "Don't know how to glob over `{}`, ".format(inputs)
                    + "please specify a list of files to read instead."
                )
            else:
                self.files = []
                for i in inputs:
                    self.files.extend(glob.glob(i))
        elif isinstance(inputs, (list, tuple)):
            self.files = list(inputs)
        else:
            raise ValueError(
                "type of inputs must be list or str, not {}".format(inputs)
            )
        if self.files:
            logger.info("Found {} input files.".format(len(self.files)))
        else:
            raise ValueError("No files found matching {}".format(inputs))
        self.cur_file = None

    @override(InputReader)
    def next(self) -> SampleBatchType:
        batch = self._try_parse(self._next_line())
        tries = 0
        while not batch and tries < 100:
            tries += 1
            logger.debug("Skipping empty line in {}".format(self.cur_file))
            batch = self._try_parse(self._next_line())
        if not batch:
            raise ValueError(
                "Failed to read valid experience batch from file: {}".format(
                    self.cur_file
                )
            )

        return self._postprocess_if_needed(batch)

    def read_all_files(self) -> SampleBatchType:
        """Reads through all files and yields one SampleBatchType per line.

        When reaching the end of the last file, will start from the beginning
        again.

        Yields:
            One SampleBatch or MultiAgentBatch per line in all input files.
        """
        for path in self.files:
            file = self._try_open_file(path)
            while True:
                line = file.readline()
                if not line:
                    break
                batch = self._try_parse(line)
                if batch is None:
                    break
                yield batch

    def _postprocess_if_needed(self, batch: SampleBatchType) -> SampleBatchType:
        if not self.ioctx.config.get("postprocess_inputs"):
            return batch

        if isinstance(batch, SampleBatch):
            out = []
            for sub_batch in batch.split_by_episode():
                out.append(self.default_policy.postprocess_trajectory(sub_batch))
            return SampleBatch.concat_samples(out)
        else:
            # TODO(ekl) this is trickier since the alignments between agent
            #  trajectories in the episode are not available any more.
            raise NotImplementedError(
                "Postprocessing of multi-agent data not implemented yet."
            )

    def _try_open_file(self, path):
        if urlparse(path).scheme not in [""] + WINDOWS_DRIVES:
            if smart_open is None:
                raise ValueError(
                    "You must install the `smart_open` module to read "
                    "from URIs like {}".format(path)
                )
            ctx = smart_open
        else:
            # Allow shortcut for home directory ("~/" -> env[HOME]).
            if path.startswith("~/"):
                path = os.path.join(os.environ.get("HOME", ""), path[2:])

            # If path doesn't exist, try to interpret is as relative to the
            # rllib directory (located ../../ from this very module).
            path_orig = path
            if not os.path.exists(path):
                path = os.path.join(Path(__file__).parent.parent, path)
            if not os.path.exists(path):
                raise FileNotFoundError(f"Offline file {path_orig} not found!")

            # Unzip files, if necessary and re-point to extracted json file.
            if re.search("\\.zip$", path):
                with zipfile.ZipFile(path, "r") as zip_ref:
                    zip_ref.extractall(Path(path).parent)
                path = re.sub("\\.zip$", ".json", path)
                assert os.path.exists(path)
            ctx = open
        file = ctx(path, "r")
        return file

    def _try_parse(self, line: str) -> Optional[SampleBatchType]:
        line = line.strip()
        if not line:
            return None
        try:
            batch = self._from_json(line)
        except Exception:
            logger.exception(
                "Ignoring corrupt json record in {}: {}".format(self.cur_file, line)
            )
            return None

        # Clip actions (from any values into env's bounds), if necessary.
        cfg = self.ioctx.config
        # TODO(jungong) : we should not clip_action in input reader.
        # Use connector to handle this.
        if cfg.get("clip_actions") and self.ioctx.worker is not None:
            if isinstance(batch, SampleBatch):
                batch[SampleBatch.ACTIONS] = clip_action(
                    batch[SampleBatch.ACTIONS], self.default_policy.action_space_struct
                )
            else:
                for pid, b in batch.policy_batches.items():
                    b[SampleBatch.ACTIONS] = clip_action(
                        b[SampleBatch.ACTIONS],
                        self.ioctx.worker.policy_map[pid].action_space_struct,
                    )
        # Re-normalize actions (from env's bounds to zero-centered), if
        # necessary.
        if (
            cfg.get("actions_in_input_normalized") is False
            and self.ioctx.worker is not None
        ):

            # If we have a complex action space and actions were flattened
            # and we have to normalize -> Error.
            error_msg = (
                "Normalization of offline actions that are flattened is not "
                "supported! Make sure that you record actions into offline "
                "file with the `_disable_action_flattening=True` flag OR "
                "as already normalized (between -1.0 and 1.0) values. "
                "Also, when reading already normalized action values from "
                "offline files, make sure to set "
                "`actions_in_input_normalized=True` so that RLlib will not "
                "perform normalization on top."
            )

            if isinstance(batch, SampleBatch):
                pol = self.default_policy
                if isinstance(
                    pol.action_space_struct, (tuple, dict)
                ) and not pol.config.get("_disable_action_flattening"):
                    raise ValueError(error_msg)
                batch[SampleBatch.ACTIONS] = normalize_action(
                    batch[SampleBatch.ACTIONS], pol.action_space_struct
                )
            else:
                for pid, b in batch.policy_batches.items():
                    pol = self.policy_map[pid]
                    if isinstance(
                        pol.action_space_struct, (tuple, dict)
                    ) and not pol.config.get("_disable_action_flattening"):
                        raise ValueError(error_msg)
                    b[SampleBatch.ACTIONS] = normalize_action(
                        b[SampleBatch.ACTIONS],
                        self.ioctx.worker.policy_map[pid].action_space_struct,
                    )
        return batch

    def _next_line(self) -> str:
        if not self.cur_file:
            self.cur_file = self._next_file()
        line = self.cur_file.readline()
        tries = 0
        while not line and tries < 100:
            tries += 1
            if hasattr(self.cur_file, "close"):  # legacy smart_open impls
                self.cur_file.close()
            self.cur_file = self._next_file()
            line = self.cur_file.readline()
            if not line:
                logger.debug("Ignoring empty file {}".format(self.cur_file))
        if not line:
            raise ValueError(
                "Failed to read next line from files: {}".format(self.files)
            )
        return line

    def _next_file(self) -> FileType:
        # If this is the first time, we open a file, make sure all workers
        # start with a different one if possible.
        if self.cur_file is None and self.ioctx.worker is not None:
            idx = self.ioctx.worker.worker_index
            total = self.ioctx.worker.num_workers or 1
            path = self.files[round((len(self.files) - 1) * (idx / total))]
        # After the first file, pick all others randomly.
        else:
            path = random.choice(self.files)
        return self._try_open_file(path)

    def _from_json(self, data: str) -> SampleBatchType:
        if isinstance(data, bytes):  # smart_open S3 doesn't respect "r"
            data = data.decode("utf-8")
        json_data = json.loads(data)
        return from_json_data(json_data, self.ioctx.worker)
