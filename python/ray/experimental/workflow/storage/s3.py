import functools
import tempfile
import json
import urllib.parse as parse
import aioboto3
import itertools
import ray
from typing import Any, Dict, Callable
from ray.experimental.workflow.common import StepID
from ray.experimental.workflow.storage.base import (
    Storage, ArgsType, StepStatus, DataLoadError, DataSaveError)
# constants used in filesystem
OBJECTS_DIR = "objects"
STEPS_DIR = "steps"
STEP_INPUTS_METADATA = "inputs.json"
STEP_OUTPUTS_METADATA = "outputs.json"
STEP_ARGS = "args.pkl"
STEP_OUTPUT = "output.pkl"
STEP_FUNC_BODY = "func_body.pkl"
CLASS_BODY = "class_body.pkl"

MAX_RECEIVED_DATA_MEMORY_SIZE = 25 * 1024 * 1024  # 25MB


def data_load_error(func):
    @functools.wraps(func)
    async def _func(*args, **kvargs):
        try:
            ret = await func(*args, **kvargs)
            return ret
        except Exception as e:
            raise DataLoadError from e

    return _func


def data_save_error(func):
    @functools.wraps(func)
    async def _func(*args, **kv_args):
        try:
            ret = await func(*args, **kv_args)
            return ret
        except Exception as e:
            raise DataSaveError from e

    return _func


class S3StorageImpl(Storage):
    def __init__(self,
                 bucket: str,
                 s3_path: str,
                 region_name=None,
                 endpoint_url=None,
                 aws_access_key_id=None,
                 aws_secret_access_key=None,
                 aws_session_token=None,
                 config=None):
        if not isinstance(bucket, str):
            raise ValueError("bucket_name must be str")
        if not isinstance(s3_path, str):
            raise ValueError("s3_path must be str")

        self._bucket = bucket
        self._s3_path = s3_path
        self._session = aioboto3.Session()
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._config = config

    @data_load_error
    async def load_step_input_metadata(self, workflow_id: str,
                                       step_id: StepID) -> Dict[str, Any]:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_INPUTS_METADATA)
        data = await self._get_object(path, True)
        return data

    @data_save_error
    async def save_step_input_metadata(self, workflow_id: str, step_id: StepID,
                                       metadata: Dict[str, Any]) -> None:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_INPUTS_METADATA)
        await self._put_object(path, metadata, True)

    @data_load_error
    async def load_step_output_metadata(self, workflow_id: str,
                                        step_id: StepID) -> Dict[str, Any]:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_OUTPUTS_METADATA)
        data = await self._get_object(path, True)
        return data

    @data_save_error
    async def save_step_output_metadata(self, workflow_id: str,
                                        step_id: StepID,
                                        metadata: Dict[str, Any]) -> None:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_OUTPUTS_METADATA)
        await self._put_object(path, metadata, True)

    @data_load_error
    async def load_step_output(self, workflow_id: str, step_id: StepID) -> Any:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id, STEP_OUTPUT)
        data = await self._get_object(path)
        return data

    @data_save_error
    async def save_step_output(self, workflow_id: str, step_id: StepID,
                               output: Any) -> None:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id, STEP_OUTPUT)
        await self._put_object(path, output)

    @data_load_error
    async def load_step_func_body(self, workflow_id: str,
                                  step_id: StepID) -> Callable:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_FUNC_BODY)
        data = await self._get_object(path)
        return data

    @data_save_error
    async def save_step_func_body(self, workflow_id: str, step_id: StepID,
                                  func_body: Callable) -> None:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id,
                                 STEP_FUNC_BODY)
        await self._put_object(path, func_body)

    @data_load_error
    async def load_step_args(self, workflow_id: str,
                             step_id: StepID) -> ArgsType:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id, STEP_ARGS)
        data = await self._get_object(path)
        return data

    @data_save_error
    async def save_step_args(self, workflow_id: str, step_id: StepID,
                             args: ArgsType) -> None:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id, STEP_ARGS)
        await self._put_object(path, args)

    @data_load_error
    async def load_object_ref(self, workflow_id: str,
                              object_id) -> ray.ObjectRef:
        path = self._get_s3_path(workflow_id, OBJECTS_DIR, object_id)
        data = await self._get_object(path)
        return ray.put(data)  # simulate an ObjectRef

    @data_save_error
    async def save_object_ref(self, workflow_id: str,
                              obj_ref: ray.ObjectRef) -> None:
        path = self._get_s3_path(workflow_id, OBJECTS_DIR, obj_ref.hex())
        data = await obj_ref
        await self._put_object(path, data)

    @data_load_error
    async def get_step_status(self, workflow_id: str,
                              step_id: StepID) -> StepStatus:
        path = self._get_s3_path(workflow_id, STEPS_DIR, step_id) + "/"
        async with self._client() as s3:
            response = await s3.list_objects_v2(
                Bucket=self._bucket, Prefix=path)
            keys = set({
                item["Key"].split("/")[-1]
                for item in response.get("Contents", [])
            })
            return StepStatus(
                output_object_exists=(STEP_OUTPUT in keys),
                output_metadata_exists=(STEP_OUTPUTS_METADATA in keys),
                input_metadata_exists=(STEP_INPUTS_METADATA in keys),
                args_exists=(STEP_ARGS in keys),
                func_body_exists=(STEP_FUNC_BODY in keys))

    async def _put_object(self, path: str, data: Any,
                          is_json: bool = False) -> None:
        with tempfile.SpooledTemporaryFile(
                mode="w+b",
                max_size=MAX_RECEIVED_DATA_MEMORY_SIZE) as tmp_file:
            if is_json:
                tmp_file.write(json.dumps(data).encode())
            else:
                ray.cloudpickle.dump(data, tmp_file)
            tmp_file.seek(0)
            async with self._client() as s3:
                await s3.upload_fileobj(tmp_file, self._bucket, path)

    async def _get_object(self, path: str, is_json: bool = False) -> Any:
        with tempfile.SpooledTemporaryFile(
                mode="w+b",
                max_size=MAX_RECEIVED_DATA_MEMORY_SIZE) as tmp_file:
            async with self._client() as s3:
                obj = await s3.get_object(Bucket=self._bucket, Key=path)
                async for chunk in obj["Body"]:
                    tmp_file.write(chunk)
            tmp_file.seek(0)
            if is_json:
                return json.loads(tmp_file.read().decode())
            else:
                return ray.cloudpickle.load(tmp_file)

    def _client(self):
        return self._session.client(
            "s3",
            region_name=self._region_name,
            endpoint_url=self._endpoint_url,
            aws_access_key_id=self._aws_access_key_id,
            aws_secret_access_key=self._aws_secret_access_key,
            aws_session_token=self._aws_session_token,
            config=self._config)

    @property
    def storage_url(self) -> str:
        params = [("region_name", self._region_name), ("endpoint_url",
                                                       self._endpoint_url),
                  ("aws_access_key_id", self._aws_access_key_id),
                  ("aws_secret_access_key",
                   self._aws_secret_access_key), ("aws_session_token",
                                                  self._aws_session_token)]
        params = "&".join(
            ["=".join(param) for param in params if param[1] is not None])
        parsed_url = parse.ParseResult(
            scheme="s3",
            netloc=self._bucket,
            path=self._s3_path,
            params="",
            query=params,
            fragment="")
        return parse.urlunparse(parsed_url)

    def _get_s3_path(self, *args) -> str:
        return "/".join(itertools.chain([self._s3_path], args))

    @data_load_error
    async def load_actor_class_body(self, workflow_id: str) -> type:
        path = self._get_s3_path(workflow_id, CLASS_BODY)
        data = await self._get_object(path)
        return data

    @data_save_error
    async def save_actor_class_body(self, workflow_id: str, cls: type) -> None:
        path = self._get_s3_path(workflow_id, CLASS_BODY)
        await self._put_object(path, cls)

    def __reduce__(self):
        return S3StorageImpl, (self._bucket, self._s3_path, self._region_name,
                               self._endpoint_url, self._aws_access_key_id,
                               self._aws_secret_access_key,
                               self._aws_session_token, self._config)
