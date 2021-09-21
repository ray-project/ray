import asyncio
import nest_asyncio
import tempfile
import json
import urllib.parse as parse
from botocore.exceptions import ClientError
import aioboto3
import itertools
import io
import ray
from typing import Any, Callable, List
from ray.workflow.storage.base import Storage, KeyNotFoundError
import ray.cloudpickle

nest_asyncio.apply()

MAX_RECEIVED_DATA_MEMORY_SIZE = 25 * 1024 * 1024  # 25MB


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
        self._s3_path.rstrip("/")
        if len(self._s3_path) == 0:
            raise ValueError(f"s3 path {self._s3_path} invalid")
        self._session = aioboto3.Session(
            region_name=region_name,
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            aws_session_token=aws_session_token)
        self._region_name = region_name
        self._endpoint_url = endpoint_url
        self._aws_access_key_id = aws_access_key_id
        self._aws_secret_access_key = aws_secret_access_key
        self._aws_session_token = aws_session_token
        self._config = config

    def make_key(self, *names: str) -> str:
        return "/".join(itertools.chain([self._s3_path], names))

    async def _upload_obj(self, key: str, fobj: io.BufferedIOBase) -> None:
        async with self._client() as s3:
            await s3.upload_fileobj(fobj, self._bucket, key)

    async def put(self, key: str, data: Any, is_json: bool = False) -> None:
        with tempfile.SpooledTemporaryFile(
                mode="w+b",
                max_size=MAX_RECEIVED_DATA_MEMORY_SIZE) as tmp_file:
            if is_json:
                tmp_file.write(json.dumps(data).encode())
            else:
                ray.cloudpickle.dump(data, tmp_file)
            tmp_file.seek(0)
            await self._upload_obj(key, tmp_file)

    async def _download_obj(self, key: str, fobj: io.BufferedIOBase) -> None:
        async with self._client() as s3:
            obj = await s3.get_object(Bucket=self._bucket, Key=key)
            async for chunk in obj["Body"]:
                fobj.write(chunk)
        fobj.seek(0)

    async def get(self, key: str, is_json: bool = False) -> Any:
        try:
            with tempfile.SpooledTemporaryFile(
                    mode="w+b",
                    max_size=MAX_RECEIVED_DATA_MEMORY_SIZE) as tmp_file:
                await self._download_obj(key, tmp_file)
                if is_json:
                    return json.loads(tmp_file.read().decode())
                else:
                    return ray.cloudpickle.load(tmp_file)
        except ClientError as ex:
            if ex.response["Error"]["Code"] == "NoSuchKey":
                raise KeyNotFoundError from ex
            else:
                raise

    def open(self, key: str, mode: str = "r") -> io.BufferedIOBase:
        assert "+" not in mode, "Cannot open file for both reading and writing."
        if "w" in mode:
            def onclose(bio):
                bio.seek(0)
                coro = self._upload_obj(key, bio)
                loop = asyncio.get_event_loop()
                loop.run_until_complete(coro)

            # TODO (Alex): Use an on-disk buffer avoid wasting memory.
            return _BytesIOWithCallback(onclose)
        else:
            tmp_file = tempfile.SpooledTemporaryFile(
                    mode="w+b",
                    max_size=MAX_RECEIVED_DATA_MEMORY_SIZE)
            coro = self._download_obj(key, tmp_file)
            loop = asyncio.get_event_loop()
            loop.run_until_complete(coro)
            return tmp_file

    async def delete_prefix(self, key_prefix: str) -> None:
        async with self._session.resource(
                "s3", endpoint_url=self._endpoint_url,
                config=self._config) as s3:
            bucket = await s3.Bucket(self._bucket)
            await bucket.objects.filter(Prefix=key_prefix).delete()

    async def scan_prefix(self, key_prefix: str) -> List[str]:
        keys = []
        async with self._client() as s3:
            if not key_prefix.endswith("/"):
                key_prefix += "/"
            paginator = s3.get_paginator("list_objects")
            operation_parameters = {
                "Bucket": self._bucket,
                "Delimiter": "/",
                "Prefix": key_prefix
            }
            page_iterator = paginator.paginate(**operation_parameters)
            async for page in page_iterator:
                for o in page.get("CommonPrefixes", []):  # "directories"
                    keys.append(o.get("Prefix", ""))
                for o in page.get("Contents", []):  # "files"
                    keys.append(o.get("Key", ""))
        keys = [k.rstrip("/").split("/")[-1] for k in keys if k != ""]
        return keys

    def _client(self):
        return self._session.client(
            "s3", endpoint_url=self._endpoint_url, config=self._config)

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

    def __reduce__(self):
        return S3StorageImpl, (self._bucket, self._s3_path, self._region_name,
                               self._endpoint_url, self._aws_access_key_id,
                               self._aws_secret_access_key,
                               self._aws_session_token, self._config)


class _BytesIOWithCallback(io.BytesIO):
    def __init__(self, callback: Callable[[io.BytesIO], None], *args,
                 **kwargs):
        self._callback = callback

    def close(self):
        if self._callback:
            self._callback(self)
            self._callback = None
        super().close()
