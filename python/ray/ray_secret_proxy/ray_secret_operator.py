from typing import Any, List, Dict
from urllib import response
from ray.util.annotations import DeveloperAPI, PublicAPI
from ray_secret import RaySecret

import boto3
from google.oauth2 import service_account
import base64
from botocore.exceptions import ClientError
import warnings

from google.cloud import secretmanager
from google.api_core.exceptions import ClientError

import logging

logger = logging.getLogger(__file__)

@DeveloperAPI
class RaySecretOperator:
    def __init__(self, **kwargs) -> None:
        raise NotImplementedError

    def initialize(self) -> None:
        raise NotImplementedError

    def get_secret(self, secret_name: str, ttl=None, **kwargs) -> RaySecret:
        raise NotImplementedError

    def list_secrets(self, filter=Any) -> List[str]:
        raise NotImplementedError



@PublicAPI
class AWSRaySecretOperator(RaySecretOperator):
    def __init__(self, **kwargs) -> None:
        self.__credentials = kwargs
        return

    def initialize(self) -> None:
        self.__client = boto3.client("secretsmanager", **self.__credentials)
        return

    def get_secret(self, secret_name: str, ttl=-1, **kwargs) -> RaySecret:
        try:
            kwargs["SecretId"] = secret_name
            response = self.__client.get_secret_value(**kwargs)
        except ClientError as e:
            if e.response["Error"]["Code"] == "DecryptionFailureException":
                # Secrets Manager can't decrypt the protected secret text using the provided KMS key.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InternalServiceErrorException":
                # An error occurred on the server side.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidParameterException":
                # You provided an invalid value for a parameter.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "InvalidRequestException":
                # You provided a parameter value that is not valid for the current state of the resource.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            elif e.response["Error"]["Code"] == "ResourceNotFoundException":
                # We can't find the resource that you asked for.
                # Deal with the exception here, and/or rethrow at your discretion.
                raise e
            else:
                warnings.warn( e )
        else:
            # Decrypts secret using the associated KMS key.
            # Depending on whether the secret is a string or binary, one of these fields will be populated.
            if "SecretString" in response:
                secret = response.pop("SecretString")
            else:
                secret = base64.b64decode(response.pop("SecretBinary"))

            secret_name = response.pop("Name")
            response.pop("ResponseMetadata", None)

            return RaySecret(
                secret_name=secret_name, secret=secret, ttl=ttl, metadata=response
            )

    def list_secrets(self, filter=None) -> List[str]:
        try:
            if filter is None:
                secret_list = self.__client.list_secrets()["SecretList"]
            else:
                secret_list = self.__client.list_secrets(Filters=filter)["SecretList"]

            return [secret["Name"] for secret in secret_list]
        except ClientError as e:
            raise e

@PublicAPI
class GCPRaySecretOperator(RaySecretOperator):
    def __init__(self, project_id: str, **kwargs) -> None:
        self.__project_id = project_id
        self.__credentials = kwargs
        return

    def initialize(self) -> None:
        if "credentials" in self.__credentials:
            creds = service_account.Credentials.from_service_account_info(
                self.__credentials["credentials"]
            )
            self.__client = secretmanager.SecretManagerServiceClient(credentials=creds)
        else:
            self.__client = secretmanager.SecretManagerServiceClient()
        return

    def get_secret(self, secret_name: str, ttl=-1, **kwargs) -> RaySecret:
        version = "latest" if "version" not in kwargs else kwargs["version"]

        if f"projects/{self.__project_id}/secrets/" not in secret_name:
            secret_name = f"projects/{self.__project_id}/secrets/{secret_name}"

        if "/versions/" not in secret_name:
            secret_name = f"{secret_name}/versions/{version}"

        try:
            response = self.__client.access_secret_version(name=secret_name)

            secret = response.payload.data.decode("UTF-8")
            response.payload.data = None
            return RaySecret(
                secret_name=secret_name, secret=secret, ttl=ttl, metadata=response.payload
            )
        except ClientError as e:
            raise e


    def list_secrets(self, filter=None) -> List[str]:
        parent = f"projects/{self.__project_id}"

        try:
            if filter is None:
                secret_list = self.__client.list_secrets(request={"parent": parent})
            else:
                secret_list = self.__client.list_secrets(
                    request={"parent": parent, "filter": filter}
                )

            return [secret.name for secret in secret_list]
        except ClientError as e:
            raise e