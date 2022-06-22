from ray.ray_secret_proxy.ray_secret import RaySecret
from ray.ray_secret_proxy.ray_secret_operator import AWSRaySecretOperator, RaySecretOperator
from typing import Any, List, Dict
from time import time
import ray

import logging

logger = logging.getLogger(__file__)

@ray.remote
class RaySecretProxy:
    def __init__(self, ray_secret_operator: RaySecretOperator, default_ttl=-1) -> None:
        ray_secret_operator.initialize()
        self.__ray_secret_operator = ray_secret_operator
        self.default_ttl = default_ttl
        self.__vault = {}
        return

    def hydrate_cache(self, secret_names: List[str] = None, filter=None) -> None:
        if self.default_ttl == 0:
            self.__vault = {}
            return
        
        secret_list = secret_names if secret_names is not None else self.list_secrets(filter)

        _ = [self.get_secret(secret_name=secret, ttl=self.default_ttl) for secret in secret_list]
        return

    def purge_cache(self) -> None:
        del self.__vault 
        self.__vault = {}
        return 

    def purge_secret(self, secret_name:str) -> None:
        if secret_name in self.__vault:
            del self.__vault[secret_name]
        return

    def get_secret(self,secret_name:str, ttl=None, **kwargs) -> RaySecret:
        ttl = self.default_ttl if ttl is None else ttl

        if ttl != 0 and (secret_name in self.__vault and not self.__vault[secret_name].is_expired()):
            logger.info(f"Cache hit: Secret {secret_name} retrieved from cache")
            secret = self.__vault[secret_name]
        else:            
            logger.info(f"Cache miss: Secret {secret_name} retrieved from source")
            secret = self.__ray_secret_operator.get_secret(secret_name=secret_name, ttl=ttl, **kwargs)

        if self.default_ttl != 0 and ttl != 0:
            self.__vault[secret.secret_name] = secret

        return secret

    def list_secrets(self,filter=None) -> List[str]:
        return self.__ray_secret_operator.list_secrets(filter)

    def list_cached_secrets(self) -> List[str]:
        return list(self.__vault.keys())