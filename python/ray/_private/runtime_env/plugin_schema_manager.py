import os
import jsonschema
import logging
from typing import List
import json
from ray._private.runtime_env.constants import RAY_RUNTIME_ENV_PLUGINS_SCHEMAS_ENV_VAR

logger = logging.getLogger(__name__)


class RuntimeEnvPluginSchemaManager:
    """This mananger is used to load plugin json schemas."""

    schemas = {}
    loaded = False

    @classmethod
    def load_schemas(cls, schema_paths: List[str]):
        """ """
        for schema_path in schema_paths:
            schema = json.load(open(schema_path))
            if "title" not in schema:
                logger.error("No valid title in %s", schema_path)
                continue
            if schema["title"] in cls.schemas:
                logger.error(
                    "The schema %s 'title' conflicts with %s",
                    schema_path,
                    cls.schemas[schema["title"]],
                )
                continue
            cls.schemas[schema["title"]] = schema

    @classmethod
    def load_schemas_from_env_var(cls):
        schema_paths = os.environ.get(RAY_RUNTIME_ENV_PLUGINS_SCHEMAS_ENV_VAR)
        if schema_paths:
            schema_json_files = list()
            for path in schema_paths.split(","):
                if path.endswith("schema.json"):
                    schema_json_files.append(path)
                elif os.path.isdir(path):
                    for root, _, files in os.walk(path):
                        for f in files:
                            if f.endswith("schema.json"):
                                schema_json_files.append(os.path.join(root, f))
            logger.info(f"Loading the runtime env schemas: {schema_json_files}")
            cls.load_schemas(schema_json_files)

    @classmethod
    def validate(cls, name, instance):
        if not cls.loaded:
            # Load the schemas lazily.
            cls.load_schemas_from_env_var()
        if name in cls.schemas:
            jsonschema.validate(instance=instance, schema=cls.schemas[name])
