import jsonschema
import logging
from typing import List
import json

logger = logging.getLogger(__name__)


class RuntimeEnvPluginSchemaManager:
    """ This mananger is used to load plugin json schemas.
    """
    schemas = {}

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
    def validate(cls, title, instance):
        if title not in cls.schemas:
            raise TypeError(f"Invalid plugin name {title}")
        jsonschema.validate(instance=instance, schema=cls.schemas[title])
