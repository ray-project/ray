import json

from collections import namedtuple


class ValidationError(Exception):
    pass


Field = namedtuple("Field", ["required", "default", "type"])


class BaseModel:
    """Base class to define schema.

    Model schema should be defined in class variable `__schema__`
    within a child class. `__schema__` should be a dictionary that contains
    `field`: `Field(
        required=required: bool,
        default=default: Any,
        type=type: type
    )`
    See the example below for more details.

    The class can have unexpected behavior if you don't follow the
    schema pattern properly.

    Example:
        class A(BaseModel):
            __schema__ = {
                "field_name": Field(
                    required=[True|False],
                    default=[default],
                    type=[type]
                ),
                "cluster_id": Field(
                    required=True,
                    default="1234",
                    type=str
                ),
            }

    Raises:
        ValidationError: Raised if a given arg doesn't satisfy the schema.
    """

    def __init__(self, **kwargs):
        self._dict = kwargs
        for key, value in kwargs.items():
            setattr(self, key, value)

    def __str__(self):
        name = "{}\n".format(self.__class__.__name__)
        return name + str(self._dict)

    def json(self):
        return json.dumps(self._dict)

    @classmethod
    def parse_obj(cls, obj):
        # Validation.
        assert type(obj) == dict, ("It can only parse dict type object, "
                                   "but {} type is given.".format(type(obj)))
        for field, schema in cls.__schema__.items():
            required, default, arg_type = schema
            if field not in obj:
                if required:
                    raise ValidationError("{} is required, but doesn't "
                                          "exist in a given object {}".format(
                                              field, obj))
                else:
                    # Set default value if the field is optional
                    obj[field] = default

        return cls(**obj)


class IngestRequest(BaseModel):
    __schema__ = {
        "ray_config": Field(required=True, default=None, type=tuple),
        "node_info": Field(required=True, default=None, type=dict),
        "raylet_info": Field(required=True, default=None, type=dict),
        "tune_info": Field(required=True, default=None, type=dict),
        "tune_availability": Field(required=True, default=None, type=dict)
    }


class IngestResponse(BaseModel):
    __schema__ = {
        "succeed": Field(required=True, default=None, type=bool),
        "actions": Field(required=False, default=[], type=list)
    }


class AuthRequest(BaseModel):
    __schema__ = {"cluster_id": Field(required=True, default=None, type=str)}


class AuthResponse(BaseModel):
    __schema__ = {
        "access_token_dashboard": Field(required=True, default=None, type=str),
        "access_token_ingest": Field(required=True, default=None, type=str)
    }


# Enum is not used because action types will be received
# through a network communication, and it will be string.
class ActionType:
    KILL_ACTOR = "KILL_ACTOR"


class KillAction(BaseModel):
    __schema__ = {
        "type": Field(required=False, default=ActionType.KILL_ACTOR, type=str),
        "actor_id": Field(required=True, default=None, type=str),
        "ip_address": Field(required=True, default=None, type=str),
        "port": Field(required=True, default=None, type=int)
    }
