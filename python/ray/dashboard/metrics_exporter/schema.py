import json


class ValidationError(Exception):
    pass


class BaseModel:
    """Base class to define schema.

    Model schema should be defined in class variable `__schema__`
    within a child class. `__schema__` should be a dictionary that
    contains `field`: `(required: bool, default: Any, type: type)`
    See the example below for more details.

    The class can have unexpected behavior if you don't follow the
    schema pattern properly.

    Example:
        class A(BaseModel):
            __schema__ = {
                #             (REQUIRED,     DEFAULT,   TYPE)
                "field_name": ([True|False], [default], [type]),
                "cluster_id": (True, "1234", str),
                "port": (False, 80, int)
            }

    Raises:
        ValidationError: Raised if a given arg doesn't satisfy the schema.
    """
    definition = {}

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
        assert type(obj) == dict, ("It can only parse dict type object.")
        for field, schema in cls.__schema__.items():
            required, default, arg_type = schema
            if field not in obj:
                if required:
                    raise ValidationError(f"{field} is required, but doesn't "
                                          "exist in a given object {obj}")
                else:
                    # Set default value if the field is optional
                    obj[field] = default

        return cls(**obj)


"""
Request/Response
"""


class IngestRequest(BaseModel):
    __schema__ = {
        "cluster_id": (True, None, str),
        "access_token": (True, None, str),
        "ray_config": (True, None, tuple),
        "node_info": (True, None, dict),
        "raylet_info": (True, None, dict),
        "tune_info": (True, None, dict),
        "tune_availability": (True, None, dict)
    }


class IngestResponse(BaseModel):
    __schema__ = {"succeed": (True, None, bool), "actions": (False, [], list)}


class AuthRequest(BaseModel):
    __schema__ = {"cluster_id": (True, None, str)}


class AuthResponse(BaseModel):
    __schema__ = {
        "dashboard_url": (True, None, str),
        "access_token": (True, None, str)
    }


"""
Actions
"""


# Types
class ActionType:
    KILL_ACTOR = "KILL_ACTOR"


class KillAction(BaseModel):
    __schema__ = {
        "type": (False, ActionType.KILL_ACTOR, str),
        "actor_id": (True, None, str),
        "ip_address": (True, None, str),
        "port": (True, None, int)
    }
