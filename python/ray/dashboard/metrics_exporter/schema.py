import json


class ValidationError(Exception):
    pass


class BaseModel:
    """Base class to define schema.

    This will raise ValidationError if
    - Number of given kwargs are bigger than needed.
    - Number of given kwargs are smaller than needed.

    This doesn't
    - Validate types.
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
        assert type(obj) == dict, ("It can only parse dict type object.")
        required_args = cls.__slots__
        given_args = obj.keys()

        # Check if given_args have args that is not required.
        for arg in given_args:
            if arg not in required_args:
                raise ValidationError(
                    "Given argument has a key {}, which is not required "
                    "by this schema: {}".format(arg, required_args))

        # Check if given args have all required args.
        if len(required_args) != len(given_args):
            raise ValidationError("Given args: {} doesn't have all the "
                                  "necessary args for this schema: {}".format(
                                      given_args, required_args))

        return cls(**obj)


class IngestRequest(BaseModel):
    __slots__ = [
        "cluster_id", "access_token", "ray_config", "node_info", "raylet_info",
        "tune_info", "tune_availability"
    ]


# TODO(sang): Add piggybacked response.
class IngestResponse(BaseModel):
    pass


class AuthRequest(BaseModel):
    __slots__ = ["cluster_id"]


class AuthResponse(BaseModel):
    __slots__ = ["dashboard_url", "access_token"]
