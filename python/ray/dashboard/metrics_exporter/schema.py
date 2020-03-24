import pydantic
import typing

class IngestRequest(pydantic.BaseModel):
    cluster_id: str
    access_token: str
    ray_config: typing.Any
    node_info: dict
    raylet_info: dict
    tune_info: dict
    tune_availability: dict


# TODO(sang): Add piggybacked response.
class IngestResponse(pydantic.BaseModel):
    pass


class AuthRequest(pydantic.BaseModel):
    cluster_id: str


class AuthResponse(pydantic.BaseModel):
    dashboard_url: str
    access_token: str
