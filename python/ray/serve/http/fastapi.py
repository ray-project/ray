import inspect
from typing import Any, Callable, Dict

from fastapi import Depends, FastAPI

from ray.serve.exceptions import RayServeException
from ray.serve.http.asgi import ASGIWrapper

FASTAPI_METHODS = [
    "GET", "PUT", "POST", "DELETE", "OPTIONS", "HEAD", "PATCH", "TRACE"
]
ROUTE_METADATA_ATTR = "__ray_serve_fastapi_route_metadata"

GENERATED_METHOD_DOCSTRING = """
Adds a {method_upper} route to the wrapped FastAPI app.

This supports the same arguments as the `@app.{method_lower}` decorator of a
regular FastAPI app (`fastapi.FastAPI`).

This can only be used on a class that also inherits from {wrapper_class_name}
The route will be added to the app when the {wrapper_class_name} constructor
is called using `super().__init__(app)`.
"""


class FastAPIRouteMetadata:
    def __init__(self, method: str, path: str, func: Callable,
                 kwargs: Dict[Any, Any]):
        self._path = path
        self._func = func
        self._kwargs = kwargs
        self._kwargs["methods"] = [method]

    def add_to_fastapi_app(self, app: FastAPI):
        app.add_api_route(self._path, self._func, **self._kwargs)


class FastAPIWrapper(ASGIWrapper):
    def __init__(self, app: FastAPI):
        self._add_method_routes_to_app(app)
        super().__init__(app)

    def _add_method_route_to_app(self, app: FastAPI, method: Callable):
        # This block just adds a default values to the self parameters so that
        # FastAPI knows to inject the object when calling the route.
        # Before: def method(self, i): ...
        # After: def method(self=self, *, i):...
        old_signature = inspect.signature(method)
        old_parameters = list(old_signature.parameters.values())
        if len(old_parameters) == 0:
            # TODO(simon): make it more flexible to support no arguments.
            raise RayServeException(
                "Methods in FastAPI class-based view must have ``self`` as "
                "their first argument.")

        old_self_parameter = old_parameters[0]
        # XXX: why do we need Depends?
        # Just passing self gives recursion depth exceeded.
        new_self_parameter = old_self_parameter.replace(
            default=Depends(lambda: self))
        new_parameters = [new_self_parameter] + [
            # Make the rest of the parameters keyword only because
            # the first argument is no longer positional.
            parameter.replace(kind=inspect.Parameter.KEYWORD_ONLY)
            for parameter in old_parameters[1:]
        ]
        new_signature = old_signature.replace(parameters=new_parameters)
        setattr(method, "__signature__", new_signature)

        route_metadata: FastAPIRouteMetadata = getattr(method,
                                                       ROUTE_METADATA_ATTR)
        route_metadata.add_to_fastapi_app(app)

    def _add_method_routes_to_app(self, app: FastAPI):
        assert isinstance(app, FastAPI)

        for attribute_name in dir(self.__class__):
            attribute = getattr(self.__class__, attribute_name)
            if hasattr(attribute, ROUTE_METADATA_ATTR):
                self._add_method_route_to_app(app, attribute)

    @classmethod
    def _add_method_decorator(cls, method: str):
        def method_decorator(cls, path: str, **kwargs):
            if callable(path):
                method_str = f"{cls.__name__}.{method.lower()}"
                raise ValueError(f"{method_str} does not support being called "
                                 "with no arguments. You must at least "
                                 "provide a route, e.g., "
                                 f"@{method_str}(\"/\").")
            elif not isinstance(path, str):
                raise TypeError("Path argument must be a string.")

            def inner_decorator(func: Callable):
                print(f"ADDING ATTR TO {func}")
                setattr(func, ROUTE_METADATA_ATTR,
                        FastAPIRouteMetadata(method, path, func, kwargs))
                return func

            return inner_decorator

        method_decorator.__name__ = method.lower()
        method_decorator.__doc__ = GENERATED_METHOD_DOCSTRING.format(
            method_upper=method,
            method_lower=method.lower(),
            wrapper_class_name=cls.__name__)
        setattr(cls, method.lower(), classmethod(method_decorator))


for method in FASTAPI_METHODS:
    FastAPIWrapper._add_method_decorator(method)
