"""A Modal ``App`` stand-in.

Modal requires an App to create a Sandbox, because an App is the unit that
owns and bills the resulting containers. Ray's sandboxes are owned by the
actor handle that created them, so nothing here needs a server-side object.

``Sandbox.create(app=...)`` accepts an App from :meth:`App.lookup` and ignores
it, as Modal code always passes one -- Modal requires it -- and nothing here
needs one. An App that was only constructed is refused, as Modal refuses one
that was never initialized. It is not a grouping mechanism: listing the
sandboxes of an App is not supported.
"""

from typing import Any, Dict, Optional, Sequence

from ray.experimental.sandbox.modal.exception import NotSupportedError
from ray.util.annotations import PublicAPI


def _reject(**kwargs) -> None:
    """Refuse a constructor argument that needs Modal's control plane."""
    for name, value in kwargs.items():
        if value:
            raise NotSupportedError(
                f"The '{name}' parameter of modal.App is not supported by the "
                f"Ray sandbox backend: an App here is a name and nothing else, "
                f"with no server-side object to attach {name} to."
            )


@PublicAPI(stability="alpha")
class App:
    """A named grouping accepted for Modal compatibility."""

    def __init__(
        self,
        name: Optional[str] = None,
        *,
        tags: Optional[Dict[str, str]] = None,
        image: Optional[Any] = None,
        secrets: Sequence[Any] = (),
        volumes: Optional[Dict[Any, Any]] = None,
        include_source: bool = True,
    ):
        """Create an App.

        Args:
            name: Name for the App. Unlike Modal's, it is not registered
                anywhere.
            tags: Unsupported.
            image: Unsupported.
            secrets: Unsupported.
            volumes: Unsupported.
            include_source: Accepted for compatibility; ignored, since nothing
                is built or uploaded here.

        Raises:
            NotSupportedError: An unsupported parameter was given.
        """
        _reject(tags=tags, image=image, secrets=secrets, volumes=volumes)
        self._name = name
        # Set by lookup(). Modal's App has no app_id until it is looked up or
        # run, and Sandbox.create refuses it until then.
        self._initialized = False

    @property
    def name(self) -> Optional[str]:
        """The App's name."""
        return self._name

    @property
    def app_id(self) -> Optional[str]:
        """The App's identifier: its name once looked up, else None, as on Modal."""
        return self._name if self._initialized else None

    @staticmethod
    def lookup(
        name: str,
        *,
        client: Optional[Any] = None,
        environment_name: Optional[str] = None,
        create_if_missing: bool = False,
    ) -> "App":
        """Return an App with the given name.

        There is no server-side registry, so this always succeeds and
        ``create_if_missing`` has no effect.

        Args:
            name: Name for the App.
            client: Accepted for compatibility; ignored.
            environment_name: Accepted for compatibility; ignored.
            create_if_missing: Accepted for compatibility; ignored.

        Returns:
            An :class:`App` with that name.
        """
        return _initialized_app(name)

    def __repr__(self) -> str:
        return f"App({self._name!r})"


async def _lookup_aio(
    name: str,
    *,
    client: Optional[Any] = None,
    environment_name: Optional[str] = None,
    create_if_missing: bool = False,
) -> App:
    """The ``.aio`` twin of :meth:`App.lookup`.

    ``App`` is a plain class rather than a ``synchronize_api`` product -- it
    does no I/O and has nothing to drive on a loop -- so the ``.aio`` Modal
    programs reach for has to be attached by hand.
    """
    return _initialized_app(name)


def _initialized_app(name: str) -> App:
    app = App(name)
    app._initialized = True
    return app


App.lookup.aio = _lookup_aio
