from __future__ import absolute_import, division, print_function

from ray import Language
from ray._raylet import CppFunctionDescriptor, JavaFunctionDescriptor
from ray.util.annotations import PublicAPI

__all__ = [
    "java_function",
    "java_actor_class",
    "cpp_function",
]


@PublicAPI(stability="beta")
def java_function(class_name: str, function_name: str):
    """Define a Java function.

    Args:
        class_name: Java class name.
        function_name: Java function name.
    """
    from ray.remote_function import RemoteFunction

    return RemoteFunction(
        Language.JAVA,
        lambda *args, **kwargs: None,
        JavaFunctionDescriptor(class_name, function_name, ""),
        {},
    )


@PublicAPI(stability="beta")
def cpp_function(function_name: str):
    """Define a Cpp function.

    Args:
        function_name: Cpp function name.
    """
    from ray.remote_function import RemoteFunction

    return RemoteFunction(
        Language.CPP,
        lambda *args, **kwargs: None,
        CppFunctionDescriptor(function_name, "PYTHON"),
        {},
    )


@PublicAPI(stability="beta")
def java_actor_class(class_name: str):
    """Define a Java actor class.

    Args:
        class_name: Java class name.
    """
    from ray.actor import ActorClass

    return ActorClass._ray_from_function_descriptor(
        Language.JAVA,
        JavaFunctionDescriptor(class_name, "<init>", ""),
        {},
    )


@PublicAPI(stability="beta")
def cpp_actor_class(create_function_name: str, class_name: str):
    """Define a Cpp actor class.

    Args:
        create_function_name: Create cpp class function name.
        class_name: Cpp class name.
    """
    from ray.actor import ActorClass

    print("create func=", create_function_name, "class_name=", class_name)
    return ActorClass._ray_from_function_descriptor(
        Language.CPP,
        CppFunctionDescriptor(create_function_name, "PYTHON", class_name),
        {},
    )


def _format_args(worker, args, kwargs):
    """Format args for various languages.

    Args:
        worker: The global worker instance.
        args: The arguments for cross language.
        kwargs: The keyword arguments for cross language.

    Returns:
        List of args and kwargs (if supported).
    """
    if not worker.load_code_from_local:
        raise ValueError(
            "Cross language feature needs --load-code-from-local to be set."
        )
    if kwargs:
        raise TypeError("Cross language remote functions does not support kwargs.")
    return args


def _get_function_descriptor_for_actor_method(
    language: str, actor_creation_function_descriptor, method_name: str, signature: str
):
    """Get function descriptor for cross language actor method call.

    Args:
        language: Target language.
        actor_creation_function_descriptor:
            The function signature for actor creation.
        method_name: The name of actor method.
        signature: The signature for the actor method. When calling Java from Python,
            it should be string in the form of "{length_of_args}".

    Returns:
        Function descriptor for cross language actor method call.
    """
    if language == Language.JAVA:
        return JavaFunctionDescriptor(
            actor_creation_function_descriptor.class_name,
            method_name,
            signature,
        )
    elif language == Language.CPP:
        return CppFunctionDescriptor(
            method_name,
            "PYTHON",
            actor_creation_function_descriptor.class_name,
        )
    else:
        raise NotImplementedError(
            "Cross language remote actor method " f"not support language {language}"
        )
