from typing import Dict, Optional, Tuple

WORKING_DIR = "working_dir"
PY_MODULES = "py_modules"

ZIP_EXTENSION = ".zip"
WHEEL_EXTENSION = ".whl"
JAR_EXTENSION = ".jar"
TAR_GZ_EXTENSION = ".tar.gz"
TGZ_EXTENSION = ".tgz"
TAR_BZ2_EXTENSION = ".tar.bz2"
TAR_XZ_EXTENSION = ".tar.xz"

# These are the user-facing formats accepted for RuntimeEnv package fields.
# Keep field-specific validation, local uploads, and Job package uploads derived
# from this mapping so they cannot silently diverge.
RUNTIME_ENV_PACKAGE_EXTENSIONS: Dict[str, Tuple[str, ...]] = {
    WORKING_DIR: (
        ZIP_EXTENSION,
        TAR_GZ_EXTENSION,
        TGZ_EXTENSION,
        TAR_XZ_EXTENSION,
    ),
    PY_MODULES: (
        ZIP_EXTENSION,
        WHEEL_EXTENSION,
        TAR_GZ_EXTENSION,
        TGZ_EXTENSION,
        TAR_XZ_EXTENSION,
    ),
}

PACKAGE_UPLOAD_EXTENSIONS = tuple(
    dict.fromkeys(
        extension
        for extensions in RUNTIME_ENV_PACKAGE_EXTENSIONS.values()
        for extension in extensions
    )
)

# .tar.bz2 is retained as a low-level download format for compatibility, but it
# is not part of the public working_dir or py_modules contract above.
TAR_EXTENSIONS = (
    TAR_GZ_EXTENSION,
    TGZ_EXTENSION,
    TAR_BZ2_EXTENSION,
    TAR_XZ_EXTENSION,
)
COMPOUND_ARCHIVE_EXTENSIONS = (
    TAR_GZ_EXTENSION,
    TAR_BZ2_EXTENSION,
    TAR_XZ_EXTENSION,
)


def get_package_extension(
    path: str, supported_extensions: Tuple[str, ...]
) -> Optional[str]:
    """Return the supported extension for a package path.

    Args:
        path: Package path or URI to inspect.
        supported_extensions: Extensions to match in any order, including compound
            extensions. If extensions overlap, the longest match takes precedence.

    Returns:
        The longest matching extension, or ``None`` if the path is unsupported.
    """
    for extension in sorted(supported_extensions, key=len, reverse=True):
        if path.endswith(extension):
            return extension
    return None


def has_package_extension(path: str, supported_extensions: Tuple[str, ...]) -> bool:
    """Return whether a package path ends in a supported extension."""
    return get_package_extension(path, supported_extensions) is not None


def validate_package_extension(
    path: str, field: str, display_path: Optional[str] = None
) -> None:
    """Validate a RuntimeEnv package path using the field's capabilities.

    Args:
        path: Package path to validate.
        field: RuntimeEnv field whose format capabilities apply.
        display_path: Optional sanitized user-facing path to include in validation
            errors. URI query parameters must be removed before passing it.

    Raises:
        ValueError: If the path does not have an extension supported by the field.
    """
    supported_extensions = RUNTIME_ENV_PACKAGE_EXTENSIONS[field]
    if has_package_extension(path, supported_extensions):
        return

    formats = ", ".join(supported_extensions)
    error_path = path if display_path is None else display_path
    raise ValueError(
        f"Only {formats} files are supported for {field} URIs; got {error_path}."
    )
