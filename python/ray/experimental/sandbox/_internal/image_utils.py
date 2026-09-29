import contextlib
import fcntl
import hashlib
import io
import json
import logging
import os
import platform
import re
import shutil
import stat
import tarfile
import tempfile
import time
import urllib.error
import urllib.parse
import urllib.request
import uuid
from typing import BinaryIO, Callable, Dict, Optional, Tuple, Union

from ray.experimental.sandbox.exceptions import SandboxCreationError

logger = logging.getLogger(__name__)

# Per-user, and created 0700. A shared path in /tmp is both a poisoning vector
# -- planting a rootfs plus its .extracted marker makes every later pull of that
# name short-circuit onto attacker content -- and, once one user creates it, an
# unusable directory for everyone else on the box.
DEFAULT_IMAGES_DIR = f"/tmp/ray-{os.getuid()}/sandbox/images"

# Scratch directories and partial files are cleaned up on the error paths that
# raise, but not when a process is killed outright. Anything older than this is
# from a build or pull that will never finish.
_STALE_TEMP_AGE_SECONDS = 6 * 60 * 60
# Image config metadata, stored at the root of an image's cache directory --
# beside `rootfs/`, not inside it.
IMAGE_CONFIG_FILENAME = ".image_config.json"
_USER_AGENT = "ray-sandbox/1.0 (python-urllib)"


def _registry_request(
    url: str, headers: Dict[str, str], auth_header: Optional[str] = None
) -> urllib.request.Request:
    """Build a registry request, keeping the bearer token off redirects.

    Registries answer blob GETs with a redirect to presigned object storage.
    urllib does not copy unredirected headers onto a redirected request, so
    marking the token this way drops it at the hop. Sending it onward would
    trip S3's ``400 InvalidArgument: Only one auth mechanism allowed``, since
    the presigned URL already carries its own signature.
    """
    req = urllib.request.Request(url, headers=headers)
    if auth_header:
        req.add_unredirected_header("Authorization", auth_header)
    return req


# Bumped when the cache key changes shape, so entries written by an older
# layout are missed rather than misread.
_CACHE_KEY_VERSION = "v2"


def sanitize_image_name(image: str) -> str:
    """Derive a stable, collision-free cache key for an image reference.

    The readable prefix is for humans reading the cache directory; the digest
    suffix is what makes the key unique.

    Sanitizing alone is not sufficient. It maps ``/``, ``:`` and ``@`` all to
    ``_``, so ``org/repo:v1`` and the distinct Docker Hub image ``org_repo_v1``
    collide; and it reduces a ``.tar`` path to its basename, so
    ``/a/rootfs.tar`` and ``/b/rootfs.tar`` share one directory -- where the
    mtime check then silently serves one the other's filesystem. A local tar
    could likewise land on the entry for a registry image of a similar name.
    """
    if not isinstance(image, str):
        raise TypeError(f"Expected image to be a string, got {type(image).__name__}")

    # Validate the raw reference, before normalization. parse_image_ref happily
    # turns "" into "library/:latest" and "..." into "library/...:latest", so a
    # check on the normalized form would no longer reject either -- the caller
    # would get a plausible-looking cache key and a confusing pull failure much
    # later instead of an error naming what they passed.
    if not re.search(r"[a-zA-Z0-9]", image):
        raise ValueError(f"Invalid image name '{image}': cannot be safely sanitized.")

    if image.endswith(".tar"):
        # Keyed on location, not content: the mtime check handles a rewritten
        # tar by re-extracting into this same entry, whereas keying on content
        # would strand the old directory on every edit.
        identity = f"file\0{os.path.realpath(image)}"
        readable = os.path.basename(image)[:-4]
    else:
        registry, repo, reference = parse_image_ref(image)
        # Normalized, so `busybox` and `docker.io/library/busybox:latest` share
        # one entry instead of being pulled twice. The readable half has to come
        # from the normalized form too -- deriving it from the raw string would
        # give those two spellings different directory names despite their
        # identical digests, which is the same cache miss by another route.
        identity = f"registry\0{registry}\0{repo}\0{reference}"
        readable = f"{repo}_{reference}"

    safe = re.sub(r"[^a-zA-Z0-9_.-]", "_", readable).lstrip(".")
    if not safe:
        raise ValueError(f"Invalid image name '{image}': cannot be safely sanitized.")

    digest = hashlib.sha256(
        f"{_CACHE_KEY_VERSION}\0{identity}".encode("utf-8")
    ).hexdigest()
    return f"{safe[:48]}-{digest[:16]}"


_DOCKER_HUB_REGISTRIES = (
    "docker.io",
    "index.docker.io",
    "registry-1.docker.io",
    "registry.hub.docker.com",
)


def parse_image_ref(image_ref: str) -> Tuple[str, str, str]:
    """Parse image reference string into (registry, repository, tag_or_digest).

    Args:
        image_ref: Container image reference string (e.g. 'busybox:latest',
            'python:3.10-slim', 'docker.io/library/python:3.12-slim',
            'ghcr.io/org/repo:1.0').

    Returns:
        Tuple of (registry, repository, tag_or_digest).
    """
    tag = "latest"
    if "@" in image_ref:
        image_ref, tag = image_ref.split("@", 1)
    elif ":" in image_ref and not image_ref.endswith(":"):
        parts = image_ref.rsplit(":", 1)
        if "/" not in parts[1]:
            image_ref, tag = parts[0], parts[1]

    parts = image_ref.split("/", 1)
    if len(parts) == 1:
        registry = "registry-1.docker.io"
        repo = f"library/{parts[0]}"
    elif "." in parts[0] or ":" in parts[0] or parts[0] == "localhost":
        if parts[0] in _DOCKER_HUB_REGISTRIES:
            registry = "registry-1.docker.io"
            repo = f"library/{parts[1]}" if "/" not in parts[1] else parts[1]
        else:
            registry = parts[0]
            repo = parts[1]
    else:
        registry = "registry-1.docker.io"
        repo = image_ref

    return registry, repo, tag


def get_platform_arch() -> str:
    """Return normalized CPU architecture matching OCI/Docker conventions."""
    mach = platform.machine().lower()
    if mach in ("x86_64", "amd64"):
        return "amd64"
    if mach in ("aarch64", "arm64"):
        return "arm64"
    if mach in ("i386", "i686", "x86"):
        return "386"
    if mach.startswith("arm"):
        return "arm"
    return mach


def get_registry_auth_headers(
    registry: str,
    repo: str,
    reference: str = "latest",
    timeout: float = 30.0,
) -> Dict[str, str]:
    """Retrieve bearer authentication token headers for registry repository."""
    url = f"https://{registry}/v2/{repo}/manifests/{reference}"
    req = urllib.request.Request(url, headers={"User-Agent": _USER_AGENT})
    try:
        urllib.request.urlopen(req, timeout=timeout)
        return {}
    except urllib.error.HTTPError as err:
        if err.code != 401:
            return {}
        auth_hdr = err.headers.get("Www-Authenticate", "")
        if not re.match(r"^\s*Bearer\b", auth_hdr, re.IGNORECASE):
            return {}

        realm_m = re.search(r'realm=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE)
        if not realm_m:
            return {}
        realm = realm_m.group(1)
        # The realm is chosen by whoever answered the request. No credentials
        # are sent today, but the moment any are, an http:// or off-host realm
        # is a credential-exfiltration path -- so constrain it now.
        if urllib.parse.urlparse(realm).scheme != "https":
            logger.warning(
                f"Ignoring non-https authentication realm '{realm}' offered by "
                f"registry '{registry}'."
            )
            return {}

        service_m = re.search(
            r'service=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE
        )
        service = service_m.group(1) if service_m else None

        scope_m = re.search(r'scope=["\']?([^"\',\s]+)["\']?', auth_hdr, re.IGNORECASE)
        scope = scope_m.group(1) if scope_m else f"repository:{repo}:pull"

        params = {}
        if service:
            params["service"] = service
        if scope:
            params["scope"] = scope

        sep = "&" if "?" in realm else "?"
        auth_url = f"{realm}{sep}{urllib.parse.urlencode(params)}" if params else realm
        auth_req = urllib.request.Request(auth_url, headers={"User-Agent": _USER_AGENT})
        try:
            with urllib.request.urlopen(auth_req, timeout=timeout) as resp:
                data = json.loads(resp.read().decode("utf-8"))
                token = data.get("token") or data.get("access_token")
                if token:
                    return {"Authorization": f"Bearer {token}"}
        except Exception as auth_err:
            logger.warning(
                f"Failed to obtain registry auth token from '{auth_url}': {auth_err}"
            )
            return {}
    except Exception as err:
        logger.debug(f"Failed to query registry '{url}' for auth challenge: {err}")
        return {}
    return {}


def extract_tar_layer(
    tar_input: Union[bytes, io.IOBase, BinaryIO],
    dest_dir: str,
) -> None:
    """Extract a tar archive layer onto dest_dir with OCI whiteout handling.

    Args:
        tar_input: Archive bytes or a readable file object.
        dest_dir: Root filesystem directory to extract into.
    """
    if isinstance(tar_input, bytes):
        tar_fileobj = io.BytesIO(tar_input)
    else:
        tar_fileobj = tar_input

    with tarfile.open(fileobj=tar_fileobj, mode="r:*") as tar:
        for member in tar.getmembers():
            name = member.name.lstrip("/")

            # Prevent path traversal
            if ".." in name.split(os.sep) or name.startswith(os.sep):
                continue

            target_path = os.path.abspath(os.path.join(dest_dir, name))
            dest_abs = os.path.abspath(dest_dir)

            # Prevent symlink traversal
            dirname = os.path.dirname(name)
            parent_dir = os.path.abspath(os.path.join(dest_dir, dirname))
            real_parent_dir = os.path.realpath(parent_dir)
            dest_real = os.path.realpath(dest_dir)

            if not (
                target_path == dest_abs or target_path.startswith(dest_abs + os.sep)
            ) or not (
                real_parent_dir == dest_real
                or real_parent_dir.startswith(dest_real + os.sep)
            ):
                continue

            basename = os.path.basename(name)

            # Handle OCI opaque whiteout (.wh..wh..opq)
            if basename == ".wh..wh..opq":
                if os.path.exists(parent_dir):
                    for item in os.listdir(parent_dir):
                        item_path = os.path.join(parent_dir, item)
                        if os.path.isdir(item_path) and not os.path.islink(item_path):
                            shutil.rmtree(item_path, ignore_errors=True)
                        else:
                            try:
                                os.remove(item_path)
                            except OSError:
                                pass
                continue

            # Handle OCI deletion whiteout (.wh.<filename>)
            if basename.startswith(".wh."):
                del_name = basename[4:]
                del_path = os.path.join(parent_dir, del_name)
                if os.path.isdir(del_path) and not os.path.islink(del_path):
                    shutil.rmtree(del_path, ignore_errors=True)
                elif os.path.exists(del_path) or os.path.islink(del_path):
                    try:
                        os.remove(del_path)
                    except OSError:
                        pass
                continue

            # Remove conflicting existing file/dir if member type differs
            if os.path.exists(target_path) or os.path.islink(target_path):
                if not (os.path.isdir(target_path) and member.isdir()):
                    try:
                        if os.path.isdir(target_path) and not os.path.islink(
                            target_path
                        ):
                            shutil.rmtree(target_path, ignore_errors=True)
                        else:
                            os.remove(target_path)
                    except OSError:
                        pass
                else:
                    # If target_path is a directory or a symlink to an existing directory
                    # inside dest_dir, preserve it (e.g. UsrMerge /bin -> usr/bin).
                    pass

            # Use safe extraction
            member.name = name
            if member.isreg():
                os.makedirs(parent_dir, exist_ok=True)
                f_in = tar.extractfile(member)
                # Strip setuid/setgid. Ownership is not preserved, so these bits
                # would land on a host path owned by whoever runs the extraction
                # -- a setuid-root binary from a user-supplied image, executable
                # by any local user unless the cache happens to sit on a nosuid
                # mount.
                mode = (member.mode or 0o644) & ~(stat.S_ISUID | stat.S_ISGID)
                with open(target_path, "wb") as f_out:
                    if f_in:
                        shutil.copyfileobj(f_in, f_out)
                if member.mode:
                    os.chmod(target_path, mode)
            elif member.isdir():
                os.makedirs(target_path, exist_ok=True)
            elif member.issym():
                os.makedirs(parent_dir, exist_ok=True)
                try:
                    os.symlink(member.linkname, target_path)
                except OSError:
                    pass
            elif member.islnk():
                os.makedirs(parent_dir, exist_ok=True)
                # realpath, not abspath: abspath normalizes ".." but leaves
                # symlinks unresolved, and os.link follows them. A layer can
                # ship `escape -> /` and then a hardlink naming
                # `escape/etc/shadow`; that passes a lexical containment check
                # and hardlinks a host file into the rootfs, which is then
                # mounted into the sandbox. Resolving first closes that.
                link_target = os.path.realpath(
                    os.path.join(dest_dir, member.linkname.lstrip("/"))
                )
                if link_target == dest_real or link_target.startswith(
                    dest_real + os.sep
                ):
                    try:
                        os.link(link_target, target_path)
                    except OSError:
                        pass


_MANIFEST_ACCEPT = (
    "application/vnd.docker.distribution.manifest.v2+json, "
    "application/vnd.docker.distribution.manifest.list.v2+json, "
    "application/vnd.oci.image.manifest.v1+json, "
    "application/vnd.oci.image.index.v1+json"
)

# An index pointing at an index is legal; an unbounded chain is not.
_MAX_INDEX_DEPTH = 4

# Records which manifest a cached entry was built from, so a mutable tag can be
# revalidated instead of being pinned forever.
MANIFEST_DIGEST_FILENAME = ".manifest_digest"


def _assert_owned_by_us(path: str) -> None:
    """Refuse to use a cache directory belonging to somebody else.

    The directory decides which root filesystem a sandbox runs. If another user
    can write it, they choose what this one executes.
    """
    st = os.stat(path)
    if st.st_uid != os.getuid():
        raise SandboxCreationError(
            f"Image cache '{path}' is owned by uid {st.st_uid}, not {os.getuid()}. "
            f"Refusing to use it; set a different images_dir."
        )
    if st.st_mode & (stat.S_IWGRP | stat.S_IWOTH):
        raise SandboxCreationError(
            f"Image cache '{path}' is group- or world-writable "
            f"({stat.filemode(st.st_mode)}). Refusing to use it."
        )


def _sweep_stale_temporaries(images_dir: str) -> None:
    """Drop scratch left behind by a killed pull.

    Best-effort and never fatal: losing a sweep is far cheaper than failing a
    pull over it.
    """
    cutoff = time.time() - _STALE_TEMP_AGE_SECONDS
    try:
        entries = os.listdir(images_dir)
    except OSError:
        return
    for name in entries:
        if not (
            _STAGING_NAME.match(name)
            or _STAGED_LINK_NAME.match(name)
            or name.endswith(".partial")
        ):
            continue
        path = os.path.join(images_dir, name)
        try:
            if os.lstat(path).st_mtime >= cutoff:
                continue
            if os.path.isdir(path) and not os.path.islink(path):
                shutil.rmtree(path, ignore_errors=True)
            else:
                os.unlink(path)
        except OSError:
            continue


# The gVisor backend's layout: where it writes each sandbox's OCI bundle, and
# where runsc keeps each container's state. The cache reads both to tell which
# versions a running sandbox still uses.
SANDBOX_BUNDLES_DIR = "/tmp/ray/sandbox"
RUNSC_STATE_DIR = "/tmp/runsc"

# Every published image is an immutable version directory, "<key>.v.<32 hex>",
# and "<key>" is a symlink naming the current one. A sandbox's bundle names the
# version itself, so what it runs from cannot change under it: publishing a new
# version only repoints the link, and an old version is deleted once it has
# been retired for _RETIRED_GRACE_SECONDS and no live bundle names it.
#
# Matched exactly: a key is "<readable>-<16 hex>", and its readable half can
# itself contain ".old." or ".tmp." -- an image named "x.old.y" used to be
# swept as scratch.
_STAGING_NAME = re.compile(r"^.+\.tmp\.[0-9a-f]{32}$")
_STAGED_LINK_NAME = re.compile(r"^.+\.lnk\.[0-9a-f]{32}$")
_VERSION_NAME = re.compile(r"^(.+)\.v\.[0-9a-f]{32}$")
# An entry published before versions existed: a plain directory at "<key>",
# set aside under this name when replaced.
_LEGACY_NAME = re.compile(r"^(.+)\.old\.[0-9a-f]{32}$")

# Written into a version when it stops being current; its mtime says when.
_RETIRED_MARKER = ".retired"

# How long a retired version is kept even if no bundle names it. A sandbox
# that resolved the version just before it was retired may not have written
# its bundle yet, or its container may not have recorded its state yet; either
# way it would be invisible to the in-use check for those few seconds.
_RETIRED_GRACE_SECONDS = 10 * 60


def entry_identity(entry_dir: str) -> str:
    """Which version ``entry_dir`` resolves to right now, as ``dev:ino``."""
    st = os.stat(entry_dir)
    return f"{st.st_dev}:{st.st_ino}"


@contextlib.contextmanager
def _key_lock(images_dir: str, key: str, blocking: bool = True):
    """Hold the per-image lock publishing takes. Yields whether it was taken."""
    with open(os.path.join(images_dir, f"{key}.lock"), "a", encoding="utf-8") as f:
        try:
            fcntl.flock(f, fcntl.LOCK_EX if blocking else fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            if blocking:
                raise
            yield False
            return
        try:
            yield True
        finally:
            fcntl.flock(f, fcntl.LOCK_UN)


def _mark_retired(version_dir: str) -> None:
    try:
        with open(os.path.join(version_dir, _RETIRED_MARKER), "wb"):
            pass
    except OSError:
        # Without the marker, the reclaim goes by when it was renamed.
        pass


def _point_at(link: str, version: str) -> None:
    """Make ``link`` name ``version``, retiring what it named before.

    One atomic replace of a symlink: a reader resolves the link to one
    complete version or the other, never to nothing. The caller holds the
    image's lock.
    """
    previous = None
    if os.path.islink(link):
        previous = os.path.realpath(link)
    elif os.path.isdir(link):
        # Published before versions existed. Set aside, the way a version is
        # retired; once, the first time this image is replaced.
        previous = f"{link}.old.{uuid.uuid4().hex}"
        os.rename(link, previous)
    staged_link = f"{link}.lnk.{uuid.uuid4().hex}"
    os.symlink(os.path.basename(version), staged_link)
    try:
        os.replace(staged_link, link)
    except OSError:
        with contextlib.suppress(OSError):
            os.unlink(staged_link)
        if previous is not None and _LEGACY_NAME.match(os.path.basename(previous)):
            os.rename(previous, link)
        raise
    if previous is not None:
        _mark_retired(previous)


def retire_entry(entry_dir: str) -> None:
    """Take an image out of the cache, deleting it once nothing runs from it.

    Deleting it outright emptied the root filesystem of every sandbox still
    running from it: the container's gofer serves files from this directory,
    so anything the container had not opened yet vanished -- measured, a
    running sandbox's ``ls /etc`` came back empty after another sandbox on the
    node forced a re-pull of the same image.

    Args:
        entry_dir: The image's path in the cache, ``<images_dir>/<key>``.
    """
    images_dir, key = os.path.split(entry_dir)
    with _key_lock(images_dir, key):
        if os.path.islink(entry_dir):
            version = os.path.realpath(entry_dir)
            os.unlink(entry_dir)
            _mark_retired(version)
        elif os.path.isdir(entry_dir):
            aside = f"{entry_dir}.old.{uuid.uuid4().hex}"
            os.rename(entry_dir, aside)
            _mark_retired(aside)
    reclaim_retired_entries(images_dir)


def reclaim_retired_entries(images_dir: str, bundles_dir: Optional[str] = None) -> None:
    """Delete the versions that are retired, past their grace, and unused.

    Best-effort and never fatal, like the rest of the sweep. Each image is
    reclaimed under its own lock, taken without blocking: an image being
    published right now is left for a later sweep, and a version is never
    judged while its link is being repointed.

    Args:
        images_dir: The image cache to reclaim from.
        bundles_dir: Where live sandbox bundles are; ``SANDBOX_BUNDLES_DIR``
            by default.
    """
    try:
        names = os.listdir(images_dir)
    except OSError:
        return
    by_key: Dict[str, list] = {}
    for name in names:
        match = _VERSION_NAME.match(name) or _LEGACY_NAME.match(name)
        if match:
            by_key.setdefault(match.group(1), []).append(name)
    in_use = None
    now = time.time()
    for key, candidates in by_key.items():
        try:
            with _key_lock(images_dir, key, blocking=False) as locked:
                if not locked:
                    continue
                link = os.path.join(images_dir, key)
                current = os.path.realpath(link) if os.path.islink(link) else None
                for name in candidates:
                    path = os.path.join(images_dir, name)
                    if path == current:
                        continue
                    retired_at = _retired_at(path)
                    if retired_at is None or now - retired_at < _RETIRED_GRACE_SECONDS:
                        continue
                    if in_use is None:
                        in_use = _versions_in_use(bundles_dir or SANDBOX_BUNDLES_DIR)
                    # A bundle from before versions names the image by its
                    # link path; its container runs from a set-aside entry.
                    if path in in_use or (_LEGACY_NAME.match(name) and link in in_use):
                        continue
                    shutil.rmtree(path, ignore_errors=True)
        except OSError:
            continue


def _retired_at(path: str) -> Optional[float]:
    """When ``path`` stopped being current, or None if unreadable."""
    try:
        return os.path.getmtime(os.path.join(path, _RETIRED_MARKER))
    except OSError:
        pass
    # No marker: set aside before markers existed, or a version orphaned by a
    # crash between being renamed into place and being pointed at. A rename
    # sets the change time, so that is when it stopped being current.
    try:
        return os.stat(path).st_ctime
    except OSError:
        return None


def _container_is_live(sandbox_id: str) -> bool:
    """Whether the container behind a bundle directory still exists.

    A sandbox whose actor was killed never deletes its bundle or its runsc
    state, so a bundle alone proves nothing -- measured, a node held 89 such
    containers, every one ``stopped``. This asks what ``runsc list`` asks: is
    the sandbox process recorded in the container's state still alive.
    Anything unreadable counts as live, so doubt keeps a version rather than
    deleting one in use. A container still starting has no state yet; the
    retirement grace period covers it.
    """
    state = os.path.join(RUNSC_STATE_DIR, f"{sandbox_id}_sandbox:{sandbox_id}.state")
    try:
        with open(state, encoding="utf-8") as f:
            pid = int((json.load(f).get("sandbox") or {}).get("pid") or 0)
    except FileNotFoundError:
        # Deleted by runsc, so there is no container to protect.
        return False
    except (OSError, ValueError, AttributeError, TypeError):
        return True
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except OSError:
        # Alive, and someone else's.
        return True
    return True


def _versions_in_use(bundles_dir: str) -> set:
    """The directories live bundles run from: a version, or a legacy link path."""
    in_use = set()
    try:
        bundles = os.listdir(bundles_dir)
    except OSError:
        return in_use
    for bundle in bundles:
        if not _container_is_live(bundle):
            continue
        try:
            with open(
                os.path.join(bundles_dir, bundle, "config.json"), encoding="utf-8"
            ) as f:
                spec = json.load(f)
        except (OSError, ValueError):
            continue
        root = (spec.get("root") or {}).get("path") if isinstance(spec, dict) else None
        if root:
            in_use.add(os.path.dirname(os.path.normpath(root)))
    return in_use


def _verify_digest(
    payload: Optional[bytes],
    expected: str,
    image: str,
    what: str,
    actual: Optional[str] = None,
) -> None:
    """Check content against the digest the manifest named for it.

    Only ``sha256:`` is checked; an algorithm this code does not implement is
    passed over rather than treated as a mismatch.
    """
    if not expected.startswith("sha256:"):
        return
    if actual is None:
        actual = f"sha256:{hashlib.sha256(payload).hexdigest()}"
    if actual != expected:
        raise SandboxCreationError(
            f"Digest mismatch on the {what} for image '{image}': the registry "
            f"named {expected} but returned {actual}."
        )


def _manifest_url(registry: str, repo: str, reference: str) -> str:
    """URL for one manifest, with the caller-supplied parts escaped.

    ``repo`` and ``reference`` come from a user string. Unescaped, a ``?`` or
    ``#`` in either silently rewrites the request target.
    """
    safe_repo = urllib.parse.quote(repo, safe="/")
    safe_reference = urllib.parse.quote(reference, safe="")
    return f"https://{registry}/v2/{safe_repo}/manifests/{safe_reference}"


def _blob_url(registry: str, repo: str, digest: str) -> str:
    safe_repo = urllib.parse.quote(repo, safe="/")
    safe_digest = urllib.parse.quote(digest, safe="")
    return f"https://{registry}/v2/{safe_repo}/blobs/{safe_digest}"


def _select_platform_manifest(manifests: list, image: str) -> str:
    """Pick this host's manifest from an index.

    Falling back to the first entry picks whatever the registry happened to
    list first -- routinely a BuildKit attestation manifest tagged
    ``unknown/unknown``, or a windows build. Both produce a confusing "no
    layers" error or an unrunnable root filesystem, so an absent variant is
    reported as what it is.
    """
    target_arch = get_platform_arch()
    for m in manifests:
        plat = m.get("platform", {})
        if plat.get("os") == "linux" and plat.get("architecture") == target_arch:
            return m["digest"]

    available = sorted(
        {
            f"{m.get('platform', {}).get('os', '?')}/"
            f"{m.get('platform', {}).get('architecture', '?')}"
            for m in manifests
        }
    )
    raise SandboxCreationError(
        f"Image '{image}' has no linux/{target_arch} variant. "
        f"The registry offers: {', '.join(available) or 'nothing'}."
    )


def _remote_manifest_digest(image: str, timeout_seconds: float) -> Optional[str]:
    """The digest a reference currently resolves to, or None if unknown."""
    try:
        registry, repo, reference = parse_image_ref(image)
        auth_headers = get_registry_auth_headers(
            registry, repo, reference=reference, timeout=timeout_seconds
        )
        headers = {"User-Agent": _USER_AGENT, "Accept": _MANIFEST_ACCEPT}
        req = _registry_request(
            _manifest_url(registry, repo, reference),
            headers,
            auth_headers.get("Authorization"),
        )
        req.get_method = lambda: "HEAD"
        with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
            return resp.headers.get("Docker-Content-Digest")
    except Exception as err:
        logger.debug(f"Could not revalidate '{image}': {err}")
        return None


# How long this process trusts an entry it has just revalidated. Keyed by the
# entry's identity as well as its path, so a replaced entry is checked afresh.
_REVALIDATE_AFTER_SECONDS = 60
_confirmed_current: Dict[Tuple[str, str], float] = {}


def _cached_entry_is_current(
    image: str, target_dir: str, timeout_seconds: float
) -> bool:
    """Whether a cached registry image still matches its reference.

    A tag is mutable. Without this check an entry is pinned for the life of the
    directory, so a node that pulled ``busybox:latest`` a month ago keeps
    running it while a node that joined today runs something else.

    A digest reference is immutable, so it is trusted without a round trip, and
    an unreachable registry keeps the cached entry rather than failing a pull
    that would otherwise have worked offline.

    An answer holds for ``_REVALIDATE_AFTER_SECONDS`` in this process. One
    sandbox creation resolves its image three times -- the runtime, the
    backend and the OCI spec each pull -- and each check is three registry
    round trips: measured, nine requests and over two seconds for a cached
    image.
    """
    if "@" in image:
        return True

    try:
        memo_key = (target_dir, entry_identity(target_dir))
    except OSError:
        return True
    confirmed = _confirmed_current.get(memo_key)
    if (
        confirmed is not None
        and time.monotonic() - confirmed < _REVALIDATE_AFTER_SECONDS
    ):
        return True

    recorded_path = os.path.join(target_dir, MANIFEST_DIGEST_FILENAME)
    try:
        with open(recorded_path, "r", encoding="utf-8") as f:
            recorded = f.read().strip()
    except OSError:
        # Written by an older version, or never recorded. Nothing to compare.
        return True

    remote = _remote_manifest_digest(image, timeout_seconds)
    if remote is None or not recorded or remote == recorded:
        # Unreachable counts too: retrying straight away would only wait out
        # the same network timeout again.
        _confirmed_current[memo_key] = time.monotonic()
        return True

    logger.info(
        f"Image '{image}' changed upstream ({recorded[:19]}... -> "
        f"{remote[:19]}...); re-pulling."
    )
    return False


def pull_and_extract_container_image(
    image: str,
    images_dir: str = DEFAULT_IMAGES_DIR,
    timeout_seconds: float = 120.0,
) -> str:
    """Resolve an image reference to an extracted root filesystem on this node.

    Args:
        image: Container image name (e.g. 'python:3.10-slim') or a path to a
            local tar archive.
        images_dir: Root directory for caching container images.
        timeout_seconds: Network request timeout.

    Returns:
        Absolute directory path containing the extracted container filesystem.
    """
    # Gate every path: the directory this returns is mounted as a container's
    # root, so it has to be ours before we hand it back. publish_image repeats
    # this for the paths that reach it, which is cheap and keeps it correct
    # when called directly.
    try:
        os.makedirs(images_dir, mode=0o700, exist_ok=True)
        _assert_owned_by_us(images_dir)
    except SandboxCreationError:
        raise
    except Exception as err:
        raise SandboxCreationError(
            f"Failed to create images directory '{images_dir}': {err}"
        ) from err

    safe_name = sanitize_image_name(image)

    if os.path.isfile(image):
        return publish_image(
            images_dir,
            safe_name,
            materialize=_materialize_local_tar(image),
            is_current=lambda entry: _local_tar_is_current(image, entry),
        )

    if (
        image.endswith(".tar")
        or image.startswith("/")
        or image.startswith("./")
        or image.startswith("../")
    ):
        raise SandboxCreationError(f"Local image archive '{image}' not found.")

    return publish_image(
        images_dir,
        safe_name,
        materialize=_materialize_registry(image, timeout_seconds),
        is_current=lambda entry: _cached_entry_is_current(
            image, entry, timeout_seconds
        ),
    )


def _local_tar_is_current(image: str, target_dir: str) -> bool:
    """Whether a cached entry is at least as new as the tar it came from."""
    try:
        marker = os.path.join(target_dir, ".extracted")
        return os.path.getmtime(marker) >= os.path.getmtime(image)
    except OSError:
        return False


def _materialize_local_tar(image: str):
    """Producer for a local tar archive holding a flat root filesystem."""

    def materialize(rootfs_dir: str) -> Dict[str, bytes]:
        try:
            with open(image, "rb") as f:
                extract_tar_layer(f, rootfs_dir)
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to extract local image archive '{image}': {err}"
            ) from err
        # A bare root filesystem carries no image config of its own.
        return {}

    return materialize


def _materialize_registry(image: str, timeout_seconds: float):
    """Producer that pulls an image over the Registry v2 HTTP API."""

    def materialize(rootfs_dir: str) -> Dict[str, bytes]:
        # Layer blobs stage beside the rootfs, inside the entry's own staging
        # directory, so a killed pull discards them along with it.
        tmp_extract_dir = os.path.dirname(rootfs_dir)
        try:
            registry, repo, reference = parse_image_ref(image)
            auth_headers = get_registry_auth_headers(
                registry,
                repo,
                reference=reference,
                timeout=timeout_seconds,
            )
            headers = {
                "User-Agent": _USER_AGENT,
                "Accept": _MANIFEST_ACCEPT,
            }
            auth_header = auth_headers.get("Authorization")

            manifest_url = _manifest_url(registry, repo, reference)
            req = _registry_request(manifest_url, headers, auth_header)
            with urllib.request.urlopen(req, timeout=timeout_seconds) as resp:
                resolved_digest = resp.headers.get("Docker-Content-Digest")
                manifest_data = json.loads(resp.read().decode("utf-8"))

            # Resolve a manifest list / OCI index down to one manifest.
            # An index can point at another index, so this loops rather
            # than resolving a single level.
            for _ in range(_MAX_INDEX_DEPTH):
                if "manifests" not in manifest_data:
                    break
                chosen_digest = _select_platform_manifest(
                    manifest_data["manifests"], image
                )
                sub_req = _registry_request(
                    _manifest_url(registry, repo, chosen_digest),
                    headers,
                    auth_header,
                )
                with urllib.request.urlopen(sub_req, timeout=timeout_seconds) as resp:
                    manifest_data = json.loads(resp.read().decode("utf-8"))
            else:
                raise SandboxCreationError(
                    f"Manifest for image '{image}' nests more than "
                    f"{_MAX_INDEX_DEPTH} index levels deep."
                )

            # Extract the image config, which carries Env and WorkingDir.
            # This must be fatal rather than best-effort: publishing ends by
            # writing the .extracted marker, and a cached entry is revalidated
            # only against its manifest digest, so swallowing a transient
            # failure here pins an image with no PATH and no WORKDIR on this
            # node -- and the damage shows up much later as a container
            # behaving subtly wrong.
            config_desc = manifest_data.get("config")
            if not (config_desc and "digest" in config_desc):
                raise SandboxCreationError(
                    f"Manifest for image '{image}' has no config "
                    f"descriptor, so its environment cannot be "
                    f"determined."
                )
            config_digest = config_desc["digest"]
            config_req = _registry_request(
                _blob_url(registry, repo, config_digest), headers, auth_header
            )
            try:
                with urllib.request.urlopen(
                    config_req, timeout=timeout_seconds
                ) as resp:
                    config_bytes = resp.read()
            except Exception as err:
                raise SandboxCreationError(
                    f"Failed to fetch the image config blob "
                    f"({config_digest}) for '{image}': {err}"
                ) from err
            _verify_digest(config_bytes, config_digest, image, "config blob")

            layers = manifest_data.get("layers", [])
            if not layers:
                raise SandboxCreationError(
                    f"No layers found in manifest for image '{image}'"
                )

            for layer in layers:
                digest = layer["digest"]
                blob_req = _registry_request(
                    _blob_url(registry, repo, digest), headers, auth_header
                )
                with urllib.request.urlopen(
                    blob_req, timeout=timeout_seconds
                ) as blob_resp:
                    with tempfile.NamedTemporaryFile(
                        dir=tmp_extract_dir, suffix=".partial", delete=True
                    ) as tmp_blob_file:
                        # Hash while streaming: a layer is content
                        # addressed, and extracting one that does not
                        # match its digest means trusting a mirror or a
                        # truncated transfer to have handed back what
                        # the manifest actually named.
                        hasher = hashlib.sha256()
                        for chunk in iter(lambda: blob_resp.read(64 * 1024), b""):
                            hasher.update(chunk)
                            tmp_blob_file.write(chunk)
                        _verify_digest(
                            None,
                            digest,
                            image,
                            f"layer {digest[:19]}...",
                            actual=f"sha256:{hasher.hexdigest()}",
                        )
                        tmp_blob_file.seek(0)
                        extract_tar_layer(tmp_blob_file, rootfs_dir)

            sidecars = {IMAGE_CONFIG_FILENAME: config_bytes}
            if resolved_digest:
                sidecars[MANIFEST_DIGEST_FILENAME] = resolved_digest.encode("utf-8")
            return sidecars

        except SandboxCreationError:
            raise
        except Exception as err:
            raise SandboxCreationError(
                f"Failed to pull and extract container image '{image}': {err}"
            ) from err

    return materialize


def _write_sidecar(path: str, data: bytes) -> None:
    """Write one of a cache entry's metadata files.

    Centralized so every producer agrees on the mode: a sidecar written through
    a temporary file lands at 0600, while one written through ``open()`` lands
    at 0644, which would leave two entries in one cache disagreeing about an
    identically named file.
    """
    with open(path, "wb") as f:
        f.write(data)
    os.chmod(path, 0o644)


def publish_image(
    images_dir: str,
    cache_key: str,
    *,
    materialize: Callable[[str], Dict[str, bytes]],
    is_current: Callable[[str], bool] = lambda target_dir: True,
) -> str:
    """Build a cache entry in a staging directory and publish it atomically.

    Takes from a producer only the two things that actually differ between one
    and another -- how to fill a root filesystem, and whether an existing entry
    may be reused -- and supplies everything else: the ownership check, the
    lock, the staging directory, the metadata ordering, and the atomic swap.

    ``materialize`` fills the ``rootfs`` directory it is handed and *returns*
    the sidecar files to place beside it. Returning them rather than writing
    them is what makes the ordering structural: a producer cannot publish an
    entry whose metadata lands after the swap, because the swap is not its to
    perform. Writing an image config after publishing leaves a window in which
    an entry has a root filesystem but no config, and a sandbox started in that
    window comes up with no PATH and no WORKDIR.

    ``materialize`` may also use the staging directory -- the parent of the path
    it is given -- for scratch of any size. It is discarded wholesale on
    failure, so nothing a producer stages there can outlive a crash.

    The entry is published as a new immutable version that ``<cache_key>`` is
    then pointed at; see ``_VERSION_NAME``.

    Args:
        images_dir: Root directory for cached images.
        cache_key: Name for this entry, from ``sanitize_image_name``.
        materialize: Populates ``rootfs`` and returns ``{filename: bytes}``.
        is_current: Whether an already-published entry may be reused as-is.

    Returns:
        The version directory now current: an absolute path that keeps naming
        this exact content even after the image is replaced, so a caller that
        builds a sandbox from it is not moved onto a different version midway.
    """
    try:
        # The *leaf*, with an explicit mode. CPython's makedirs drops `mode` on
        # its recursive call, so creating this path as an intermediate of a
        # deeper directory would leave it at 0o777 & ~umask -- which
        # _assert_owned_by_us then rejects under a group-writable umask.
        os.makedirs(images_dir, mode=0o700, exist_ok=True)
        _assert_owned_by_us(images_dir)
    except SandboxCreationError:
        raise
    except Exception as err:
        raise SandboxCreationError(
            f"Failed to create images directory '{images_dir}': {err}"
        ) from err

    _sweep_stale_temporaries(images_dir)

    target_dir = os.path.join(images_dir, cache_key)
    with _key_lock(images_dir, cache_key):
        marker_path = os.path.join(target_dir, ".extracted")
        if (
            os.path.isdir(target_dir)
            and os.path.exists(marker_path)
            and is_current(target_dir)
        ):
            published = os.path.realpath(target_dir)
        else:
            staging = os.path.join(images_dir, f"{cache_key}.tmp.{uuid.uuid4().hex}")
            os.makedirs(staging, mode=0o755, exist_ok=True)
            rootfs_dir = os.path.join(staging, "rootfs")
            os.makedirs(rootfs_dir, mode=0o755, exist_ok=True)
            version = os.path.join(images_dir, f"{cache_key}.v.{uuid.uuid4().hex}")
            try:
                sidecars = materialize(rootfs_dir) or {}
                for name, data in sidecars.items():
                    _write_sidecar(os.path.join(staging, name), data)
                _write_sidecar(os.path.join(staging, ".extracted"), b"ok")
                os.rename(staging, version)
                _point_at(target_dir, version)
            except BaseException:
                shutil.rmtree(staging, ignore_errors=True)
                # Renamed into place but never pointed at: nothing can be
                # running from it.
                if os.path.realpath(target_dir) != version:
                    shutil.rmtree(version, ignore_errors=True)
                raise
            published = version
    # After the lock is released, so the reclaim can take this image's lock too
    # and retire what the new version replaced.
    reclaim_retired_entries(images_dir)
    return published
