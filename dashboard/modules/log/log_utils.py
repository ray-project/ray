import mimetypes
import ray.dashboard.modules.log.log_consts as log_consts


def register_mimetypes():
    for _type, extensions in log_consts.MIME_TYPES.items():
        for ext in extensions:
            mimetypes.add_type(_type, ext)
