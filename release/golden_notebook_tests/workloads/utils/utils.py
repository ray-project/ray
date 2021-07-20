import os


def is_anyscale_connect():
    """Returns whether or not the Ray Address points to an Anyscale cluster."""
    address = os.environ.get("RAY_ADDRESS")
    is_anyscale_connect = address is not None and address.startswith(
        "anyscale://")
    return is_anyscale_connect
