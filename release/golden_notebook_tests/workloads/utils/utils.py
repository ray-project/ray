def is_anyscale_connect(address: str) -> bool:
    """Returns whether or not the Ray Address points to an Anyscale cluster."""
    is_anyscale_connect = address is not None and address.startswith(
        "anyscale://")
    return is_anyscale_connect
