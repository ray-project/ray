import secrets


def generate_new_authentication_token() -> str:
    """Generate an authentication token for the cluster.

    256 bits of entropy is considered sufficient to be durable to brute force attacks.
    """
    return secrets.token_hex(32)
