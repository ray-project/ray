def should_fail():
    """
    >>> import torch
    >>> torch.cuda.is_available()
    False
    """


def should_pass():
    """
    >>> import torch
    >>> torch.cuda.is_available()
    True
    """
