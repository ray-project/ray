# source: setproctitle.pxi
import threading
from typing import Union

_current_proctitle: Union[str, None]
_current_proctitle_lock: threading.Lock

def setproctitle(title: str) -> None: ...
def getproctitle() -> str: ...
