# source: setproctitle.pxi
import threading

_current_proctitle:str|None
_current_proctitle_lock:threading.Lock

def setproctitle(title:str)->None: ...
def getproctitle()->str: ...
