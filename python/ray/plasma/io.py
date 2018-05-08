"""
plasma::io
"""

import sys
import asyncio
from ctypes import c_size_t, c_int64, sizeof
from asyncio.streams import StreamReader, StreamWriter

PLASMA_PROTOCOL_VERSION = bytes(c_int64(0))  # int64_t


async def WriteMessage(fd: StreamWriter, message_type, length, buf):
    version = PLASMA_PROTOCOL_VERSION
    fd.write(version)
    fd.write(bytes(c_int64(message_type)))
    fd.write(bytes(c_size_t(length)))
    fd.write(buf)
    await fd.drain()


async def ReadMessage(fd: StreamReader):
    version = await fd.read(sizeof(c_int64))
    assert version == PLASMA_PROTOCOL_VERSION
    message_type = await fd.read(sizeof(c_int64))
    message_type = int.from_bytes(message_type, sys.byteorder, signed=True)
    length_temp = await fd.read(sizeof(c_int64))
    length_temp = int.from_bytes(length_temp, sys.byteorder, signed=True)
    buffer = await fd.read(length_temp)
    if len(buffer) < length_temp:
        buffer = buffer.ljust(length_temp, b'\0')
    return message_type, buffer


class AsyncPlasmaSockets:
    """
    Async socket services
    
    Notes:
        This class is not thread-safe
    """
    
    MAX_SOCKET_NUM = 8
    
    def __init__(self, socket_name):
        self.socket_name = socket_name
        self._loop = asyncio.get_event_loop()
        self.active_sockets = []
        self.idle_sockets = []
        
        this = self
        
        class GetSocket:
            async def __aenter__(self):
                while this.full:
                    await asyncio.sleep(0.01, loop=this._loop)
                
                if not this.idle_sockets:
                    self.socket_pair = await asyncio.open_unix_connection(this.socket_name, loop=this._loop)
                else:
                    self.socket_pair = this.idle_sockets.pop()
                this.active_sockets.append(self.socket_pair)
                return self.socket_pair
            
            async def __aexit__(self, exc_type, exc_val, exc_tb):
                this.active_sockets.remove(self.socket_pair)
                assert not this.full
                this.idle_sockets.append(self.socket_pair)
        
        self.get_socket = GetSocket
    
    @property
    def full(self):
        return len(self.active_sockets) >= self.MAX_SOCKET_NUM
    
    def shutdown(self):
        self._loop.stop()
        # self._loop.run_until_complete(self._loop.shutdown_asyncgens())
        self._loop.close()
        self.idle_sockets.extend(self.active_sockets)
        self.active_sockets.clear()
        for socket_pair in self.idle_sockets:
            socket_pair[1].close()
        self.idle_sockets.clear()
