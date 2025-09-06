import aiofiles
import asyncio
import logging


class AsyncRotatingFileHandler(logging.handlers.RotatingFileHandler):
    """
    Handler for asynchronously logging to a set of files, which switches
    from one file to the next when the current file reaches a certain size.
    """

    async def _open(self):
        """
        Open the current base file with the (original) mode and encoding.
        Return the resulting stream.
        """
        return await aiofiles.open(self.baseFilename, self.mode, encoding=self.encoding)
    
    async def _close(self):
        """
        Closes the stream.
        """
        self.acquire()
        try:
            try:
                if self._afile:
                    afile = self._afile
                    try:
                        if hasattr(afile, "flush"):
                            await afile.flush()
                    finally:
                        self._afile = None
                        if hasattr(afile, "close"):
                            await afile.close()
            finally:
                logging.StreamHandler.close(self)
        finally:
            self.release()
    
    async def _flush(self):
        """
        Flushes the stream.
        """
        self.acquire()
        try:
            if self._afile and hasattr(self._afile, "flush"):
                await self._afile.flush()
        finally:
            self.release()

    async def _emit(self, record):
        """
        Emits a record.

        Output the record to the file, catering for rollover as described
        in doRollover().
        """
        try:
            if self.shouldRollover(record):
                self.doRollover()

            if self._afile is None:
                self._afile = await self._open()
            
            msg = self.format(record)
            afile = self._afile

            try:
                self.acquire()
                if hasattr(afile, "write"):
                    await afile.write(msg + self.terminator)

                if hasattr(afile, "flush"):
                    await afile.flush()
            finally:
                self.release()

        except RecursionError:
            raise
        except Exception:
            self.handleError(record)

    def close(self):
        """
        Closes the stream.
        """
        loop = asyncio.get_running_loop()
        if loop is None:
            super().close()
            return

        loop.create_task(self._close())
    
    def flush(self):
        """
        Flushes the stream.
        """
        loop = asyncio.get_running_loop()
        if loop is None:
            super().flush()
            return

        loop.create_task(self._flush())

    def emit(self, record):
        """
        Emits a record.
        """
        loop = asyncio.get_running_loop()
        if loop is None:
            super().emit(record)
            return

        loop.create_task(self._emit(record))
