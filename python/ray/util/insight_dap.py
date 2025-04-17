import logging
import json
import asyncio
from typing import Dict, Any, List, Optional, Callable


class DAPClient:
    """
    Debug Adapter Protocol client for connecting to a debugpy server.
    """

    def __init__(self, host: str, port: int):
        self.host = host
        self.port = port
        self.reader = None
        self.writer = None
        self.sequence = 1
        self.running = False
        self.event_handlers = {}
        self.response_handlers = {}
        self.response_futures = {}
        self.logger = logging.getLogger("DAPClient")
        self.read_task = None

    async def connect(self) -> bool:
        """
        Connect to the debugpy server at the specified host and port.

        Returns:
            bool: True if connection was successful, False otherwise.
        """
        try:
            self.reader, self.writer = await asyncio.open_connection(
                self.host, self.port
            )
            self.running = True

            # Start task to read responses
            self.read_task = asyncio.create_task(self._read_messages())

            self.logger.info(f"Connected to debugpy server at {self.host}:{self.port}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to connect to {self.host}:{self.port}: {e}")
            return False

    async def disconnect(self) -> None:
        """
        Disconnect from the debugpy server.
        """
        self.running = False
        if self.read_task:
            self.read_task.cancel()
            try:
                await self.read_task
            except asyncio.CancelledError:
                pass

        if self.writer:
            self.writer.close()
            try:
                await self.writer.wait_closed()
            except:
                pass
            self.writer = None
            self.reader = None

    async def _read_messages(self) -> None:
        """
        Read and process messages from the debug adapter.
        """
        try:
            while self.running:
                # Read header
                header = b""
                while self.running:
                    chunk = await self.reader.read(1)
                    if not chunk:  # Connection closed
                        self.running = False
                        break

                    header += chunk
                    if header.endswith(b"\r\n\r\n"):
                        break

                if not self.running:
                    break

                # Parse header
                content_length = 0
                for line in header.decode("utf-8").splitlines():
                    if line.startswith("Content-Length:"):
                        content_length = int(line.strip().split(": ")[1])

                # Read content
                content = await self.reader.readexactly(content_length)

                if content:
                    # Process in a separate task to avoid blocking the read loop
                    asyncio.create_task(
                        self._process_message(json.loads(content.decode("utf-8")))
                    )

        except asyncio.CancelledError:
            # Expected when task is cancelled during disconnect
            pass
        except asyncio.IncompleteReadError:
            self.logger.error("Connection closed while reading message")
            self.running = False
        except Exception as e:
            self.logger.error(f"Error reading from socket: {e}")
            self.running = False

    async def _process_message(self, message: Dict[str, Any]) -> None:
        """
        Process a message received from the debug adapter.

        Args:
            message: The parsed JSON message
        """
        message_type = message.get("type", "")

        if message_type == "response":
            seq = message.get("request_seq", 0)
            command = message.get("command", "")
            success = message.get("success", False)

            if seq in self.response_handlers:
                handler = self.response_handlers.pop(seq)
                handler(message)
            elif seq in self.response_futures:
                future = self.response_futures.pop(seq)
                if not future.done():
                    future.set_result(message)

            self.logger.debug(f"Response: {command} (success={success})")

        elif message_type == "event":
            event = message.get("event", "")
            handler = self.event_handlers.get(event)

            if handler:
                await handler(message)

            self.logger.debug(f"Event: {event}")

        else:
            self.logger.warning(f"Unknown message type: {message_type}")

    async def _send_message(self, message: Dict[str, Any]) -> None:
        """
        Send a message to the debug adapter.

        Args:
            message: The message to send
        """
        if not self.writer:
            self.logger.error("Not connected to debug adapter")
            return

        json_str = json.dumps(message)
        content = json_str.encode("utf-8")
        header = f"Content-Length: {len(content)}\r\n\r\n".encode("utf-8")

        try:
            self.writer.write(header + content)
            await self.writer.drain()
            self.logger.debug(f"Sent message: {message}")
        except Exception as e:
            self.logger.error(f"Failed to send message: {e}")

    async def send_request(
        self, command: str, args: Dict[str, Any] = None, callback: Callable = None
    ) -> int:
        """
        Send a request to the debug adapter.

        Args:
            command: The command to send
            args: The arguments for the command
            callback: Optional callback function to handle the response

        Returns:
            The sequence number of the request
        """
        seq = self.sequence
        self.sequence += 1

        request = {
            "type": "request",
            "seq": seq,
            "command": command,
            "arguments": args or {},
        }

        if callback:
            self.response_handlers[seq] = callback

        await self._send_message(request)
        return seq

    async def _send_request_and_wait(
        self, command: str, args: Dict[str, Any] = None, timeout: float = 5.0
    ) -> Optional[Dict[str, Any]]:
        """
        Send a request and wait for the response.

        Args:
            command: The command to send
            args: The arguments for the command
            timeout: Maximum time to wait for a response (in seconds)

        Returns:
            The response or None if timed out
        """
        future = asyncio.get_running_loop().create_future()
        seq = await self.send_request(command, args)
        self.response_futures[seq] = future

        try:
            return await asyncio.wait_for(future, timeout)
        except asyncio.TimeoutError:
            self.logger.warning(f"Timeout waiting for response to {command}")
            self.response_futures.pop(seq, None)
            return None

    def register_event_handler(self, event: str, handler: Callable) -> None:
        """
        Register a handler for a specific event.

        Args:
            event: The event name
            handler: The handler function
        """
        self.event_handlers[event] = handler

    # Common DAP commands

    async def initialize(self) -> Dict[str, Any]:
        """
        Initialize the debug session.

        Returns:
            The response from the debug adapter
        """
        args = {
            "clientID": "python-dap-client",
            "clientName": "Python DAP Client",
            "adapterID": "python",
            "pathFormat": "path",
            "linesStartAt1": True,
            "columnsStartAt1": True,
            "supportsVariableType": True,
            "supportsVariablePaging": True,
            "supportsRunInTerminalRequest": True,
        }
        return await self._send_request_and_wait("initialize", args)

    async def attach(self, **kwargs) -> Dict[str, Any]:
        """
        Attach to a running debugpy process.

        Args:
            **kwargs: Attach configuration arguments which may include:
                - host: The host name or IP address of the debuggee
                - port: The port of the debuggee
                - pathMappings: The path mappings for source files
                - justMyCode: Debug only user-written code
                - redirectOutput: Redirect output to debug console

        Returns:
            The response from the debug adapter
        """
        # Default arguments
        args = {
            "request": "attach",
            "type": "python",
            "name": "Python Debugger: Attach",
            "justMyCode": False,
            "redirectOutput": False,
            "connect": {"host": "127.0.0.1", "port": 5678},
        }

        # Update with provided arguments
        for key, value in kwargs.items():
            if key in ["justMyCode", "redirectOutput", "pathMappings"]:
                args[key] = value
            elif key == "host":
                args["connect"]["host"] = value
            elif key == "port":
                args["connect"]["port"] = int(value)

        # Use a longer timeout for attach command (30 seconds)
        return await self._send_request_and_wait("attach", args, timeout=0.1)

    async def set_breakpoints(
        self, source: Dict[str, str], lines: List[int]
    ) -> Dict[str, Any]:
        """
        Set breakpoints in a source file.

        Args:
            source: The source file information
            breakpoints: List of breakpoints

        Returns:
            The response from the debug adapter
        """
        args = {
            "source": source,
            "breakpoints": [{"line": line} for line in lines],
            "sourceModified": False,
        }
        return await self._send_request_and_wait("setBreakpoints", args)

    async def continue_execution(self) -> Dict[str, Any]:
        """
        Continue execution after a stop.

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("continue", {"threadId": 0})

    async def step_in(self, thread_id: int) -> Dict[str, Any]:
        """
        Step into the next function call.

        Args:
            thread_id: The thread ID

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("stepIn", {"threadId": thread_id})

    async def step_out(self, thread_id: int) -> Dict[str, Any]:
        """
        Step out of the current function.

        Args:
            thread_id: The thread ID

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("stepOut", {"threadId": thread_id})

    async def step_over(self, thread_id: int) -> Dict[str, Any]:
        """
        Step over the next function call.

        Args:
            thread_id: The thread ID

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("next", {"threadId": thread_id})

    async def pause(self, thread_id: int) -> Dict[str, Any]:
        """
        Pause execution.

        Args:
            thread_id: The thread ID

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("pause", {"threadId": thread_id})

    async def get_threads(self) -> Dict[str, Any]:
        """
        Get all threads.

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("threads")

    async def get_stack_trace(self, thread_id: int) -> Dict[str, Any]:
        """
        Get the stack trace for a thread.

        Args:
            thread_id: The thread ID

        Returns:
            The response from the debug adapter
        """
        args = {
            "threadId": thread_id,
            "format": {
                "parameters": True,
                "parameterTypes": True,
                "parameterNames": True,
                "parameterValues": True,
                "line": True,
            },
        }
        return await self._send_request_and_wait("stackTrace", args)

    async def get_scopes(self, frame_id: int) -> Dict[str, Any]:
        """
        Get the scopes for a stack frame.

        Args:
            frame_id: The frame ID

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait("scopes", {"frameId": frame_id})

    async def evaluate(
        self, expression: str, frame_id: int = 0, thread_id: int = None
    ) -> Dict[str, Any]:
        """
        Evaluate an expression.

        Args:
            expression: The expression to evaluate
            frame_id: The frame ID
            thread_id: The thread ID (required by DAP spec if frameId is specified)

        Returns:
            The response from the debug adapter
        """
        args = {"expression": expression, "context": "repl"}

        # According to DAP spec, if frameId is specified, threadId must also be specified
        if frame_id:
            args["frameId"] = frame_id
            # Use provided thread_id or the current thread if one is active
            if thread_id is not None:
                args["threadId"] = thread_id

        return await self._send_request_and_wait("evaluate", args)

    async def disconnect_request(self) -> Dict[str, Any]:
        """
        Request disconnection from the debug adapter.

        Returns:
            The response from the debug adapter
        """
        return await self._send_request_and_wait(
            "disconnect", {"restart": False, "terminateDebuggee": False}
        )
