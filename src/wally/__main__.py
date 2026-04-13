"""Entry point for the Wally MCP agent (Streamable HTTP transport).

Wally runs as a single HTTP server launched by Woof as a subprocess.
One port serves both MCP (at /mcp) and preview JPEGs (at /previews/…).
On startup it binds an OS-assigned loopback port and prints:

    WALLY_READY port=<port>

to stdout, then serves until terminated (SIGTERM from Woof on shutdown).

Logs are written to the platform log directory by default.
Override with WALLY_LOG_FILE=/path/to/file.log.
"""

from __future__ import annotations

import asyncio
import logging
import os
import socket
import sys

from ouestcharlie_toolkit import setup_logging

# Set up logging before importing agent code (which may trigger library imports).
_log_file = setup_logging("wally", log_file_env_var="WALLY_LOG_FILE", level=logging.DEBUG)
_log = logging.getLogger(__name__)
_log.info("Wally starting — log: %s", _log_file)

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import Response

from wally.agent import WallyAgent
from wally.http_server import MediaMiddleware


class _BearerGuard(BaseHTTPMiddleware):
    """Reject MCP HTTP requests that lack a valid Bearer token."""

    def __init__(self, app: object, *, token: str) -> None:
        super().__init__(app)  # type: ignore[arg-type]
        self._expected = f"Bearer {token}"

    async def dispatch(self, request: object, call_next: object) -> object:  # type: ignore[override]
        from starlette.requests import Request
        from starlette.types import ASGIApp

        req: Request = request  # type: ignore[assignment]
        nxt: ASGIApp = call_next  # type: ignore[assignment]
        if req.headers.get("authorization") != self._expected:
            return Response("Unauthorized", status_code=401)
        return await nxt(req)  # type: ignore[return-value]


def _bind_free_port() -> tuple[socket.socket, int]:
    """Bind an OS-assigned loopback TCP port; caller passes the socket to uvicorn."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("127.0.0.1", 0))
    return sock, sock.getsockname()[1]


async def _serve(app: object, sock: socket.socket, port: int) -> None:
    import uvicorn

    config = uvicorn.Config(
        app,  # type: ignore[arg-type]
        log_level="info",
        access_log=True,
    )
    server = uvicorn.Server(config)

    async def _signal_ready() -> None:
        # Wait until uvicorn has finished binding and is accepting connections
        # before signalling Woof.  Emitting WALLY_READY too early (before
        # server.serve() runs startup) causes connection failures on Windows
        # where import/startup overhead is larger than on macOS/Linux.
        while not server.started:
            await asyncio.sleep(0.05)
        sys.stdout.write(f"WALLY_READY port={port}\n")
        sys.stdout.flush()
        _log.info("Wally ready — port %d", port)

    await asyncio.gather(server.serve(sockets=[sock]), _signal_ready())


def main() -> None:
    agent_token = os.environ.get("WOOF_AGENT_TOKEN", "")
    backend_name = os.environ.get("WALLY_BACKEND_NAME", "")

    agent = WallyAgent()

    # Layer the ASGI stack (innermost first):
    #   MCP app (Starlette, handles /mcp)
    #   → MediaMiddleware (intercepts /previews/… and /thumbnails/…)
    #   → _BearerGuard (enforces auth on all routes, including media)
    mcp_app = agent.mcp.streamable_http_app()
    app: object = MediaMiddleware(
        mcp_app,
        backend_config=agent.backend_config,
        backend_name=backend_name,
    )
    if agent_token:
        app = _BearerGuard(app, token=agent_token)

    sock, port = _bind_free_port()
    asyncio.run(_serve(app, sock, port))


if __name__ == "__main__":
    main()
