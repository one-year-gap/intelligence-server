"""Provider-neutral async client using an actual official SDK stdio subprocess."""
from contextlib import AsyncExitStack
import asyncio
from pathlib import Path
import sys

from mcp import ClientSession
from mcp.client.stdio import StdioServerParameters, stdio_client
from tool_backend import ToolBackend, tool_identity


class MCPToolError(ValueError):
    """Sanitized transport failure safe to put in a model-visible trace."""


class MCPTools:
    def __init__(self, manifest_path, taxonomy_path):
        self.manifest_path = str(Path(manifest_path).resolve())
        self.taxonomy_path = str(Path(taxonomy_path).resolve())
        self._backend = ToolBackend(self.manifest_path, self.taxonomy_path)
        self.identity = self._backend.identity
        self.timeout_seconds = 10.0
        self._stack = None
        self._session = None

    async def __aenter__(self):
        if self._stack is not None:
            raise RuntimeError('MCPTools context is already open')
        self._stack = AsyncExitStack()
        try:
            if self.identity != tool_identity(Path(self.manifest_path).read_bytes(), Path(self.taxonomy_path).read_bytes()):
                raise MCPToolError('MCP snapshot changed before startup')
            async with asyncio.timeout(self.timeout_seconds):
                params = StdioServerParameters(command=sys.executable, args=[str(Path(__file__).with_name('mcp_server.py')), '--manifest', self.manifest_path, '--taxonomy', self.taxonomy_path])
                read, write = await self._stack.enter_async_context(stdio_client(params))
                self._session = await self._stack.enter_async_context(ClientSession(read, write))
                await self._session.initialize()
            return self
        except Exception:
            await self._close()
            raise MCPToolError('MCP startup failed') from None
        except BaseException as primary:
            try:
                await self._close()
            except Exception:
                primary.add_note('MCP cleanup also failed')
            raise

    def assert_case(self, case, taxonomy):
        self._backend.assert_case(case, taxonomy)

    async def _close(self):
        try:
            if self._stack is not None:
                # Do not inject caller errors into the transport task group.
                await self._stack.aclose()
        except Exception:
            raise MCPToolError('MCP cleanup failed') from None
        finally:
            self._stack = self._session = None

    async def __aexit__(self, exc_type, exc, tb):
        try:
            await self._close()
        except Exception:
            if exc is not None and not isinstance(exc, Exception):
                exc.add_note('MCP cleanup also failed')
                raise exc
            raise

    def _active(self):
        if self._session is None:
            raise RuntimeError('use MCPTools as an async context manager')
        return self._session

    async def list_tools(self):
        try:
            async with asyncio.timeout(self.timeout_seconds):
                result = await self._active().list_tools()
                return [{'name': tool.name, 'description': tool.description, 'inputSchema': tool.input_schema} for tool in result.tools]
        except Exception:
            raise MCPToolError('MCP discovery failed') from None

    async def call(self, name, args):
        try:
            async with asyncio.timeout(self.timeout_seconds):
                result = await self._active().call_tool(name, args, read_timeout_seconds=self.timeout_seconds)
                body = result.structured_content
                if result.is_error:
                    raise MCPToolError('MCP tool rejected request')
                if not isinstance(body, dict):
                    raise MCPToolError('MCP tool returned no structured object')
                return body
        except MCPToolError:
            raise
        except Exception:
            raise MCPToolError('MCP call failed') from None
