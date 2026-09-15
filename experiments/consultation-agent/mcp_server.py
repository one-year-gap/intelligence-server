"""Official MCP SDK 2.2 stdio server. Paths are host-only startup arguments."""
import argparse
import asyncio
import json

from mcp import types
from mcp.server.lowlevel import Server
from mcp.server.stdio import stdio_server

from tool_backend import ToolBackend


async def serve(manifest_path, taxonomy_path):
    backend = ToolBackend(manifest_path, taxonomy_path)

    async def list_tools(context, params):
        return types.ListToolsResult(tools=[types.Tool(**tool) for tool in backend.list_tools()])

    async def call_tool(context, params):
        try:
            result = backend.dispatch(params.name, params.arguments or {})
            return types.CallToolResult(content=[types.TextContent(type='text', text=json.dumps(result, ensure_ascii=False))], structured_content=result)
        except ValueError as exc:
            error = {'error': str(exc)}
            return types.CallToolResult(content=[types.TextContent(type='text', text=json.dumps(error, ensure_ascii=False))], structured_content=error, is_error=True)

    server = Server('holliverse-read-only', version='1', on_list_tools=list_tools, on_call_tool=call_tool)
    async with stdio_server() as (read, write):
        await server.run(read, write, server.create_initialization_options())


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--manifest', required=True)
    parser.add_argument('--taxonomy', required=True)
    args = parser.parse_args()
    asyncio.run(serve(args.manifest, args.taxonomy))
