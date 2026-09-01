from __future__ import annotations

import json
from typing import Any, Callable, Dict, Optional

from assistant_gateway.tools.base import Tool, ToolContext


def get_claude_mcp_server(
    name: str,
    version: str,
    tools: Dict[str, Tool],
    input_overrides: Optional[Dict[str, Dict[str, Any]]] = None,
):
    """
    Build a McpSdkServerConfig for the given tools with runtime overrides applied.

    Args:
        name: MCP server name (e.g. "calculator").
        version: MCP server version string (e.g. "0.1.0").
        tools: Mapping of tool name -> Tool instance.
        input_overrides: Per-request overrides dict. Structure:
            {
                "__global__": {...},        # applied to every tool
                "<tool_name>": {...},       # applied only to that tool
            }

    Returns:
        McpSdkServerConfig ready to pass into ClaudeAgentOptions.
    """
    from claude_agent_sdk import create_sdk_mcp_server

    tool_functions = [
        wrap_tool_for_claude(tool, input_overrides)
        for tool in tools.values()
    ]
    return create_sdk_mcp_server(
        name=name,
        version=version,
        tools=tool_functions,
    )


def wrap_tool_for_claude(
    tool: Tool,
    input_overrides: Optional[Dict[str, Dict[str, Any]]],
) -> Callable:
    from claude_agent_sdk import tool as claude_tool_decorator

    input_schema = build_input_schema(tool)
    tool_description = (
        f"{tool.config.description}\n\n"
        f"Input description: {tool.config.input_description or 'No input description provided'}\n"
        f"Output description: {tool.config.output_description or 'No output description provided'}"
    )

    @claude_tool_decorator(tool.name, tool_description, input_schema)
    async def _invoke(args: Dict[str, Any]) -> Dict[str, Any]:
        context = ToolContext(input=args)
        _resolve_runtime_overrides(
            context=context,
            input_overrides=input_overrides,
            tool_name=tool.name,
        )
        result = await tool.run(context)
        output = result.output
        if isinstance(output, str):
            text = output
        else:
            try:
                text = json.dumps(output, default=str)
            except TypeError:
                text = str(output)
        return {"content": [{"type": "text", "text": text}]}

    return _invoke


def build_input_schema(tool: Tool) -> Dict[str, Any]:
    model = tool.config.input_model
    if not model:
        return {"type": "object", "properties": {}}

    json_schema = model.model_json_schema()
    json_schema = _resolve_schema_refs(json_schema)
    json_schema.pop("$defs", None)
    return json_schema


def _resolve_schema_refs(
    schema: Any,
    defs: Optional[Dict[str, Any]] = None,
) -> Any:
    """
    Example:
        # Input (as produced by Pydantic):
        {
            "type": "object",
            "properties": {
                "address": {"$ref": "#/$defs/Address"}
            },
            "$defs": {
                "Address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city":   {"type": "string"}
                    }
                }
            }
        }

        # Output (after _resolve_schema_refs + deleting "$defs"):
        {
            "type": "object",
            "properties": {
                "address": {
                    "type": "object",
                    "properties": {
                        "street": {"type": "string"},
                        "city":   {"type": "string"}
                    }
                }
            }
        }
    """
    if defs is None and isinstance(schema, dict):
        defs = schema.get("$defs", {})

    if isinstance(schema, dict):
        if "$ref" in schema:
            ref_path = schema["$ref"]
            if ref_path.startswith("#/$defs/") and defs:
                def_name = ref_path.split("/")[-1]
                if def_name in defs:
                    return _resolve_schema_refs(defs[def_name].copy(), defs)
            return schema
        return {k: _resolve_schema_refs(v, defs) for k, v in schema.items()}
    elif isinstance(schema, list):
        return [_resolve_schema_refs(item, defs) for item in schema]
    return schema


def _resolve_runtime_overrides(
    context: ToolContext,
    input_overrides: Optional[Dict[str, Dict[str, Any]]],
    tool_name: str,
) -> None:
    """
    Apply runtime layers of the override merge for a single tool invocation.

    global overrides => input_overrides["__global__"]
    tool overrides   => input_overrides[tool_name]

    author tool overrides are applied after this (last) when tool.run is called
    """
    overrides = input_overrides or {}
    context.apply_input_overrides(overrides.get("__global__", {}))
    context.apply_input_overrides(overrides.get(tool_name, {}))
