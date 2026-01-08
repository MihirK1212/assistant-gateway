from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple
from claude_agent_sdk import McpSdkServerConfig
from claude_agent_sdk import ClaudeAgentOptions
from claude_agent_sdk import ClaudeSDKClient

from assistant_gateway.agents.base import Agent
from assistant_gateway.tools.base import Tool, ToolContext
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.schemas import (
    AgentInteraction,
    AgentOutput,
    Role,
    AgentStep,
    ToolCall,
    ToolResult,
    UserInput,
)


class ClaudeBaseAgent(Agent):
    """
    Adapter that prepares tools from ``ToolRegistry`` for the ClaudeAgent SDK.

    The actual conversation loop still needs to be implemented, but the
    infrastructure for translating registry entries into ``@tool``-decorated
    callables lives here.
    """

    def __init__(self, api_key: Optional[str]) -> None:
        super().__init__()
        self.api_key = api_key

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        """
        Get the MCP server options for the Claude agent.
        These options will be used to instantiate the ClaudeSDKClient.
        You can make use of the get_mcp_server_config to get the MCP server config,
        and then combine multiple server configs into a single options object.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @classmethod
    def get_mcp_server_config(
        cls,
        name: str,
        version: str,
        tool_registry: ToolRegistry,
        predefined_tool_context: ToolContext,
    ) -> Tuple[McpSdkServerConfig, List[Callable]]:
        """
        Translate the registry into Claude SDK ``@tool`` callables and register
        them against an MCP server instance.

        Args:
                tool_registry: The tool registry to use.
                predefined_tool_context: The global tool context to use.

        Returns:
                A tuple containing the MCP server and the tool functions.
        """
        from claude_agent_sdk import create_sdk_mcp_server

        tool_functions = [
            cls._wrap_tool_for_claude(tool, predefined_tool_context)
            for tool in tool_registry.all()
        ]
        server = create_sdk_mcp_server(
            name=name,
            version=version,
            tools=tool_functions,
        )
        return server, tool_functions

    async def run(self, interactions: List[AgentInteraction]) -> AgentOutput:
        mcp_server_options = self.get_mcp_server_options()

        print('interactions inside claude base agent', interactions)

        # Convert messages to Claude SDK format
        # Extract content using helper method that handles different AgentInteraction subclasses
        claude_messages = []
        for msg in interactions:
            if msg.role not in (Role.user, Role.assistant):
                continue
            content_text = self._get_interaction_content(msg)
            if content_text is None:
                continue
            claude_messages.append(
                {"role": msg.role.value, "content": self._stringify(content_text)}
            )

        prompt = claude_messages[-1]["content"] if claude_messages else ""

        print('prompt inside claude base agent', prompt)

        # Call Claude with the configured MCP server options using ClaudeSDKClient
        # Collect all messages from the stream for proper parsing
        all_messages: List[Any] = []
        async with ClaudeSDKClient(options=mcp_server_options) as client:
            await client.query(prompt)
            async for message in client.receive_response():
                all_messages.append(message)

        print('all_messages inside claude base agent', all_messages)

        # Parse all messages into message, steps and result text in order to return an AgentOutput
        assistant_messages: List[str] = []
        steps: List[AgentStep] = []
        result_text: Optional[str] = None

        # Track tool calls by ID for associating results with their calls
        tool_call_names: Dict[str, str] = {}

        for message in all_messages:
            # Handle AssistantMessage - has content list with ContentBlocks
            if self._is_assistant_message(message):
                content_blocks = self._get_value(message, "content", [])
                if not isinstance(content_blocks, list):
                    continue
                step_thought: Optional[str] = None
                step_tool_calls: List[ToolCall] = []
                step_tool_results: List[ToolResult] = []
                step_messages: List[str] = []

                for content_block in content_blocks:
                    # TextBlock - has 'text' attribute
                    if self._is_text_block(content_block):
                        text = self._get_value(content_block, "text")
                        if text is not None:
                            text_str = self._stringify(text)
                            step_messages.append(text_str)

                    # ThinkingBlock - has 'thinking' and 'signature' attributes
                    elif self._is_thinking_block(content_block):
                        thinking = self._get_value(content_block, "thinking")
                        if thinking is not None:
                            step_thought = self._stringify(thinking)

                    # ToolUseBlock - has 'id', 'name', 'input' attributes
                    elif self._is_tool_use_block(content_block):
                        call_id = self._stringify(self._get_value(content_block, "id"))
                        if not call_id:
                            continue
                        tool_name = self._stringify(
                            self._get_value(content_block, "name")
                        )
                        input_payload = (
                            self._get_value(content_block, "input", {}) or {}
                        )
                        if not isinstance(input_payload, dict):
                            try:
                                input_payload = dict(input_payload)
                            except Exception:
                                input_payload = {"value": input_payload}
                        tool_call = ToolCall(
                            id=call_id,
                            name=tool_name,
                            input=input_payload,
                        )
                        step_tool_calls.append(tool_call)
                        # Track tool name for later result association
                        tool_call_names[call_id] = tool_name

                    # ToolResultBlock - has 'tool_use_id', 'content', 'is_error' attributes
                    elif self._is_tool_result_block(content_block):
                        tool_use_id = self._stringify(
                            self._get_value(content_block, "tool_use_id")
                        )
                        output_content = self._get_value(content_block, "content")
                        tool_result = ToolResult(
                            tool_call_id=tool_use_id or None,
                            output=output_content,
                            name=(
                                tool_call_names.get(tool_use_id)
                                if tool_use_id
                                else None
                            ),
                            is_error=bool(
                                self._get_value(content_block, "is_error", False)
                                or False
                            ),
                            raw_response=content_block,
                        )
                        step_tool_results.append(tool_result)

                # Create assistant message if we have text
                # AgentOutput.messages is List[str], so append the text directly
                if step_messages:
                    assistant_messages.append("\n".join(step_messages))

                # Create an AgentStep if we have thought, tool calls, or tool results
                if step_thought or step_tool_calls or step_tool_results:
                    step = AgentStep(
                        thought=step_thought,
                        tool_calls=step_tool_calls,
                        tool_results=step_tool_results,
                    )
                    steps.append(step)

            # Handle ResultMessage - has 'result', 'is_error', 'total_cost_usd', etc.
            elif self._is_result_message(message):
                result_value = self._get_value(message, "result")
                if result_value:
                    result_text = self._stringify(result_value)

            # Handle SystemMessage - has 'subtype' and 'data' attributes
            elif self._is_system_message(message):
                # System messages are metadata, not included in response messages
                pass

            # Handle UserMessage - has 'content' attribute (str or list)
            elif self._is_user_message(message):
                # User messages from the stream are typically echoes, skip them
                pass

        # If we have a result from ResultMessage, use that as final_text
        # AgentOutput.messages is List[str], so compare and append strings directly
        if result_text:
            final_text = result_text
            if not assistant_messages or assistant_messages[-1] != result_text:
                assistant_messages.append(result_text)
        else:
            final_text = "\n".join(assistant_messages) if assistant_messages else None

        return AgentOutput(
            role=Role.assistant,
            messages=assistant_messages,
            steps=steps,
            final_text=final_text,
        )

    @classmethod
    def _get_interaction_content(cls, interaction: AgentInteraction) -> str:
        """
        Extract content from an AgentInteraction.

        Handles:
        - UserInput: has .content attribute
        - StoredAgentInteraction: has metadata['content']
        - Other AgentInteraction subclasses: check for content in metadata or direct attribute
        """
        # Check if it's a UserInput with direct content attribute
        if isinstance(interaction, UserInput):
            return cls._stringify(interaction.content)

        # Check for messages on an AgentOutput-like interaction
        if hasattr(interaction, "messages"):
            msgs = getattr(interaction, "messages")
            if isinstance(msgs, list) and msgs:
                # Join text parts to form a single content string
                joined = "\n".join(cls._stringify(m) for m in msgs if m is not None)
                if joined:
                    return joined

        # Check for final_text on an AgentOutput-like interaction
        if hasattr(interaction, "final_text"):
            final_text = getattr(interaction, "final_text")
            if final_text:
                return cls._stringify(final_text)

        # Check for content in metadata (StoredAgentInteraction pattern)
        if hasattr(interaction, "metadata") and isinstance(interaction.metadata, dict):
            content = interaction.metadata.get("content")
            if content is not None:
                return cls._stringify(content)

        # Fallback: check for direct content attribute (for flexibility)
        if hasattr(interaction, "content"):
            return cls._stringify(interaction.content)

        return ""

    @staticmethod
    def _has_attr_or_key(obj: Any, key: str) -> bool:
        return hasattr(obj, key) or (isinstance(obj, dict) and key in obj)

    @staticmethod
    def _get_value(obj: Any, key: str, default: Any = None) -> Any:
        if isinstance(obj, dict):
            return obj.get(key, default)
        return getattr(obj, key, default)

    @staticmethod
    def _stringify(value: Any) -> str:
        if value is None:
            return ""
        try:
            return str(value)
        except Exception:
            return ""

    @staticmethod
    def _is_assistant_message(message: Any) -> bool:
        """Check if message is an AssistantMessage (has content list and model)."""
        return ClaudeBaseAgent._has_attr_or_key(message, "content") and isinstance(
            ClaudeBaseAgent._get_value(message, "content"), list
        )

    @staticmethod
    def _is_result_message(message: Any) -> bool:
        """Check if message is a ResultMessage (has subtype, duration_ms, is_error, etc.)."""
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "subtype")
            and ClaudeBaseAgent._has_attr_or_key(message, "duration_ms")
            and ClaudeBaseAgent._has_attr_or_key(message, "is_error")
            and ClaudeBaseAgent._has_attr_or_key(message, "num_turns")
        )

    @staticmethod
    def _is_system_message(message: Any) -> bool:
        """Check if message is a SystemMessage (has subtype and data, but not ResultMessage fields)."""
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "subtype")
            and ClaudeBaseAgent._has_attr_or_key(message, "data")
            and not ClaudeBaseAgent._has_attr_or_key(message, "duration_ms")
        )

    @staticmethod
    def _is_user_message(message: Any) -> bool:
        """Check if message is a UserMessage (has content but not model or subtype)."""
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "content")
            and not ClaudeBaseAgent._has_attr_or_key(message, "model")
            and not ClaudeBaseAgent._has_attr_or_key(message, "subtype")
        )

    @staticmethod
    def _is_text_block(block: Any) -> bool:
        """Check if content block is a TextBlock."""
        return ClaudeBaseAgent._has_attr_or_key(
            block, "text"
        ) and not ClaudeBaseAgent._has_attr_or_key(block, "thinking")

    @staticmethod
    def _is_thinking_block(block: Any) -> bool:
        """Check if content block is a ThinkingBlock."""
        return ClaudeBaseAgent._has_attr_or_key(
            block, "thinking"
        ) and ClaudeBaseAgent._has_attr_or_key(block, "signature")

    @staticmethod
    def _is_tool_use_block(block: Any) -> bool:
        """Check if content block is a ToolUseBlock."""
        return (
            ClaudeBaseAgent._has_attr_or_key(block, "id")
            and ClaudeBaseAgent._has_attr_or_key(block, "name")
            and ClaudeBaseAgent._has_attr_or_key(block, "input")
            and not ClaudeBaseAgent._has_attr_or_key(block, "tool_use_id")
        )

    @staticmethod
    def _is_tool_result_block(block: Any) -> bool:
        """Check if content block is a ToolResultBlock."""
        return ClaudeBaseAgent._has_attr_or_key(block, "tool_use_id")

    @classmethod
    def _wrap_tool_for_claude(cls, tool: Tool, predefined_tool_context: ToolContext):
        from claude_agent_sdk import tool as claude_tool_decorator

        tool_input_schema = cls._build_input_schema(tool)
        # print(f"tool input schema: {tool_input_schema}")

        @claude_tool_decorator(tool.name, tool.config.description, tool_input_schema)
        async def _invoke(args: Dict[str, Any]):
            tool_context_with_input = predefined_tool_context.with_input(args)
            result = await tool.run(tool_context_with_input)
            output = result.output
            if isinstance(output, str):
                text = output
            else:
                try:
                    text = json.dumps(output, default=str)
                except TypeError:
                    text = str(output)
            return {
                "content": [
                    {
                        "type": "text",
                        "text": text,
                    }
                ]
            }

        return _invoke

    @classmethod
    def _build_input_schema(cls, tool: Tool) -> Dict[str, Any]:
        """Build a proper JSON Schema from the tool's input model."""
        model = tool.config.input_model
        if not model:
            return {"type": "object", "properties": {}}

        # Use Pydantic's built-in JSON schema generation
        json_schema = model.model_json_schema()

        # Filter out fields we don't want to expose to the tool input
        excluded_fields = {}
        if "properties" in json_schema:
            json_schema["properties"] = {
                k: v
                for k, v in json_schema["properties"].items()
                if k not in excluded_fields
            }
        if "required" in json_schema:
            json_schema["required"] = [
                r for r in json_schema["required"] if r not in excluded_fields
            ]

        # Resolve $defs references inline for simpler schema
        json_schema = cls._resolve_schema_refs(json_schema)

        # Remove $defs after resolving
        if "$defs" in json_schema:
            del json_schema["$defs"]

        return json_schema

    @classmethod
    def _resolve_schema_refs(
        cls, schema: Dict[str, Any], defs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """Recursively resolve $ref references in JSON Schema."""
        if defs is None:
            defs = schema.get("$defs", {})

        if isinstance(schema, dict):
            # Handle $ref
            if "$ref" in schema:
                ref_path = schema["$ref"]
                # Extract the definition name from "#/$defs/DefinitionName"
                if ref_path.startswith("#/$defs/"):
                    def_name = ref_path.split("/")[-1]
                    if def_name in defs:
                        # Return a copy of the resolved definition (recursively resolve it too)
                        return cls._resolve_schema_refs(defs[def_name].copy(), defs)
                return schema

            # Recursively resolve all dict values
            return {k: cls._resolve_schema_refs(v, defs) for k, v in schema.items()}
        elif isinstance(schema, list):
            return [cls._resolve_schema_refs(item, defs) for item in schema]
        else:
            return schema
