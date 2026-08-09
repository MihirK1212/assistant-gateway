from __future__ import annotations

import json
from typing import Any, Callable, Dict, List, Optional, Tuple

from assistant_gateway.agents.base import Agent
from assistant_gateway.schemas import (
    AgentInteraction,
    AgentOutput,
    AgentStep,
    Role,
    ToolCall,
    ToolResult,
    UserInput,
)
from assistant_gateway.tools.base import Tool, ToolContext
from assistant_gateway.tools.registry import ToolRegistry
from claude_agent_sdk import ClaudeAgentOptions, ClaudeSDKClient, McpSdkServerConfig


class ClaudeBaseAgent(Agent):
    """
    Base class that can be used to create a Claude agent. 

    It provides the utility for creating an MCP config from a tool registry, and wrapping tools for the Claude SDK.
    The actual agent configuration (mcp server options) needs to be implemented by the subclass.
    """

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        """
        Combine multiple server configs into a single options object.
        """
        raise NotImplementedError("Subclasses must implement this method")

    @classmethod
    def get_mcp_server_config(
        cls,
        name: str,
        version: str,
        tool_registry: ToolRegistry,
        agent_level_input_overrides: Optional[Dict[str, Any]] = None,
    ) -> Tuple[McpSdkServerConfig, List[Callable]]:
        from claude_agent_sdk import create_sdk_mcp_server

        tool_functions = [
            cls._wrap_tool_for_claude(tool, agent_level_input_overrides)
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

        sorted_interactions = sorted(
            interactions,
            key=lambda m: (m.sequence_id is None, m.sequence_id, m.created_at),
        )

        last_user_input = next(
            (m for m in reversed(sorted_interactions) if isinstance(m, UserInput)),
            None,
        )
        if not last_user_input:
            raise ValueError("interactions must contain at least one UserInput")

        claude_messages = []
        for msg in sorted_interactions:
            if msg.role not in (Role.user, Role.assistant):
                continue
            content_text = self._get_interaction_content(msg)
            if content_text is None:
                continue
            claude_messages.append(
                {"role": msg.role.value, "content": self._stringify(content_text)}
            )

        last_user_message = next(
            (m for m in reversed(claude_messages) if m["role"] == Role.user.value),
            None,
        )
        prompt = last_user_message["content"] if last_user_message else ""

        all_messages: List[Any] = []
        async with ClaudeSDKClient(options=mcp_server_options) as client:
            await client.query(prompt)
            async for message in client.receive_response():
                all_messages.append(message)
        
        assistant_messages: List[str] = []
        steps: List[AgentStep] = []
        result_text: Optional[str] = None

        tool_call_names: Dict[str, str] = {}

        """
        The messages received can be of the following types: 

        1. assistant message: 
            - has content list with content blocks
            - content blocks can be: 
                - text block => has 'text' attribute
                - thinking block => has 'thinking' and 'signature' attributes
                - tool use block => has 'id', 'name', 'input' attributes
                - tool result block => has 'tool_use_id', 'content', 'is_error' attributes

        2. result message:
            - has 'result', 'is_error', 'total_cost_usd', 'duration_ms', 'num_turns' attributes

        3. system message:
            - has 'subtype' and 'data' attributes

        4. user message:
            - has 'content' attribute
        """

        for message in all_messages:
            if self._is_assistant_message(message):
                content_blocks = self._get_value(message, "content", [])
                if not isinstance(content_blocks, list):
                    continue

                step_thought: Optional[str] = None
                step_tool_calls: List[ToolCall] = []
                step_tool_results: List[ToolResult] = []
                step_messages: List[str] = []

                for content_block in content_blocks:
                    if self._is_text_block(content_block):
                        text = self._get_value(content_block, "text")
                        if text is not None:
                            text_str = self._stringify(text)
                            step_messages.append(text_str)

                    elif self._is_thinking_block(content_block):
                        thinking = self._get_value(content_block, "thinking")
                        if thinking is not None:
                            step_thought = self._stringify(thinking)

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
                        tool_call_names[call_id] = tool_name

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

                if step_messages:
                    assistant_messages.append("\n".join(step_messages))

                if step_thought or step_tool_calls or step_tool_results:
                    step = AgentStep(
                        thought=step_thought,
                        tool_calls=step_tool_calls,
                        tool_results=step_tool_results,
                    )
                    steps.append(step)

            elif self._is_result_message(message):
                result_value = self._get_value(message, "result")
                if result_value:
                    result_text = self._stringify(result_value)

            elif self._is_system_message(message):
                pass

            elif self._is_user_message(message):
                pass

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
            user_input_interaction_id=last_user_input.id,
        )

    @classmethod
    def _get_interaction_content(cls, interaction: AgentInteraction) -> str:
        """
        Extract the string content from an AgentInteraction. 
        An interaction can either be a UserInput or an AgentOutput. 

        UserInput => use the content attribute to get the string content 

        Else:
            - check if the interaction has a messages attribute. if yes, join the messages and return the joined string 
            - check if the interaction has a final_text attribute. if yes, return the final_text
        """
        if isinstance(interaction, UserInput):
            return cls._stringify(interaction.content)

        if hasattr(interaction, "messages"):
            msgs = getattr(interaction, "messages")
            if isinstance(msgs, list) and msgs:
                joined = "\n".join(cls._stringify(m) for m in msgs if m is not None)
                if joined:
                    return joined

        if hasattr(interaction, "final_text"):
            final_text = getattr(interaction, "final_text")
            if final_text:
                return cls._stringify(final_text)

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
        return ClaudeBaseAgent._has_attr_or_key(message, "content") and isinstance(
            ClaudeBaseAgent._get_value(message, "content"), list
        )

    @staticmethod
    def _is_result_message(message: Any) -> bool:
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "subtype")
            and ClaudeBaseAgent._has_attr_or_key(message, "duration_ms")
            and ClaudeBaseAgent._has_attr_or_key(message, "is_error")
            and ClaudeBaseAgent._has_attr_or_key(message, "num_turns")
        )

    @staticmethod
    def _is_system_message(message: Any) -> bool:
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "subtype")
            and ClaudeBaseAgent._has_attr_or_key(message, "data")
            and not ClaudeBaseAgent._has_attr_or_key(message, "duration_ms")
        )

    @staticmethod
    def _is_user_message(message: Any) -> bool:
        return (
            ClaudeBaseAgent._has_attr_or_key(message, "content")
            and not ClaudeBaseAgent._has_attr_or_key(message, "model")
            and not ClaudeBaseAgent._has_attr_or_key(message, "subtype")
        )

    @staticmethod
    def _is_text_block(block: Any) -> bool:
        return ClaudeBaseAgent._has_attr_or_key(
            block, "text"
        ) and not ClaudeBaseAgent._has_attr_or_key(block, "thinking")

    @staticmethod
    def _is_thinking_block(block: Any) -> bool:
        return ClaudeBaseAgent._has_attr_or_key(
            block, "thinking"
        ) and ClaudeBaseAgent._has_attr_or_key(block, "signature")

    @staticmethod
    def _is_tool_use_block(block: Any) -> bool:
        return (
            ClaudeBaseAgent._has_attr_or_key(block, "id")
            and ClaudeBaseAgent._has_attr_or_key(block, "name")
            and ClaudeBaseAgent._has_attr_or_key(block, "input")
            and not ClaudeBaseAgent._has_attr_or_key(block, "tool_use_id")
        )

    @staticmethod
    def _is_tool_result_block(block: Any) -> bool:
        return ClaudeBaseAgent._has_attr_or_key(block, "tool_use_id")

    @classmethod
    def _wrap_tool_for_claude(
        cls, tool: Tool, agent_level_input_overrides: Optional[Dict[str, Any]] = None
    ):
        from claude_agent_sdk import tool as claude_tool_decorator

        tool_input_schema = cls._build_input_schema(tool)

        tool_description = f'''
        {tool.config.description}

        Input description: {tool.config.input_description or "No input description provided"}
        Output description: {tool.config.output_description or "No output description provided"}
        '''

        print(f"tool description: {tool_description}")

        @claude_tool_decorator(tool.name, tool_description, tool_input_schema)
        async def _invoke(args: Dict[str, Any]):
            tool_context = ToolContext(input=args).apply_input_overrides(
                agent_level_input_overrides
            )
            result = await tool.execute(tool_context)
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
        model = tool.config.input_model
        if not model:
            return {"type": "object", "properties": {}}

        json_schema = model.model_json_schema()

        json_schema = cls._resolve_schema_refs(json_schema)

        if "$defs" in json_schema:
            del json_schema["$defs"]

        return json_schema

    @classmethod
    def _resolve_schema_refs(
        cls, schema: Dict[str, Any], defs: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
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
        if defs is None:
            defs = schema.get("$defs", {})

        if isinstance(schema, dict):
            if "$ref" in schema:
                ref_path = schema["$ref"]
                if ref_path.startswith("#/$defs/"):
                    def_name = ref_path.split("/")[-1]
                    if def_name in defs:
                        return cls._resolve_schema_refs(defs[def_name].copy(), defs)
                return schema
            return {k: cls._resolve_schema_refs(v, defs) for k, v in schema.items()}
        elif isinstance(schema, list):
            return [cls._resolve_schema_refs(item, defs) for item in schema]
        else:
            return schema
