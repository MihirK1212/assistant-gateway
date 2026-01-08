from __future__ import annotations

import uuid
from typing import List

from assistant_gateway.agents.base import Agent
from assistant_gateway.schemas import (
    AgentStep,
    AssistantResponse,
    Message,
    Role,
    ToolCall,
    ToolResult,
)
from assistant_gateway.tools.base import ToolContext
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.rest_tool import RESTTool


def register_basic_crud_tools(registry: ToolRegistry) -> None:
    """
    Register simple, composable tools for a typical TODO CRUD API.
    This is optional since the generic 'crud.rest' tool exists,
    but named tools improve agent affordances.
    """
    registry.register(RESTTool(name="todo.list", description="List todos. GET /todos"))
    registry.register(
        RESTTool(name="todo.get", description="Get a todo by id. GET /todos/{id}")
    )
    registry.register(
        RESTTool(name="todo.create", description="Create a todo. POST /todos")
    )
    registry.register(
        RESTTool(
            name="todo.update", description="Update a todo. PUT or PATCH /todos/{id}"
        )
    )
    registry.register(
        RESTTool(name="todo.delete", description="Delete a todo. DELETE /todos/{id}")
    )


def register_default_crud_suite(registry: ToolRegistry) -> ToolRegistry:
    """
    Register the generic CRUD REST tool plus the optional named affordances.
    """

    if registry.get("crud.rest") is None:
        registry.register(
            RESTTool(
                name="crud.rest",
                description="Call the CRUD backend using arbitrary HTTP method/path.",
            )
        )
    register_basic_crud_tools(registry)
    return registry


class SimpleEchoAgent(Agent):
    """
    A minimal agent that:
    - Echos the latest user message
    - Demonstrates a single tool call if the user starts with 'tool:' and passes JSON after it
    """

    @property
    def tool_registry(self) -> ToolRegistry:
        registry = ToolRegistry()
        if not list(registry.all()):
            register_default_crud_suite(registry)
        return registry

    async def run(self, messages: List[Message]) -> AssistantResponse:
        last_user = next((m for m in reversed(messages) if m.role == Role.user), None)
        if not last_user:
            return AssistantResponse(
                messages=[
                    Message(role=Role.assistant, content="How can I help you today?")
                ]
            )

        content = last_user.content.strip()
        steps: List[AgentStep] = []
        tool_results: List[ToolResult] = []

        # Quick demo: "tool: <name> {json}"
        if content.lower().startswith("tool:"):
            try:
                rest = content[5:].strip()
                name, json_part = rest.split(" ", 1)
                import json as _json

                payload = _json.loads(json_part)
                call_id = str(uuid.uuid4())
                tool_call = ToolCall(id=call_id, name=name, input=payload)
                steps.append(
                    AgentStep(
                        thought="Calling a tool as requested.", tool_calls=[tool_call]
                    )
                )
                tool = self.tool_registry.get(name)
                if not tool:
                    final = f"Unknown tool: {name}"
                    return AssistantResponse(
                        messages=[Message(role=Role.assistant, content=final)],
                        steps=steps,
                        tool_results=tool_results,
                        final_text=final,
                    )
                tool_ctx_with_input = ToolContext().with_input(payload)
                result = await tool.run(tool_ctx_with_input)
                result.tool_call_id = call_id
                tool_results.append(result)
                final_text = f"Tool {name} result: {result.output}"
                return AssistantResponse(
                    messages=[Message(role=Role.assistant, content=final_text)],
                    steps=steps,
                    tool_results=tool_results,
                    final_text=final_text,
                )
            except Exception as e:
                err = f"Failed to parse or execute tool call: {e}"
                return AssistantResponse(
                    messages=[Message(role=Role.assistant, content=err)]
                )

        # Default: echo
        reply = f"You said: {content}"
        return AssistantResponse(
            messages=[Message(role=Role.assistant, content=reply)],
            steps=steps,
            tool_results=tool_results,
            final_text=reply,
        )
