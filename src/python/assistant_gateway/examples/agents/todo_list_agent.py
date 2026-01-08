import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(".")
sys.path.append("..")
sys.path.append("../..")

import os
import asyncio
from typing import Optional, List
from pydantic import Field
import dotenv
from claude_agent_sdk import ClaudeAgentOptions
from pydantic import BaseModel

from assistant_gateway.agents.claude import ClaudeBaseAgent
from assistant_gateway.tools.rest_tool import RESTTool
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.base import ToolContext
from assistant_gateway.schemas import Message, Role
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.rest_tool import RESTTool, RestToolContext


dotenv.load_dotenv()

# DEFAULT_MODEL = "claude-haiku-4-5-20251001"
DEFAULT_MODEL = "claude-sonnet-4-5-20250929"

# sample widget id for user1: 690e2f69de08bc3bed3ce385


class GetTodoListQueryParamsModel(BaseModel):
    widgetId: str


class AddTodoItemDataPayloadModel(BaseModel):
    content: str = Field(description="The content of the todo item")


class GetTodoListRESTTool(RESTTool):
    def __init__(self):
        super().__init__(
            name="get_todo_list",
            description="""Get the todo list for a given widgetId from the Space API 
            The API endpoint is GET /api/widgets/todo/{widgetId}
            """,
            query_params_model=GetTodoListQueryParamsModel,
        )


class AddTodoItemRESTTool(RESTTool):
    def __init__(self):
        super().__init__(
            name="add_todo_item",
            description="""Add a new todo item to the todo list for a given widgetId from the Space API 
            The API endpoint is POST /api/widgets/todo/{widgetId}
            """,
            data_payload_model=AddTodoItemDataPayloadModel,
        )


TODO_API_REST_TOOLS = [
    GetTodoListRESTTool(),
    AddTodoItemRESTTool(),
]

# temporary hardcoded token, will come dynamically later
JWT_BEARER_TOKEN = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJ1c2VySWQiOiIzOGUyM2FiMS1kNzNlLTQ2NjYtOTExYi0yNWNkZmRlZGMwY2UiLCJ1c2VybmFtZSI6InVzZXIxIiwiYXV0aE1ldGhvZElkIjoiMzBjMzI3ZDQtOTkxMC00NjI5LTgyYTktZWZjNGNmN2NhZDgwIiwidG9rZW5QdXJwb3NlIjoidXNlci1hdXRoIiwiaWF0IjoxNzY1NzE2Mjc3LCJleHAiOjE3NjU3MTk4Nzd9.61W1ctlUzS9EVmXkAjUaYv1hA0U0nBBiVMAAnTsgt_k"
PREDEFINED_TOOL_CONTEXT = RestToolContext(
    backend_url=os.environ.get("BACKEND_URL"),
    default_headers={
        "Authorization": f"Bearer {JWT_BEARER_TOKEN}",
    },
)


class ClaudeTodoListAgent(ClaudeBaseAgent):
    def __init__(
        self,
        api_key: Optional[str] = os.environ.get("ANTHROPIC_API_KEY"),
        model: str = DEFAULT_MODEL,
    ) -> None:
        super().__init__(api_key)

        space_todo_list_mcp_server, space_todo_list_tool_functions = (
            self.get_mcp_server_config(
                name="space-todo-list-agent",
                version="0.1.0",
                tool_registry=self.tool_registry,
                predefined_tool_context=PREDEFINED_TOOL_CONTEXT,
            )
        )

        # Use with Claude
        self._mcp_server_options = ClaudeAgentOptions(
            model=model or DEFAULT_MODEL,
            mcp_servers={"space-todo-list": space_todo_list_mcp_server},
            system_prompt="You are a helpful space todo list assistant. Use the available tools to add and get todo items for a given widgetId from the Space API.",
            allowed_tools=[
                "mcp__space-todo-list__get_todo_list",
                "mcp__space-todo-list__add_todo_item",
            ],
        )

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        return self._mcp_server_options

    @property
    def tool_registry(self) -> ToolRegistry:
        registry = ToolRegistry()
        for tool in TODO_API_REST_TOOLS:
            registry.register(tool)
        return registry


######################################################
################# Interactive CLI ####################
######################################################


async def main():
    """Interactive CLI for the CalculatorAgent."""
    # Get API key from environment or prompt
    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        api_key = input("Enter your Anthropic API key: ").strip()
        if not api_key:
            print("Error: API key is required.")
            return

    # Get model from environment or use default
    model = os.environ.get("CLAUDE_MODEL", DEFAULT_MODEL)

    # Initialize the agent
    print("\n" + "=" * 50)
    print("  Space Todo ListAgent (Claude)")
    print(f"  Model: {model}")
    print("  Tools: get_todo_list")
    print("=" * 50)
    print("\nType your todo list questions or 'quit' to exit.\n")

    agent = ClaudeTodoListAgent(api_key=api_key, model=model)

    # Conversation history
    messages: List[Message] = []

    while True:
        try:
            # Get user input
            user_input = input("You: ").strip()

            if not user_input:
                continue

            if user_input.lower() in ("quit", "exit", "q"):
                print("\nGoodbye!")
                break

            # Add user message to history
            messages.append(Message(role=Role.user, content=user_input))

            # Get response from agent
            response = await agent.run(messages=messages)

            # Display the response
            if response.final_text:
                print(f"\nAssistant: {response.final_text}\n")
            elif response.messages:
                for msg in response.messages:
                    print(f"\nAssistant: {msg.content}\n")
            else:
                print("\nAssistant: (no response)\n")

            # Show tool calls if any
            if response.steps:
                for step in response.steps:
                    if step.tool_calls:
                        for tc in step.tool_calls:
                            print(f"  [Tool: {tc.name}, Input: {tc.input}]")

            # Add assistant response to history
            if response.messages:
                messages.extend(response.messages)

        except KeyboardInterrupt:
            print("\n\nInterrupted. Goodbye!")
            break
        except Exception as e:
            print(f"\nError: {e}\n")
            raise e


if __name__ == "__main__":
    asyncio.run(main())
