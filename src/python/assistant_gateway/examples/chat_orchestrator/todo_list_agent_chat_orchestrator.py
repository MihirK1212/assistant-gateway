import sys
import os

# Allow running this file directly without installing the package.
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(CURRENT_DIR)))  # assistant_gateway/
sys.path.append(os.path.dirname(CURRENT_DIR))  # assistant_gateway/examples/
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
)  # repo root

import asyncio
from typing import Dict, Optional

import dotenv
from claude_agent_sdk import ClaudeAgentOptions
from pydantic import BaseModel, Field


from assistant_gateway.agents.claude import ClaudeBaseAgent
from assistant_gateway.chat_orchestrator.chat.store import InMemoryChatStore
from assistant_gateway.chat_orchestrator.core.config import (
    AgentConfig,
    GatewayConfig,
    GatewayDefaultFallbackConfig,
)
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    UserContext,
)
from assistant_gateway.chat_orchestrator.orchestration.orchestrator import (
    ConversationOrchestrator,
)
from assistant_gateway.chat_orchestrator.tasks_queue_manager import (
    InMemoryTasksQueueManager,
)
from assistant_gateway.schemas import AssistantResponse, Role
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.rest_tool import RESTTool, RestToolContext

dotenv.load_dotenv()

# Default Claude model; override with CLAUDE_MODEL env var if desired.
DEFAULT_MODEL = os.environ.get("CLAUDE_MODEL", "claude-sonnet-4-5-20250929")


# ==== REST tools (duplicated from the agent example for clarity) ====


class GetTodoListQueryParamsModel(BaseModel):
    widgetId: str


class AddTodoItemDataPayloadModel(BaseModel):
    content: str = Field(description="The content of the todo item")


class GetTodoListRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="get_todo_list",
            description=(
                "Get the todo list for a given widgetId from the Space API. "
                "Endpoint: GET /api/widgets/todo/{widgetId}"
            ),
            query_params_model=GetTodoListQueryParamsModel,
        )


class AddTodoItemRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="add_todo_item",
            description=(
                "Add a new todo item to the todo list for a given widgetId from the "
                "Space API. Endpoint: POST /api/widgets/todo/{widgetId}"
            ),
            data_payload_model=AddTodoItemDataPayloadModel,
        )


def build_tool_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(GetTodoListRESTTool())
    registry.register(AddTodoItemRESTTool())
    return registry


# ==== Agent that accepts dynamic context ====


class DynamicClaudeTodoListAgent(ClaudeBaseAgent):
    """
    Claude agent wired with the todo REST tools.

    The tool context (backend URL + headers) is injected at construction time so it
    can be derived from the chat_orchestrator GatewayConfig builder arguments.
    """

    def __init__(
        self,
        *,
        api_key: str,
        model: Optional[str],
        tool_context: RestToolContext,
    ) -> None:
        super().__init__(api_key)
        self._tool_context = tool_context
        self._model = model or DEFAULT_MODEL
        self._tool_registry = build_tool_registry()

        server, _ = self.get_mcp_server_config(
            name="space-todo-list-agent",
            version="0.1.0",
            tool_registry=self._tool_registry,
            predefined_tool_context=self._tool_context,
        )

        self._options = ClaudeAgentOptions(
            model=self._model,
            mcp_servers={"space-todo-list": server},
            system_prompt=(
                "You are a helpful space todo list assistant. Use the available tools "
                "to add and get todo items for a given widgetId from the Space API."
            ),
            allowed_tools=[
                "mcp__space-todo-list__get_todo_list",
                "mcp__space-todo-list__add_todo_item",
            ],
        )

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        return self._options


# ==== AgentConfig builder ====


def build_todo_agent(
    user_context: Optional[UserContext],
    backend_server_context: Optional[BackendServerContext],
    default_fallback_config: Optional[GatewayDefaultFallbackConfig],
) -> DynamicClaudeTodoListAgent:
    """
    Create a todo-list agent using dynamic inputs supplied by the orchestrator.
    """

    backend_url = (
        (backend_server_context.base_url if backend_server_context else None)
        or (
            default_fallback_config.fallback_backend_url
            if default_fallback_config
            else None
        )
        or os.environ.get("BACKEND_URL")
    )

    if not backend_url:
        raise ValueError(
            "Missing backend_url. Provide BackendServerContext.base_url or set "
            "BACKEND_URL/FALLBACK_BACKEND_URL."
        )

    token = (user_context.auth_token if user_context else None) or os.environ.get(
        "TODO_AGENT_BEARER_TOKEN"
    )
    headers: Dict[str, str] = {"Authorization": f"Bearer {token}"} if token else {}

    tool_context = RestToolContext(
        backend_url=backend_url,
        default_headers=headers,
    )
    print("tool_context inside build_todo_agent", tool_context)

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    if not api_key:
        raise ValueError("ANTHROPIC_API_KEY is required to run the agent.")

    return DynamicClaudeTodoListAgent(
        api_key=api_key,
        model=os.environ.get("CLAUDE_MODEL", DEFAULT_MODEL),
        tool_context=tool_context,
    )


def build_gateway_config() -> GatewayConfig:
    """
    Compose GatewayConfig with:
    - one todo-list AgentConfig that uses the dynamic builder above
    - in-memory chat store
    - in-memory queue manager
    """

    default_fallback = GatewayDefaultFallbackConfig(
        fallback_backend_url=os.environ.get("BACKEND_URL")
    )

    return GatewayConfig(
        agent_configs={
            "todo-list": AgentConfig(
                name="todo-list",
                builder=build_todo_agent,
            )
        },
        default_fallback_config=default_fallback,
        chat_store=InMemoryChatStore(),
        queue_manager=InMemoryTasksQueueManager(),
    )


# ==== CLI that mirrors the FastAPI router endpoints ====


async def create_chat_cli(orchestrator: ConversationOrchestrator) -> str:
    user_id = input("user_id: ").strip() or "demo-user"
    agent_name = input("agent_name [todo-list]: ").strip() or "todo-list"
    chat = await orchestrator.create_chat(
        user_id=user_id, agent_name=agent_name, metadata={}, extra_metadata={}
    )
    print(
        f"\nCreated chat {chat.chat_id} for user {chat.user_id} with agent {chat.agent_name}\n"
    )
    return chat.chat_id


async def get_chat_cli(orchestrator: ConversationOrchestrator, chat_id: str) -> None:
    chat = await orchestrator.get_chat(chat_id)
    print(f"\nChat {chat.chat_id} (agent={chat.agent_name}, status={chat.status})\n")


async def list_messages_cli(
    orchestrator: ConversationOrchestrator, chat_id: str
) -> None:
    messages = await orchestrator.list_messages(chat_id)
    if not messages:
        print("\nNo messages yet.\n")
        return
    print("\nMessages:")
    for msg in messages:
        prefix = "User" if msg.role == Role.user else "Assistant"
        print(f"- [{prefix}] {msg.content}")
    print("")


def prompt_user_context() -> Optional[UserContext]:
    token = input("auth token (optional): ").strip()
    session = input("session_id (optional): ").strip()
    if not any([token, session]):
        return None
    return UserContext(auth_token=token or None, session_id=session or None)


def prompt_backend_context() -> Optional[BackendServerContext]:
    backend = input("backend_url override (optional): ").strip()
    if not backend:
        return None
    return BackendServerContext(base_url=backend)


def render_assistant_response(response: AssistantResponse) -> None:
    if not response:
        print("\n(no assistant response yet)\n")
        return
    if response.final_text:
        print(f"\nAssistant: {response.final_text}\n")
    elif response.messages:
        for msg in response.messages:
            print(f"\nAssistant: {msg.content}\n")
    else:
        print("\nAssistant responded with an empty payload.\n")


async def send_message_cli(
    orchestrator: ConversationOrchestrator, chat_id: str
) -> Optional[str]:
    content = input("message: ").strip()
    if not content:
        print("Skipping empty message.\n")
        return None

    run_mode = input("run mode [sync/background]: ").strip().lower()
    run_in_background = run_mode == "background"

    user_context = prompt_user_context()
    backend_context = prompt_backend_context()

    chat, assistant_response, task = await orchestrator.send_message(
        chat_id=chat_id,
        content=content,
        run_in_background=run_in_background,
        message_metadata={},
        user_context=user_context,
        backend_server_context=backend_context,
    )

    if assistant_response:
        render_assistant_response(assistant_response)
    if task:
        print(f"\nBackground task enqueued: task_id={task.id}, status={task.status}\n")
    print(f"chat {chat.chat_id} updated at {chat.updated_at}")
    return task.id if task else None


async def get_task_cli(
    orchestrator: ConversationOrchestrator, chat_id: str, task_id: str
) -> None:
    task = await orchestrator.get_task(chat_id=chat_id, task_id=task_id)
    print(f"\nTask {task.id} status={task.status}")
    if task.result:
        print("Task result:")
        render_assistant_response(task.result)
    if task.error:
        print(f"Task error: {task.error}")
    print("")


async def main() -> None:
    """
    Simple terminal UI that mirrors the REST endpoints:
      - POST /chats
      - GET /chats/{chat_id}
      - GET /chats/{chat_id}/messages
      - POST /chats/{chat_id}/messages
      - GET /chats/{chat_id}/tasks/{task_id}
    """

    orchestrator = ConversationOrchestrator(config=build_gateway_config())
    last_chat_id: Optional[str] = None
    last_task_id: Optional[str] = None

    print(
        "\nTodo List Agent Orchestrator\n"
        "Commands:\n"
        "  create      -> create chat (POST /chats)\n"
        "  chat        -> get chat details (GET /chats/{chat_id})\n"
        "  messages    -> list chat messages (GET /chats/{chat_id}/messages)\n"
        "  send        -> send message (POST /chats/{chat_id}/messages)\n"
        "  task        -> get task (GET /chats/{chat_id}/tasks/{task_id})\n"
        "  quit/exit   -> leave\n"
    )

    while True:
        cmd = input("command [create/chat/messages/send/task/quit]: ").strip().lower()
        if cmd in ("quit", "exit", "q"):
            print("Goodbye!")
            break
        if cmd == "create":
            last_chat_id = await create_chat_cli(orchestrator)
            continue
        if cmd == "chat":
            chat_id = input(f"chat_id [{last_chat_id or ''}]: ").strip() or last_chat_id
            if not chat_id:
                print("chat_id is required.\n")
                continue
            await get_chat_cli(orchestrator, chat_id)
            last_chat_id = chat_id
            continue
        if cmd == "messages":
            chat_id = input(f"chat_id [{last_chat_id or ''}]: ").strip() or last_chat_id
            if not chat_id:
                print("chat_id is required.\n")
                continue
            await list_messages_cli(orchestrator, chat_id)
            last_chat_id = chat_id
            continue
        if cmd == "send":
            chat_id = input(f"chat_id [{last_chat_id or ''}]: ").strip() or last_chat_id
            if not chat_id:
                print("chat_id is required.\n")
                continue
            last_task_id = await send_message_cli(orchestrator, chat_id) or last_task_id
            last_chat_id = chat_id
            continue
        if cmd == "task":
            chat_id = input(f"chat_id [{last_chat_id or ''}]: ").strip() or last_chat_id
            task_id = input(f"task_id [{last_task_id or ''}]: ").strip() or last_task_id
            if not chat_id or not task_id:
                print("chat_id and task_id are required.\n")
                continue
            await get_task_cli(orchestrator, chat_id, task_id)
            last_chat_id = chat_id
            last_task_id = task_id
            continue

        print("Unknown command. Try create/chat/messages/send/task/quit.\n")


if __name__ == "__main__":
    asyncio.run(main())
