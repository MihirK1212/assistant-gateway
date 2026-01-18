from typing import Dict, List, Optional

from claude_agent_sdk import ClaudeAgentOptions
from pydantic import BaseModel, Field


from assistant_gateway.agents.claude import ClaudeBaseAgent
from assistant_gateway.chat_orchestrator.core.config import (
    GatewayDefaultFallbackConfig,
)
from assistant_gateway.chat_orchestrator.core.schemas import (
    BackendServerContext,
    UserContext,
)
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.rest_tool import RESTTool
from typing import Any


import os 
import dotenv 

dotenv.load_dotenv()

# Query param models for calculator endpoints
class TwoNumbersQueryParamsModel(BaseModel):
    a: float = Field(description="The first number")
    b: float = Field(description="The second number")


class SingleNumberQueryParamsModel(BaseModel):
    a: float = Field(description="The input number")


class LogMessageQueryParamsModel(BaseModel):
    message: str = Field(description="The message to log")


# Output models for calculator endpoints
class ArithmeticResultOutputModel(BaseModel):
    result: float = Field(description="The result of the arithmetic operation")


class CustomSeriesOutputModel(BaseModel):
    series: List[float] = Field(
        description="The generated series [a-2, a-1, a, a+1, a+2]"
    )


class LogOutputModel(BaseModel):
    status: str = Field(description="The status of the log operation")
    message: str = Field(description="The logged message")


# REST Tools for each calculator endpoint
class AddRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="add",
            description=("Add two numbers together. " "Endpoint: GET /add?a={a}&b={b}"),
            query_params_model=TwoNumbersQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class MultiplyRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="multiply",
            description=(
                "Multiply two numbers together. " "Endpoint: GET /multiply?a={a}&b={b}"
            ),
            query_params_model=TwoNumbersQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class DivideRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="divide",
            description=(
                "Divide the first number by the second number. "
                "Returns an error if dividing by zero. "
                "Endpoint: GET /divide?a={a}&b={b}"
            ),
            query_params_model=TwoNumbersQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class MihirCustomTransformRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="mihir_custom_transform",
            description=(
                "Apply Mihir's custom transformation to a number: result = a * 28 + 12. "
                "Endpoint: GET /mihir_custom_transform?a={a}"
            ),
            query_params_model=SingleNumberQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class MihirCustomSeriesRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="mihir_custom_series",
            description=(
                "Generate Mihir's custom series from a number: returns [a-2, a-1, a, a+1, a+2]. "
                "Endpoint: GET /mihir_custom_series?a={a}"
            ),
            query_params_model=SingleNumberQueryParamsModel,
            output_model=CustomSeriesOutputModel,
        )


class MihirCustomLogRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="mihir_custom_log",
            description=(
                "Log a message to the logs file. "
                "Endpoint: POST /mihir_custom_log?message={message}"
            ),
            data_payload_model=LogMessageQueryParamsModel,
            output_model=LogOutputModel,
        )


def build_tool_registry() -> ToolRegistry:
    registry = ToolRegistry()
    registry.register(AddRESTTool())
    registry.register(MultiplyRESTTool())
    registry.register(DivideRESTTool())
    registry.register(MihirCustomTransformRESTTool())
    registry.register(MihirCustomSeriesRESTTool())
    registry.register(MihirCustomLogRESTTool())
    return registry


class DynamicClaudeCalculatorAgent(ClaudeBaseAgent):
    """
    Claude agent wired with the calculator REST tools.

    The tool context (backend URL + headers) is injected at construction time so it
    can be derived from the chat_orchestrator GatewayConfig builder arguments.
    """

    def __init__(
        self,
        *,
        model: str,
        agent_level_input_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        super().__init__()
        self._agent_level_input_overrides = agent_level_input_overrides
        self._model = model
        self._tool_registry = build_tool_registry()

        server, _ = self.get_mcp_server_config(
            name="calculator-agent",
            version="0.1.0",
            tool_registry=self._tool_registry,
            agent_level_input_overrides=agent_level_input_overrides,
        )

        self._options = ClaudeAgentOptions(
            model=self._model,
            mcp_servers={"calculator": server},
            system_prompt=(
                "You are a helpful calculator assistant. Use the available tools "
                "to perform arithmetic operations: addition, multiplication, division, "
                "and Mihir's custom transformations."
            ),
            allowed_tools=[
                "mcp__calculator__add",
                "mcp__calculator__multiply",
                "mcp__calculator__divide",
                "mcp__calculator__mihir_custom_transform",
                "mcp__calculator__mihir_custom_series",
                "mcp__calculator__mihir_custom_log",
            ],
        )

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        return self._options


def build_calculator_agent(
    user_context: Optional[UserContext],
    backend_server_context: Optional[BackendServerContext],
    default_fallback_config: Optional[GatewayDefaultFallbackConfig],
) -> DynamicClaudeCalculatorAgent:
    """
    Create a calculator agent using dynamic inputs supplied by the orchestrator.
    """

    backend_url = "http://127.0.0.1:5000"

    api_key = os.environ.get("ANTHROPIC_API_KEY")
    print("Anthropic API Key:", api_key)

    agent_level_input_overrides = {"backend_url": backend_url}

    return DynamicClaudeCalculatorAgent(
        model="claude-sonnet-4-5-20250929",
        agent_level_input_overrides=agent_level_input_overrides,
    )
