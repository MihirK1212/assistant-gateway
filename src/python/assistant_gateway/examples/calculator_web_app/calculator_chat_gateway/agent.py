from typing import Any, Dict, List, Optional

import dotenv
from assistant_gateway.agents import claude_utils
from assistant_gateway.agents.claude import ClaudeBaseAgent
from assistant_gateway.tools.rest_tool import RESTTool
from claude_agent_sdk import ClaudeAgentOptions
from pydantic import BaseModel, Field

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
    series: List[float] = Field(description="The generated custom mihir series")


class LogOutputModel(BaseModel):
    status: str = Field(description="The status of the log operation")
    message: str = Field(description="The logged message")


# REST Tools for each calculator endpoint
class AddRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="add",
            description=("Add two numbers together. Endpoint: GET /add?a={a}&b={b}"),
            query_params_model=TwoNumbersQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class MultiplyRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="multiply",
            description=("Multiply two numbers together. Endpoint: GET /multiply?a={a}&b={b}"),
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
                "Apply Mihir's custom transformation to a number"
                "Endpoint: GET /mihir_custom_transform?a={a}"
            ),
            query_params_model=SingleNumberQueryParamsModel,
            output_model=ArithmeticResultOutputModel,
        )


class MihirCustomSeriesRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="mihir_custom_series",
            description=("Generate Mihir's custom series from a number Endpoint: GET /mihir_custom_series?a={a}"),
            query_params_model=SingleNumberQueryParamsModel,
            output_model=CustomSeriesOutputModel,
        )


class MihirCustomLogRESTTool(RESTTool):
    def __init__(self) -> None:
        super().__init__(
            name="mihir_custom_log",
            description=(
                "Log a message to the logs file. "
                "Endpoint: POST /mihir_custom_log?message=hello%20from%20original%20swagger"
            ),
            query_params_model=LogMessageQueryParamsModel,
            output_model=LogOutputModel,
            timeout_seconds=200,
        )


class DynamicClaudeCalculatorAgent(ClaudeBaseAgent):
    """
    Claude agent wired with the calculator REST tools.

    The tool context (backend URL + headers) is injected at construction time so it
    can be derived from the chat_orchestrator GatewayConfig builder arguments.
    """

    def __init__(self, *, model: str) -> None:
        super().__init__()
        self._model = model

    def get_claude_agent_options(self, input_overrides: Optional[Dict[str, Any]] = None) -> ClaudeAgentOptions:
        tools = {
            "add": AddRESTTool(),
            "multiply": MultiplyRESTTool(),
            "divide": DivideRESTTool(),
            "mihir_custom_transform": MihirCustomTransformRESTTool(),
            "mihir_custom_series": MihirCustomSeriesRESTTool(),
            "mihir_custom_log": MihirCustomLogRESTTool(),
        }

        mcp_server = claude_utils.get_claude_mcp_server(
            name="calculator-mcp", version="0.1.0", tools=tools, input_overrides=input_overrides
        )

        return ClaudeAgentOptions(
            model=self._model,
            mcp_servers={"calculator": mcp_server},
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


def build_calculator_agent() -> DynamicClaudeCalculatorAgent:
    return DynamicClaudeCalculatorAgent(model="claude-haiku-4-5-20251001")
