import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(".")
sys.path.append("..")
sys.path.append("../..")

import asyncio
from claude_agent_sdk import ClaudeAgentOptions
from pydantic import BaseModel
from typing import Optional, List
import dotenv

from assistant_gateway.agents.claude import ClaudeBaseAgent
from assistant_gateway.tools.registry import ToolRegistry
from assistant_gateway.tools.base import ToolConfig, ToolContext, ToolResult
from assistant_gateway.tools.base import Tool
from assistant_gateway.schemas import (
    Message,
    Role,
    ToolResult,
)

dotenv.load_dotenv()

# DEFAULT_MODEL = "claude-sonnet-4-20250514"
DEFAULT_MODEL = "claude-haiku-4-5-20251001"


######################################################
################# Addition Tool #####################
class AdditionInput(BaseModel):
    a: int
    b: int


class AdditionOutput(BaseModel):
    result: int


ADDITION_TOOL_CONFIG = ToolConfig(
    name="addition",
    description="Add two numbers a and b, where a and b are integers. return the result of the addition of a and b, result = a + b",
    input_model=AdditionInput,
    output_description="The result of the addition of a and b, result = a + b",
    output_model=AdditionOutput,
)


class AdditionTool(Tool):
    def __init__(self) -> None:
        super().__init__(config=ADDITION_TOOL_CONFIG)

    async def run(self, context: ToolContext) -> ToolResult:
        input = context.input
        result = input["a"] + input["b"]
        return ToolResult(name=self.name, output=result)


######################################################


######################################################
################# Multiplication Tool #####################
class MultiplicationInput(BaseModel):
    a: int
    b: int


class MultiplicationOutput(BaseModel):
    result: int


MULTIPLICATION_TOOL_CONFIG = ToolConfig(
    name="multiplication",
    description="Multiply two numbers a and b, where a and b are integers. return the result of the multiplication of a and b, result = a * b",
    input_model=MultiplicationInput,
    output_description="The result of the multiplication of a and b, result = a * b",
    output_model=MultiplicationOutput,
)


class MultiplicationTool(Tool):
    def __init__(self) -> None:
        super().__init__(config=MULTIPLICATION_TOOL_CONFIG)

    async def run(self, context: ToolContext) -> ToolResult:
        input = context.input
        result = input["a"] * input["b"]
        return ToolResult(name=self.name, output=result)


######################################################


######################################################
################# Insane Tool #####################
class InsaneInput(BaseModel):
    x: int


class InsaneOutput(BaseModel):
    result: int


INSANITY_TOOL_CONFIG = ToolConfig(
    name="insanity",
    description="Return the result of the insanity of the number x",
    input_model=InsaneInput,
    output_description="The result of the insanity of the number x",
    output_model=InsaneOutput,
)


class InsanityTool(Tool):
    def __init__(self) -> None:
        super().__init__(config=INSANITY_TOOL_CONFIG)

    async def run(self, context: ToolContext) -> ToolResult:
        input = context.input
        result = input["x"] * 28 + 12
        return ToolResult(name=self.name, output=result)


######################################################


class ClaudeTodoListAgent(ClaudeBaseAgent):
    def __init__(
        self,
        api_key: Optional[str],
        model: str = DEFAULT_MODEL,
    ) -> None:
        super().__init__(api_key)

        calc_mcp_server, calc_tool_functions = self.get_mcp_server_config(
            name="calc-claude-agent",
            version="0.1.0",
            tool_registry=self.tool_registry,
            predefined_tool_context=ToolContext(),
        )

        # Use with Claude
        self._mcp_server_options = ClaudeAgentOptions(
            model=model,
            mcp_servers={"calc": calc_mcp_server},
            system_prompt="You are a helpful calculator assistant. Use the available tools to perform calculations.",
            allowed_tools=[
                "mcp__calc__addition",
                "mcp__calc__multiplication",
                "mcp__calc__insanity",
            ],
        )

    def get_mcp_server_options(self) -> ClaudeAgentOptions:
        return self._mcp_server_options

    @property
    def tool_registry(self) -> ToolRegistry:
        registry = ToolRegistry()
        registry.register(AdditionTool())
        registry.register(MultiplicationTool())
        registry.register(InsanityTool())
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
    print("  CalculatorAgent (Claude)")
    print(f"  Model: {model}")
    print("  Tools: addition, multiplication, insanity")
    print("=" * 50)
    print("\nType your math questions or 'quit' to exit.\n")

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
            response = await agent.run(
                messages=messages,
            )

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
