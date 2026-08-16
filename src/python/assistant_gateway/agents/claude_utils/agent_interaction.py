from __future__ import annotations

from dataclasses import replace
from typing import Any, List

from assistant_gateway.agents.claude_utils.message_parsing import (
    interactions_to_claude_messages,
    parse_claude_messages_to_output,
)
from assistant_gateway.schemas import AgentInteraction, AgentOutput
from claude_agent_sdk import ClaudeAgentOptions


async def run_claude_agent_for_interactions(
    claude_agent_options: ClaudeAgentOptions,
    interactions: List[AgentInteraction],
) -> AgentOutput:
    """
    Execute the Claude agent for the given conversation interactions.

    Args:
        claude_agent_options: A ClaudeAgentOptions instance configured with the
            desired model, MCP servers, system prompt, and allowed tools.
        interactions: Full ordered list of AgentInteractions for this chat up
            to and including the latest user message.

    Returns:
        AgentOutput with parsed messages, steps, and final_text.

    Raises:
        ValueError: If interactions contains no UserInput.
    """
    from claude_agent_sdk import ClaudeSDKClient
    from claude_agent_sdk.types import ResultMessage

    claude_messages, last_user_input = interactions_to_claude_messages(interactions)

    if last_user_input is None:
        raise ValueError("interactions must contain at least one UserInput")

    last_user_message = next(
        (m for m in reversed(claude_messages) if m["role"] == "user"),
        None,
    )
    prompt = last_user_message["content"] if last_user_message else ""

    previous_session_id = _extract_last_session_id(interactions)
    options = claude_agent_options
    if previous_session_id:
        options = replace(claude_agent_options, resume=previous_session_id)

    all_messages: List[Any] = []
    session_id: str | None = None
    async with ClaudeSDKClient(options=options) as client:
        await client.query(prompt)
        async for message in client.receive_response():
            all_messages.append(message)
            if isinstance(message, ResultMessage):
                session_id = message.session_id

    output = parse_claude_messages_to_output(all_messages, last_user_input)
    output.sdk_session_id = session_id
    return output


def _extract_last_session_id(interactions: List[AgentInteraction]) -> str | None:
    for interaction in reversed(interactions):
        if isinstance(interaction, AgentOutput) and interaction.sdk_session_id:
            return interaction.sdk_session_id
    return None
