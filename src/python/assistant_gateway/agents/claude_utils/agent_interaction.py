from __future__ import annotations

from dataclasses import replace
from typing import Any, Dict, List, Optional

from assistant_gateway.schemas import (
    AgentInteraction,
    AgentOutput,
    AgentStep,
    Role,
    ToolCall,
    ToolResult,
    UserInput,
)
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
        interactions: Full list of AgentInteractions for this chat up
            to and including the latest user message.

    Returns:
        AgentOutput with parsed messages, steps, and final_text.

    Raises:
        ValueError: If interactions contains no UserInput.
    """
    from claude_agent_sdk import ClaudeSDKClient
    from claude_agent_sdk.types import ResultMessage

    interactions = _sort_interactions(interactions)

    last_user_input: Optional[UserInput] = next(
        (m for m in reversed(interactions) if isinstance(m, UserInput)),
        None,
    )
    if last_user_input is None:
        raise ValueError("interactions must contain at least one UserInput")

    prompt = get_interaction_content(last_user_input) or ""

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


def _sort_interactions(interactions: List[AgentInteraction]) -> List[AgentInteraction]:
    return sorted(
        interactions,
        key=lambda m: (m.sequence_id is None, m.sequence_id, m.created_at),
    )

def _extract_last_session_id(interactions: List[AgentInteraction]) -> str | None:
    """Extract the most recent sdk_session_id from an ordered list of interactions."""
    for interaction in reversed(interactions):
        if isinstance(interaction, AgentOutput) and interaction.sdk_session_id:
            return interaction.sdk_session_id
    return None


def get_interaction_content(interaction: AgentInteraction) -> Optional[str]:
    """Extract the string content from an AgentInteraction."""
    if isinstance(interaction, UserInput):
        return _stringify(interaction.content)

    if hasattr(interaction, "messages"):
        msgs = getattr(interaction, "messages")
        if isinstance(msgs, list) and msgs:
            joined = "\n".join(_stringify(m) for m in msgs if m is not None)
            if joined:
                return joined

    if hasattr(interaction, "final_text"):
        final_text = getattr(interaction, "final_text")
        if final_text:
            return _stringify(final_text)

    if hasattr(interaction, "content"):
        return _stringify(interaction.content)

    return ""


def parse_claude_messages_to_output(
    all_messages: List[Any],
    last_user_input: UserInput,
) -> AgentOutput:
    """
    Parse raw messages from the Claude SDK into an AgentOutput.

    The SDK messages can be:
    - assistant message: content list with text/thinking/tool_use/tool_result blocks
    - result message: has result, is_error, total_cost_usd, duration_ms, num_turns
    - system message: has subtype and data
    - user message: has content
    """
    assistant_messages: List[str] = []
    steps: List[AgentStep] = []
    result_text: Optional[str] = None
    tool_call_names: Dict[str, str] = {}

    for message in all_messages:
        if _is_assistant_message(message):
            content_blocks = _get_value(message, "content", [])
            if not isinstance(content_blocks, list):
                continue

            step_thought: Optional[str] = None
            step_tool_calls: List[ToolCall] = []
            step_tool_results: List[ToolResult] = []
            step_messages: List[str] = []

            for content_block in content_blocks:
                if _is_text_block(content_block):
                    text = _get_value(content_block, "text")
                    if text is not None:
                        text_str = _stringify(text)
                        step_messages.append(text_str)

                elif _is_thinking_block(content_block):
                    thinking = _get_value(content_block, "thinking")
                    if thinking is not None:
                        step_thought = _stringify(thinking)

                elif _is_tool_use_block(content_block):
                    call_id = _stringify(_get_value(content_block, "id"))
                    if not call_id:
                        continue
                    tool_name = _stringify(_get_value(content_block, "name"))
                    input_payload = _get_value(content_block, "input", {}) or {}
                    if not isinstance(input_payload, dict):
                        try:
                            input_payload = dict(input_payload)
                        except Exception:
                            input_payload = {"value": input_payload}
                    tool_call = ToolCall(id=call_id, name=tool_name, input=input_payload)
                    step_tool_calls.append(tool_call)
                    tool_call_names[call_id] = tool_name

                elif _is_tool_result_block(content_block):
                    tool_use_id = _stringify(_get_value(content_block, "tool_use_id"))
                    output_content = _get_value(content_block, "content")
                    tool_result = ToolResult(
                        tool_call_id=tool_use_id or None,
                        output=output_content,
                        name=(tool_call_names.get(tool_use_id) if tool_use_id else None),
                        is_error=bool(_get_value(content_block, "is_error", False) or False),
                        raw_response=content_block,
                    )
                    step_tool_results.append(tool_result)

            if step_messages:
                assistant_messages.append("\n".join(step_messages))

            if step_thought or step_tool_calls or step_tool_results:
                steps.append(
                    AgentStep(
                        thought=step_thought,
                        tool_calls=step_tool_calls,
                        tool_results=step_tool_results,
                    )
                )

        elif _is_result_message(message):
            result_value = _get_value(message, "result")
            if result_value:
                result_text = _stringify(result_value)

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


def _has_attr_or_key(obj: Any, key: str) -> bool:
    return hasattr(obj, key) or (isinstance(obj, dict) and key in obj)


def _get_value(obj: Any, key: str, default: Any = None) -> Any:
    if isinstance(obj, dict):
        return obj.get(key, default)
    return getattr(obj, key, default)


def _stringify(value: Any) -> str:
    if value is None:
        return ""
    try:
        return str(value)
    except Exception:
        return ""


def _is_assistant_message(message: Any) -> bool:
    return _has_attr_or_key(message, "content") and isinstance(_get_value(message, "content"), list)


def _is_result_message(message: Any) -> bool:
    return (
        _has_attr_or_key(message, "subtype")
        and _has_attr_or_key(message, "duration_ms")
        and _has_attr_or_key(message, "is_error")
        and _has_attr_or_key(message, "num_turns")
    )


def _is_system_message(message: Any) -> bool:
    return (
        _has_attr_or_key(message, "subtype")
        and _has_attr_or_key(message, "data")
        and not _has_attr_or_key(message, "duration_ms")
    )


def _is_user_message(message: Any) -> bool:
    return (
        _has_attr_or_key(message, "content")
        and not _has_attr_or_key(message, "model")
        and not _has_attr_or_key(message, "subtype")
    )


def _is_text_block(block: Any) -> bool:
    return _has_attr_or_key(block, "text") and not _has_attr_or_key(block, "thinking")


def _is_thinking_block(block: Any) -> bool:
    return _has_attr_or_key(block, "thinking") and _has_attr_or_key(block, "signature")


def _is_tool_use_block(block: Any) -> bool:
    return (
        _has_attr_or_key(block, "id")
        and _has_attr_or_key(block, "name")
        and _has_attr_or_key(block, "input")
        and not _has_attr_or_key(block, "tool_use_id")
    )


def _is_tool_result_block(block: Any) -> bool:
    return _has_attr_or_key(block, "tool_use_id")
