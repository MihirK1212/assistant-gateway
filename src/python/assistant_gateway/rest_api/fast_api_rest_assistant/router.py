from __future__ import annotations

import logging
from functools import lru_cache

from fastapi import APIRouter, Depends, Response, status, WebSocket, WebSocketDisconnect

from assistant_gateway.rest_api.schemas import (
    ChatInteractionsResponse,
    ChatResponse,
    CreateChatRequest,
    CreateChatResponse,
    RunMode,
    SendMessageRequest,
    SendMessageResponse,
    TaskResponse,
)
from assistant_gateway.chat_orchestrator.orchestration.orchestrator import (
    ConversationOrchestrator,
)
from assistant_gateway.clauq_btm.queue_manager.serialization import serialize_event


logger = logging.getLogger(__name__)


@lru_cache()
def get_orchestrator() -> ConversationOrchestrator:
    raise NotImplementedError(
        "Orchestrator is not implemented. Please provide a ConversationOrchestrator instance."
    )


router = APIRouter()


@router.post(
    "/chats", response_model=CreateChatResponse, status_code=status.HTTP_201_CREATED
)
async def create_chat(
    body: CreateChatRequest,
    orchestrator: ConversationOrchestrator = Depends(get_orchestrator),
) -> CreateChatResponse:
    chat = await orchestrator.create_chat(
        user_id=body.user_id,
        agent_name=body.agent_name,
        metadata=body.metadata,
        extra_metadata=body.extra_metadata,
    )
    return CreateChatResponse(chat=chat)


@router.get("/chats/{chat_id}", response_model=ChatResponse)
async def get_chat(
    chat_id: str, orchestrator: ConversationOrchestrator = Depends(get_orchestrator)
) -> ChatResponse:
    chat = await orchestrator.get_chat(chat_id)
    return ChatResponse(chat=chat)


@router.get("/chats/{chat_id}/interactions", response_model=ChatInteractionsResponse)
async def list_chat_interactions(
    chat_id: str, orchestrator: ConversationOrchestrator = Depends(get_orchestrator)
) -> ChatInteractionsResponse:
    interactions = await orchestrator.list_interactions(chat_id)
    return ChatInteractionsResponse(chat_id=chat_id, interactions=interactions)


@router.post("/chats/{chat_id}/messages", response_model=SendMessageResponse)
async def send_chat_message(
    chat_id: str,
    body: SendMessageRequest,
    response: Response,
    orchestrator: ConversationOrchestrator = Depends(get_orchestrator),
) -> SendMessageResponse:
    chat, assistant_response, task = await orchestrator.send_message(
        chat_id=chat_id,
        content=body.content,
        run_in_background=body.run_mode == RunMode.background,
        message_metadata=body.message_metadata,
        user_context=body.user_context,
        backend_server_context=body.backend_server_context,
    )
    if task:
        response.status_code = status.HTTP_202_ACCEPTED
    return SendMessageResponse(
        chat=chat, assistant_response=assistant_response, task=task
    )


@router.get("/chats/{chat_id}/tasks/{task_id}", response_model=TaskResponse)
async def get_task(
    chat_id: str,
    task_id: str,
    orchestrator: ConversationOrchestrator = Depends(get_orchestrator),
) -> TaskResponse:
    task = await orchestrator.get_task(chat_id=chat_id, task_id=task_id)
    return TaskResponse(task=task)


@router.websocket("/chats/{chat_id}/events")
async def subscribe_to_chat_events(
    websocket: WebSocket,
    chat_id: str,
    orchestrator: ConversationOrchestrator = Depends(get_orchestrator),
) -> None:
    """
    WebSocket endpoint to subscribe to task events for a specific chat.

    Events are streamed in real-time as JSON messages. Each event contains:
    - event_type: Type of event (queued, started, completed, failed, interrupted, progress)
    - task_id: ID of the task
    - queue_id: ID of the queue (same as chat_id)
    - status: Current task status
    - timestamp: When the event occurred
    - error: Error message (if applicable)
    - progress: Progress data (if applicable)
    - task: Full task data (if available)

    The connection remains open until the client disconnects or an error occurs.
    """
    await websocket.accept()

    try:
        # Subscribe to events for this chat via the orchestrator
        async with orchestrator.subscribe_to_events(chat_id) as subscription:
            async for event in subscription:
                # Serialize event to JSON and send over WebSocket
                event_data = serialize_event(event)
                await websocket.send_json(event_data)
    except WebSocketDisconnect:
        logger.debug(f"WebSocket disconnected for chat {chat_id}")
    except Exception as e:
        logger.error(f"Error in WebSocket event subscription for chat {chat_id}: {e}")
        try:
            await websocket.close(code=1011, reason=str(e))
        except Exception:
            pass  # Connection may already be closed
