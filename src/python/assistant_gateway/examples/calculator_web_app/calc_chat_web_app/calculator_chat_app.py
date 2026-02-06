"""
Calculator Chat Web App - Streamlit UI for the Calculator Agent Gateway

Features:
- Single active chat at a time (start new chat to begin fresh)
- Sync and background task execution modes
- Real-time updates via WebSocket event subscription
- Pending task indicators below associated user inputs
- Clean, modern UI with tool call visualization

Requires: Streamlit >= 1.37.0, requests, websockets
"""

from __future__ import annotations

import asyncio
import html as html_module
import json
import logging
import os
import queue
import sys
import threading
from datetime import timedelta
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

# ─── Path Setup ─────────────────────────────────────────────────────────────

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(CURRENT_DIR)))
sys.path.append(os.path.dirname(CURRENT_DIR))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR))))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR))))
)

logger = logging.getLogger(__name__)

# ─── Configuration ──────────────────────────────────────────────────────────

GATEWAY_BASE_URL = os.environ.get("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
GATEWAY_WS_URL = (
    GATEWAY_BASE_URL.replace("https://", "wss://").replace("http://", "ws://")
)
API_PREFIX = "/api/v1"

# ─── Page Config ────────────────────────────────────────────────────────────

st.set_page_config(
    page_title="Calculator Agent Chat",
    page_icon="🧮",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ─── Custom CSS ─────────────────────────────────────────────────────────────

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

    :root {
        --bg-page: #f8fafc;
        --bg-card: #ffffff;
        --bg-hover: #f1f5f9;
        --accent-blue: #3b82f6;
        --accent-blue-dark: #2563eb;
        --accent-green: #10b981;
        --accent-amber: #f59e0b;
        --accent-red: #ef4444;
        --accent-purple: #8b5cf6;
        --text-primary: #0f172a;
        --text-secondary: #475569;
        --text-muted: #94a3b8;
        --border: #e2e8f0;
        --border-focus: #93c5fd;
        --shadow-sm: 0 1px 2px rgba(0,0,0,0.04);
        --shadow-md: 0 4px 6px -1px rgba(0,0,0,0.07), 0 2px 4px -2px rgba(0,0,0,0.05);
        --radius-sm: 6px;
        --radius-md: 10px;
        --radius-lg: 14px;
    }

    .stApp { background: var(--bg-page); }

    /* ── Header ── */
    .app-header {
        text-align: center;
        padding: 1.5rem 0 0.5rem;
    }
    .app-header h1 {
        font-family: 'Inter', sans-serif;
        font-weight: 700;
        font-size: 2rem;
        color: var(--text-primary);
        margin: 0;
        letter-spacing: -0.02em;
    }
    .app-header p {
        font-family: 'Inter', sans-serif;
        color: var(--text-muted);
        font-size: 0.95rem;
        margin-top: 0.25rem;
    }

    /* ── Chat Container ── */
    .chat-wrapper {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius-lg);
        padding: 1.25rem 1.5rem;
        margin-bottom: 1rem;
        box-shadow: var(--shadow-sm);
        min-height: 200px;
    }

    /* ── Messages ── */
    .msg-user {
        background: #eff6ff;
        border-left: 3px solid var(--accent-blue);
        border-radius: 0 var(--radius-md) var(--radius-md) 0;
        padding: 0.85rem 1.1rem;
        margin: 0.6rem 0;
        font-family: 'Inter', sans-serif;
        font-size: 0.92rem;
        color: var(--text-primary);
        line-height: 1.55;
    }
    .msg-user .msg-label {
        font-weight: 600;
        color: var(--accent-blue-dark);
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.3rem;
    }
    .msg-assistant {
        background: #f0fdf4;
        border-left: 3px solid var(--accent-green);
        border-radius: 0 var(--radius-md) var(--radius-md) 0;
        padding: 0.85rem 1.1rem;
        margin: 0.6rem 0;
        font-family: 'Inter', sans-serif;
        font-size: 0.92rem;
        color: var(--text-primary);
        line-height: 1.55;
    }
    .msg-assistant .msg-label {
        font-weight: 600;
        color: #047857;
        font-size: 0.78rem;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        margin-bottom: 0.3rem;
    }

    /* ── Tool Calls ── */
    .tool-calls-section {
        margin-top: 0.6rem;
        padding-top: 0.5rem;
        border-top: 1px dashed #d1fae5;
    }
    .tool-call-item {
        background: #fffbeb;
        border: 1px solid #fcd34d;
        border-radius: var(--radius-sm);
        padding: 0.6rem 0.85rem;
        margin: 0.35rem 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.8rem;
    }
    .tool-call-name {
        color: #b45309;
        font-weight: 600;
    }
    .tool-call-input {
        color: var(--text-secondary);
        margin-top: 0.15rem;
        word-break: break-word;
    }
    .tool-call-result {
        color: #047857;
        font-weight: 500;
        margin-top: 0.3rem;
    }
    .tool-call-error {
        color: var(--accent-red);
        font-weight: 500;
        margin-top: 0.3rem;
    }

    /* ── Pending Task Indicator ── */
    @keyframes pulse-bg {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.65; }
    }
    @keyframes spin {
        0% { transform: rotate(0deg); }
        100% { transform: rotate(360deg); }
    }
    .pending-indicator {
        background: #fefce8;
        border-left: 3px solid var(--accent-amber);
        border-radius: 0 var(--radius-md) var(--radius-md) 0;
        padding: 0.7rem 1rem;
        margin: 0.25rem 0 0.6rem 0;
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        display: flex;
        align-items: center;
        gap: 0.6rem;
        animation: pulse-bg 2s ease-in-out infinite;
    }
    .pending-indicator .spinner {
        width: 16px;
        height: 16px;
        border: 2px solid #fcd34d;
        border-top-color: #d97706;
        border-radius: 50%;
        animation: spin 0.8s linear infinite;
        flex-shrink: 0;
    }
    .pending-indicator .pending-text {
        color: #92400e;
        font-weight: 500;
    }

    /* ── Failed Task Indicator ── */
    .failed-indicator {
        background: #fef2f2;
        border-left: 3px solid var(--accent-red);
        border-radius: 0 var(--radius-md) var(--radius-md) 0;
        padding: 0.7rem 1rem;
        margin: 0.25rem 0 0.6rem 0;
        font-family: 'Inter', sans-serif;
        font-size: 0.85rem;
        color: #991b1b;
    }
    .failed-indicator strong { font-weight: 600; }

    /* ── Status Badges ── */
    .badge {
        display: inline-block;
        padding: 0.2rem 0.6rem;
        border-radius: 5px;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.04em;
        font-family: 'Inter', sans-serif;
    }
    .badge-pending   { background: #fef3c7; color: #92400e; border: 1px solid #fde68a; }
    .badge-progress  { background: #dbeafe; color: #1e40af; border: 1px solid #93c5fd; }
    .badge-completed { background: #d1fae5; color: #065f46; border: 1px solid #6ee7b7; }
    .badge-failed    { background: #fee2e2; color: #991b1b; border: 1px solid #fca5a5; }

    /* ── Empty State ── */
    .empty-state {
        text-align: center;
        padding: 3.5rem 2rem;
        color: var(--text-muted);
        font-family: 'Inter', sans-serif;
    }
    .empty-state .icon { font-size: 3rem; margin-bottom: 0.75rem; }
    .empty-state .title {
        font-size: 1.1rem;
        font-weight: 600;
        color: var(--text-secondary);
        margin-bottom: 0.35rem;
    }

    /* ── Sidebar ── */
    [data-testid="stSidebar"] { background: var(--bg-card); }
    .sidebar-card {
        background: var(--bg-page);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.85rem 1rem;
        margin: 0.5rem 0;
        font-family: 'Inter', sans-serif;
    }
    .sidebar-card .card-label {
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--text-muted);
        margin-bottom: 0.25rem;
    }
    .sidebar-card .card-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.82rem;
        color: var(--text-primary);
        word-break: break-all;
    }

    /* ── Form & Inputs ── */
    .stTextInput > div > div > input {
        background: var(--bg-card) !important;
        border: 1.5px solid var(--border) !important;
        border-radius: var(--radius-md) !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.92rem !important;
        color: var(--text-primary) !important;
        padding: 0.6rem 0.85rem !important;
    }
    .stTextInput > div > div > input:focus {
        border-color: var(--accent-blue) !important;
        box-shadow: 0 0 0 3px rgba(59,130,246,0.12) !important;
    }
    .stButton > button {
        border-radius: var(--radius-md) !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        transition: all 0.15s ease !important;
    }
    .stRadio > div {
        background: var(--bg-page);
        border-radius: var(--radius-md);
        padding: 0.35rem 0.5rem;
    }
    .stRadio label {
        font-family: 'Inter', sans-serif !important;
        color: var(--text-primary) !important;
        font-size: 0.88rem !important;
    }

    /* ── Info Card ── */
    .info-card {
        background: var(--bg-card);
        border: 1px solid var(--border);
        border-radius: var(--radius-md);
        padding: 0.85rem 1rem;
        margin: 0.4rem 0;
        box-shadow: var(--shadow-sm);
    }
    .info-card .label {
        font-family: 'Inter', sans-serif;
        font-size: 0.7rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        color: var(--text-muted);
        margin-bottom: 0.2rem;
    }
    .info-card .value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1rem;
        color: var(--accent-blue);
        font-weight: 600;
    }

    /* ── WS Status Indicator ── */
    .ws-status {
        display: flex;
        align-items: center;
        gap: 0.4rem;
        font-family: 'Inter', sans-serif;
        font-size: 0.75rem;
        color: var(--text-muted);
        margin: 0.35rem 0;
    }
    .ws-dot {
        width: 7px;
        height: 7px;
        border-radius: 50%;
        flex-shrink: 0;
    }
    .ws-dot.connected { background: var(--accent-green); }
    .ws-dot.disconnected { background: var(--text-muted); }

    /* ── Footer ── */
    .app-footer {
        text-align: center;
        padding: 1.5rem 0 1rem;
        color: var(--text-muted);
        font-family: 'Inter', sans-serif;
        font-size: 0.8rem;
    }
</style>
""",
    unsafe_allow_html=True,
)


# ═══════════════════════════════════════════════════════════════════════════
# API CLIENT
# ═══════════════════════════════════════════════════════════════════════════


def api_create_chat(user_id: str) -> dict:
    """Create a new chat with the calculator agent."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats"
    payload = {
        "user_id": user_id,
        "agent_name": "calculator",
        "metadata": {},
        "extra_metadata": {},
    }
    resp = requests.post(url, json=payload, timeout=(5, 15))
    resp.raise_for_status()
    return resp.json()


def api_get_chat(chat_id: str) -> dict:
    """Get chat metadata."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}"
    resp = requests.get(url, timeout=(5, 10))
    resp.raise_for_status()
    return resp.json()


def api_get_interactions(chat_id: str) -> dict:
    """Get all interactions for a chat, ordered by sequence."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/interactions"
    resp = requests.get(url, timeout=(5, 10))
    resp.raise_for_status()
    return resp.json()


def api_send_message(chat_id: str, content: str, run_mode: str = "sync") -> dict:
    """Send a message. Returns chat, assistant_response (sync), and task."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/messages"
    payload = {
        "content": content,
        "run_mode": run_mode,
        "message_metadata": {},
        "user_context": None,
        "backend_server_context": None,
    }
    # Background requests return quickly; sync may take longer
    read_timeout = 15 if run_mode == "background" else 120
    resp = requests.post(url, json=payload, timeout=(5, read_timeout))
    resp.raise_for_status()
    return resp.json()


def api_get_task(chat_id: str, task_id: str) -> dict:
    """Get task status by ID."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/tasks/{task_id}"
    resp = requests.get(url, timeout=(5, 10))
    resp.raise_for_status()
    return resp.json()


# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET LISTENER (background thread)
# ═══════════════════════════════════════════════════════════════════════════


def _ws_listener_thread(
    chat_id: str,
    event_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    """
    Background thread that connects to the WebSocket endpoint for a chat
    and pushes received events into the shared queue.
    Falls back silently if 'websockets' is not installed.
    """
    try:
        import websockets  # noqa: F811
    except ImportError:
        logger.debug("websockets library not installed; WS listener disabled")
        return

    async def _run() -> None:
        url = f"{GATEWAY_WS_URL}{API_PREFIX}/chats/{chat_id}/events"
        retry = 0
        max_retries = 10

        while not stop_event.is_set() and retry < max_retries:
            try:
                async with websockets.connect(url) as ws:
                    retry = 0  # reset on successful connect
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            event = json.loads(raw)
                            event_queue.put(event)
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            break  # reconnect
            except asyncio.CancelledError:
                break
            except Exception:
                retry += 1
                if not stop_event.is_set():
                    await asyncio.sleep(min(2**retry, 30))

    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(_run())
    except Exception:
        pass
    finally:
        loop.close()


# ═══════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═══════════════════════════════════════════════════════════════════════════

_STATE_DEFAULTS: Dict[str, Any] = {
    "chat_id": None,
    "user_id": "streamlit_user",
    "interactions": [],
    # Background tasks being tracked: {task_id: {task_id, interaction_id, status, error}}
    "bg_tasks": {},
    # WebSocket internals
    "_ws_queue": None,
    "_ws_stop": None,
    "_ws_thread": None,
    "_ws_chat_id": None,
}

for _key, _default in _STATE_DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default


# ═══════════════════════════════════════════════════════════════════════════
# WEBSOCKET CONNECTION MANAGEMENT
# ═══════════════════════════════════════════════════════════════════════════


def _stop_ws_listener() -> None:
    """Gracefully stop the current WebSocket listener thread."""
    stop_evt: Optional[threading.Event] = st.session_state.get("_ws_stop")
    if stop_evt is not None:
        stop_evt.set()
    thread: Optional[threading.Thread] = st.session_state.get("_ws_thread")
    if thread is not None and thread.is_alive():
        thread.join(timeout=3)
    st.session_state["_ws_thread"] = None
    st.session_state["_ws_stop"] = None
    st.session_state["_ws_queue"] = None
    st.session_state["_ws_chat_id"] = None


def _ensure_ws_connected() -> None:
    """
    Start a WebSocket listener for the current chat if not already connected.
    If the chat has changed, stop the old listener first.
    """
    chat_id = st.session_state.chat_id
    if not chat_id:
        _stop_ws_listener()
        return

    # Already connected to this chat?
    thread: Optional[threading.Thread] = st.session_state.get("_ws_thread")
    if (
        st.session_state.get("_ws_chat_id") == chat_id
        and thread is not None
        and thread.is_alive()
    ):
        return

    # Stop any existing listener
    _stop_ws_listener()

    # Start new listener
    eq: queue.Queue = queue.Queue()
    stop_evt = threading.Event()
    t = threading.Thread(
        target=_ws_listener_thread,
        args=(chat_id, eq, stop_evt),
        daemon=True,
    )
    t.start()

    st.session_state["_ws_queue"] = eq
    st.session_state["_ws_stop"] = stop_evt
    st.session_state["_ws_thread"] = t
    st.session_state["_ws_chat_id"] = chat_id


def _is_ws_connected() -> bool:
    """Check if the WebSocket listener thread is alive."""
    t: Optional[threading.Thread] = st.session_state.get("_ws_thread")
    return t is not None and t.is_alive()


# ═══════════════════════════════════════════════════════════════════════════
# EVENT PROCESSING
# ═══════════════════════════════════════════════════════════════════════════

_TERMINAL_STATUSES = frozenset({"completed", "failed", "interrupted"})
_ACTIVE_STATUSES = frozenset({"pending", "in_progress"})


def _drain_ws_events() -> bool:
    """
    Drain all events from the WebSocket queue and update bg_tasks.
    Returns True if any task reached a terminal state.
    """
    eq: Optional[queue.Queue] = st.session_state.get("_ws_queue")
    if eq is None:
        return False

    any_terminal = False
    while True:
        try:
            event = eq.get_nowait()
        except queue.Empty:
            break

        task_id = event.get("task_id", "")
        new_status = event.get("status", "")

        if task_id in st.session_state.bg_tasks:
            st.session_state.bg_tasks[task_id]["status"] = new_status
            st.session_state.bg_tasks[task_id]["error"] = event.get("error")

            if new_status in _TERMINAL_STATUSES:
                if new_status == "completed":
                    # completed -> response now stored; remove from tracking
                    del st.session_state.bg_tasks[task_id]
                # failed/interrupted -> keep in bg_tasks so we can show the error
                any_terminal = True

    return any_terminal


def _poll_task_statuses() -> bool:
    """
    HTTP-poll every active bg task. Returns True if any task reached a terminal
    state (used as fallback when WebSocket events are missed).
    """
    chat_id = st.session_state.chat_id
    if not chat_id:
        return False

    any_terminal = False
    for task_id in list(st.session_state.bg_tasks.keys()):
        info = st.session_state.bg_tasks.get(task_id)
        if info is None:
            continue
        if info["status"] in _TERMINAL_STATUSES:
            continue  # already terminal

        try:
            resp = api_get_task(chat_id, task_id)
            task_data = resp.get("task", {})
            new_status = task_data.get("status", info["status"])

            if new_status != info["status"]:
                info["status"] = new_status
                info["error"] = task_data.get("error")

                if new_status in _TERMINAL_STATUSES:
                    if new_status == "completed":
                        del st.session_state.bg_tasks[task_id]
                    any_terminal = True
        except Exception:
            pass  # will retry on next cycle

    return any_terminal


def _refresh_interactions() -> None:
    """Re-fetch interactions from the API for the current chat."""
    chat_id = st.session_state.chat_id
    if not chat_id:
        return
    try:
        resp = api_get_interactions(chat_id)
        st.session_state.interactions = resp.get("interactions", [])
    except Exception:
        pass


# ═══════════════════════════════════════════════════════════════════════════
# RENDERING HELPERS
# ═══════════════════════════════════════════════════════════════════════════

def _esc(text: Any) -> str:
    """HTML-escape a value for safe embedding."""
    return html_module.escape(str(text))


def _render_tool_calls_html(steps: List[dict]) -> str:
    """Build HTML for tool call details inside an assistant message."""
    parts: List[str] = []
    for step in steps:
        tool_calls = step.get("tool_calls", [])
        tool_results = step.get("tool_results", [])
        result_map = {r.get("tool_call_id"): r for r in tool_results}

        for tc in tool_calls:
            name = _esc(tc.get("name", "unknown"))
            tc_input = tc.get("input", {})
            tc_id = tc.get("id", "")
            result = result_map.get(tc_id, {})

            # Format input nicely
            try:
                input_str = json.dumps(tc_input, indent=None, default=str)
            except Exception:
                input_str = str(tc_input)

            result_output = result.get("output", None)
            is_error = result.get("is_error", False)

            if result_output is not None:
                try:
                    output_str = json.dumps(result_output, indent=None, default=str)
                except Exception:
                    output_str = str(result_output)
                result_cls = "tool-call-error" if is_error else "tool-call-result"
                result_html = (
                    f'<div class="{result_cls}">'
                    f'{"&#10060; Error" if is_error else "&#10132; Result"}: '
                    f"{_esc(output_str)}</div>"
                )
            else:
                result_html = ""

            parts.append(
                f'<div class="tool-call-item">'
                f'<div class="tool-call-name">&#128295; {name}</div>'
                f'<div class="tool-call-input">Input: {_esc(input_str)}</div>'
                f"{result_html}"
                f"</div>"
            )

    if not parts:
        return ""
    return '<div class="tool-calls-section">' + "".join(parts) + "</div>"


def _get_task_for_interaction(interaction_id: str) -> Optional[Dict]:
    """Find the bg_task entry (if any) associated with a user input interaction."""
    for task_info in st.session_state.bg_tasks.values():
        if task_info.get("interaction_id") == interaction_id:
            return task_info
    return None


def _render_user_message(interaction: dict) -> None:
    """Render a user input bubble."""
    content = _esc(interaction.get("content", ""))
    st.markdown(
        f'<div class="msg-user">'
        f'<div class="msg-label">You</div>'
        f"{content}"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_assistant_message(interaction: dict) -> None:
    """Render an assistant output bubble with tool call details."""
    final_text = interaction.get("final_text", "")
    messages = interaction.get("messages", [])
    steps = interaction.get("steps", [])

    display = _esc(final_text or (messages[-1] if messages else "No response"))
    tool_html = _render_tool_calls_html(steps) if steps else ""

    st.markdown(
        f'<div class="msg-assistant">'
        f'<div class="msg-label">&#129302; Calculator Agent</div>'
        f"{display}"
        f"{tool_html}"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_pending_indicator(task_info: dict) -> None:
    """Render a pulsing indicator for a task that is still processing."""
    status = task_info.get("status", "pending")
    label = "Waiting in queue..." if status == "pending" else "Processing your request..."
    badge_cls = "badge-pending" if status == "pending" else "badge-progress"
    badge_text = status.upper().replace("_", " ")

    st.markdown(
        f'<div class="pending-indicator">'
        f'<div class="spinner"></div>'
        f'<span class="pending-text">{_esc(label)}</span>'
        f'<span class="badge {badge_cls}">{_esc(badge_text)}</span>'
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_failed_indicator(task_info: dict) -> None:
    """Render an error indicator for a failed / interrupted task."""
    status = task_info.get("status", "failed")
    error = task_info.get("error") or "Unknown error"
    badge_cls = "badge-failed"
    badge_text = status.upper()

    st.markdown(
        f'<div class="failed-indicator">'
        f'<span class="badge {badge_cls}">{_esc(badge_text)}</span> '
        f"<strong>Task {_esc(status)}:</strong> {_esc(error)}"
        f"</div>",
        unsafe_allow_html=True,
    )


def _render_interactions() -> None:
    """
    Render the full conversation: user inputs interleaved with assistant
    responses. For user inputs whose background task is still active (or
    failed), render the appropriate inline indicator.
    """
    interactions = st.session_state.interactions

    if not interactions:
        st.markdown(
            '<div class="empty-state">'
            '<div class="icon">&#128172;</div>'
            '<div class="title">No messages yet</div>'
            "Type a message below to start the conversation!"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    # Pre-compute set of user_input_ids that have an assistant response
    ids_with_response: set = set()
    for inter in interactions:
        if inter.get("role") == "assistant":
            uid = inter.get("user_input_interaction_id")
            if uid:
                ids_with_response.add(uid)

    for inter in interactions:
        role = inter.get("role", "")

        if role == "user":
            _render_user_message(inter)
            user_input_id = inter.get("id", "")

            # Show task indicator if there is no assistant response yet
            if user_input_id not in ids_with_response:
                task_info = _get_task_for_interaction(user_input_id)
                if task_info:
                    status = task_info.get("status", "")
                    if status in _ACTIVE_STATUSES:
                        _render_pending_indicator(task_info)
                    elif status in _TERMINAL_STATUSES:
                        _render_failed_indicator(task_info)

        elif role == "assistant":
            _render_assistant_message(inter)


# ═══════════════════════════════════════════════════════════════════════════
# TASK MONITOR (auto-refresh fragment)
# ═══════════════════════════════════════════════════════════════════════════

_HAS_FRAGMENT = callable(getattr(st, "fragment", None))


def _check_and_refresh_tasks() -> None:
    """
    Core monitor logic: drain WS events or poll, and if anything changed
    refresh interactions and trigger a full page rerun.
    """
    if not st.session_state.bg_tasks:
        return

    needs_refresh = _drain_ws_events()
    if not needs_refresh:
        needs_refresh = _poll_task_statuses()
    if needs_refresh:
        _refresh_interactions()
        st.rerun()


if _HAS_FRAGMENT:

    @st.fragment(run_every=timedelta(seconds=1))
    def _task_monitor_fragment() -> None:
        """Auto-refresh fragment: checks for completed tasks every second."""
        _check_and_refresh_tasks()

else:
    # Fallback for older Streamlit: no auto-refresh; manual refresh available
    _task_monitor_fragment = None  # type: ignore[assignment]


# ═══════════════════════════════════════════════════════════════════════════
# MAIN UI
# ═══════════════════════════════════════════════════════════════════════════

# ── Header ──────────────────────────────────────────────────────────────

st.markdown(
    '<div class="app-header">'
    "<h1>&#129518; Calculator Agent Chat</h1>"
    "<p>Powered by Claude AI with arithmetic tools</p>"
    "</div>",
    unsafe_allow_html=True,
)

# ── Sidebar ─────────────────────────────────────────────────────────────

with st.sidebar:
    st.markdown("### Actions")

    # New Chat button
    if st.button("&#10024; New Chat", use_container_width=True):
        _stop_ws_listener()
        try:
            with st.spinner("Creating chat..."):
                resp = api_create_chat(st.session_state.user_id)
            chat_data = resp.get("chat", {})
            st.session_state.chat_id = chat_data.get("chat_id")
            st.session_state.interactions = []
            st.session_state.bg_tasks = {}
            st.toast("New chat created!", icon="✅")
            st.rerun()
        except requests.exceptions.ConnectionError:
            st.error(
                f"Cannot connect to the gateway server at "
                f"**{GATEWAY_BASE_URL}**. Is it running?"
            )
        except requests.exceptions.Timeout:
            st.error("Connection timed out. The gateway server may not be running.")
        except requests.exceptions.RequestException as exc:
            st.error(f"Failed to create chat: {exc}")

    # Refresh button
    if st.session_state.chat_id:
        if st.button("&#128260; Refresh", use_container_width=True):
            _refresh_interactions()
            st.rerun()

    st.markdown("---")

    # Chat info
    st.markdown("### Chat Info")
    if st.session_state.chat_id:
        st.markdown(
            f'<div class="sidebar-card">'
            f'<div class="card-label">Chat ID</div>'
            f'<div class="card-value">{_esc(st.session_state.chat_id[:24])}...</div>'
            f"</div>",
            unsafe_allow_html=True,
        )

        # WebSocket status
        ws_ok = _is_ws_connected()
        dot_cls = "connected" if ws_ok else "disconnected"
        ws_label = "Live" if ws_ok else "Disconnected"
        st.markdown(
            f'<div class="ws-status">'
            f'<div class="ws-dot {dot_cls}"></div>'
            f"WebSocket: {ws_label}"
            f"</div>",
            unsafe_allow_html=True,
        )

        # Active background tasks count
        active_count = sum(
            1 for t in st.session_state.bg_tasks.values() if t["status"] in _ACTIVE_STATUSES
        )
        if active_count > 0:
            st.markdown(
                f'<div class="sidebar-card">'
                f'<div class="card-label">Background Tasks</div>'
                f'<div class="card-value" style="color: var(--accent-amber);">'
                f"{active_count} active"
                f"</div></div>",
                unsafe_allow_html=True,
            )
    else:
        st.markdown(
            '<div class="sidebar-card" style="text-align:center; color: var(--text-muted);">'
            "No active chat"
            "</div>",
            unsafe_allow_html=True,
        )

    st.markdown("---")

    # Available operations
    st.markdown("### Calculator Operations")
    st.markdown(
        '<div class="sidebar-card" style="font-size:0.85rem; line-height:1.7;">'
        '<strong style="color:var(--accent-blue);">add</strong> &#8211; Add two numbers<br>'
        '<strong style="color:var(--accent-blue);">multiply</strong> &#8211; Multiply two numbers<br>'
        '<strong style="color:var(--accent-blue);">divide</strong> &#8211; Divide numbers<br>'
        '<strong style="color:var(--accent-amber);">mihir_custom_transform</strong> &#8211; a &times; 28 + 12<br>'
        '<strong style="color:var(--accent-amber);">mihir_custom_series</strong> &#8211; [a-2 .. a+2]<br>'
        '<strong style="color:var(--accent-purple);">mihir_custom_log</strong> &#8211; Log a message'
        "</div>",
        unsafe_allow_html=True,
    )

# ── Ensure WebSocket ────────────────────────────────────────────────────

if st.session_state.chat_id:
    _ensure_ws_connected()

# ── Main Chat Area ──────────────────────────────────────────────────────

if not st.session_state.chat_id:
    # Welcome screen
    st.markdown(
        '<div class="chat-wrapper">'
        '<div class="empty-state">'
        '<div class="icon">&#129518;</div>'
        '<div class="title">Welcome to Calculator Agent Chat</div>'
        'Click <strong>"New Chat"</strong> in the sidebar to start a conversation.'
        "</div></div>",
        unsafe_allow_html=True,
    )
else:
    # Chat conversation
    st.markdown('<div class="chat-wrapper">', unsafe_allow_html=True)
    _render_interactions()
    st.markdown("</div>", unsafe_allow_html=True)

    # ── Message Input ───────────────────────────────────────────────────

    with st.form("msg_form", clear_on_submit=True):
        user_input = st.text_input(
            "Message",
            placeholder="Ask the calculator agent... (e.g., 'What is 42 * 17?')",
            label_visibility="collapsed",
        )
        col_mode, col_send = st.columns([3, 1])
        with col_mode:
            run_mode = st.radio(
                "Execution mode",
                options=["sync", "background"],
                horizontal=True,
                format_func=lambda x: "⚡ Sync" if x == "sync" else "🔄 Background",
                label_visibility="collapsed",
            )
        with col_send:
            submitted = st.form_submit_button("Send  &#10148;", use_container_width=True)

        if submitted and user_input.strip():
            content = user_input.strip()
            try:
                if run_mode == "sync":
                    # ── Sync: wait for response ─────────────────────────
                    with st.spinner("Thinking..."):
                        resp = api_send_message(
                            st.session_state.chat_id, content, "sync"
                        )
                    _refresh_interactions()
                    st.rerun()

                else:
                    # ── Background: submit and continue ─────────────────
                    resp = api_send_message(
                        st.session_state.chat_id, content, "background"
                    )
                    task = resp.get("task")
                    if task:
                        task_id = task.get("id", "")
                        interaction_id = task.get("interaction_id", "")
                        st.session_state.bg_tasks[task_id] = {
                            "task_id": task_id,
                            "interaction_id": interaction_id,
                            "status": task.get("status", "pending"),
                            "error": task.get("error"),
                        }
                    # Refresh interactions to include the new user input
                    _refresh_interactions()
                    st.rerun()

            except requests.exceptions.HTTPError as exc:
                status_code = (
                    exc.response.status_code if exc.response is not None else None
                )
                if status_code == 409:
                    st.warning(
                        "Another operation is in progress on this chat. "
                        "Please wait a moment and try again."
                    )
                elif status_code == 404:
                    st.error(
                        "Chat not found. It may have expired. "
                        "Please create a new chat."
                    )
                else:
                    st.error(f"Server error ({status_code}): {exc}")
            except requests.exceptions.ConnectionError:
                st.error(
                    "Cannot connect to the gateway server. "
                    f"Is it running at {GATEWAY_BASE_URL}?"
                )
            except requests.exceptions.Timeout:
                st.error("Request timed out. The server may be overloaded.")
            except requests.exceptions.RequestException as exc:
                st.error(f"Request failed: {exc}")

    # ── Example Prompts ─────────────────────────────────────────────────

    st.markdown("##### Quick Examples")
    example_cols = st.columns(4)

    _EXAMPLES = [
        ("&#10133; Add", "What is 125 + 847?"),
        ("&#10006; Multiply", "Calculate 42 times 17"),
        ("&#10135; Divide", "Divide 1000 by 7"),
        ("&#128290; Series", "Generate a custom series for 10"),
    ]

    for col, (label, prompt) in zip(example_cols, _EXAMPLES):
        with col:
            if st.button(label, use_container_width=True, key=f"ex_{label}"):
                try:
                    with st.spinner("Calculating..."):
                        api_send_message(st.session_state.chat_id, prompt, "sync")
                    _refresh_interactions()
                    st.rerun()
                except requests.exceptions.HTTPError as exc:
                    sc = exc.response.status_code if exc.response is not None else None
                    if sc == 409:
                        st.warning("Another operation is in progress. Try again shortly.")
                    else:
                        st.error(f"Error: {exc}")
                except requests.exceptions.RequestException as exc:
                    st.error(f"Error: {exc}")

    # ── Task Monitor ────────────────────────────────────────────────────

    if _HAS_FRAGMENT:
        _task_monitor_fragment()
    else:
        # Fallback: check once per page load + manual refresh button
        _check_and_refresh_tasks()
        if st.session_state.bg_tasks:
            active = [
                t for t in st.session_state.bg_tasks.values()
                if t["status"] in _ACTIVE_STATUSES
            ]
            if active:
                st.info(
                    f"&#9203; {len(active)} background task(s) processing. "
                    "Click **Refresh** in the sidebar to check for updates."
                )

# ── Footer ──────────────────────────────────────────────────────────────

st.markdown(
    '<div class="app-footer">'
    "Calculator Agent Gateway Demo &bull; Built with Streamlit &amp; FastAPI"
    "</div>",
    unsafe_allow_html=True,
)
