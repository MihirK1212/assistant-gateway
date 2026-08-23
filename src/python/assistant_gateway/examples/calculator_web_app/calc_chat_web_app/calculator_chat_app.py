"""
Calculator Chat Web App - Minimalist Streamlit UI for the Calculator Agent Gateway.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import queue
import threading
from datetime import timedelta
from typing import Any, Dict, List, Optional

import requests
import streamlit as st

logger = logging.getLogger(__name__)

# ─── Configuration ────────────────────────────────────────────────────────────

GATEWAY_BASE_URL = os.environ.get("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
CALCULATOR_API_URL = os.environ.get("CALCULATOR_API_URL", "http://127.0.0.1:5000")
GATEWAY_WS_URL = (
    GATEWAY_BASE_URL.replace("https://", "wss://").replace("http://", "ws://")
)
API_PREFIX = "/api/v1"

# ─── Page Config ──────────────────────────────────────────────────────────────

st.set_page_config(page_title="Calculator Agent", layout="centered")


# ─── API Client ───────────────────────────────────────────────────────────────

def api_create_chat(user_id: str = "user") -> dict:
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats"
    resp = requests.post(
        url,
        json={"user_id": user_id, "agent_name": "calculator"},
        timeout=(5, 15),
    )
    resp.raise_for_status()
    return resp.json()


def api_get_interactions(chat_id: str) -> dict:
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/interactions"
    resp = requests.get(url, timeout=(5, 10))
    resp.raise_for_status()
    return resp.json()


def api_send_message(
    chat_id: str,
    content: str,
    run_mode: str = "sync",
    backend_url: str = CALCULATOR_API_URL,
) -> dict:
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/messages"
    payload = {
        "content": content,
        "run_mode": run_mode,
        "input_overrides": {
            "__global__": {
                "backend_url": backend_url,
            }
        },
    }
    read_timeout = 15 if run_mode == "background" else 120
    resp = requests.post(url, json=payload, timeout=(5, read_timeout))
    resp.raise_for_status()
    return resp.json()


def api_get_task(chat_id: str, task_id: str) -> dict:
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/tasks/{task_id}"
    resp = requests.get(url, timeout=(5, 10))
    resp.raise_for_status()
    return resp.json()


# ─── WebSocket Listener ───────────────────────────────────────────────────────

def _ws_listener_thread(
    chat_id: str,
    event_queue: queue.Queue,
    stop_event: threading.Event,
) -> None:
    try:
        import websockets
    except ImportError:
        return

    async def _run() -> None:
        url = f"{GATEWAY_WS_URL}{API_PREFIX}/chats/{chat_id}/events"
        retry = 0
        while not stop_event.is_set() and retry < 10:
            try:
                async with websockets.connect(url) as ws:
                    retry = 0
                    while not stop_event.is_set():
                        try:
                            raw = await asyncio.wait_for(ws.recv(), timeout=1.0)
                            event = json.loads(raw)
                            event_queue.put(event)
                        except asyncio.TimeoutError:
                            continue
                        except Exception:
                            break
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


def _stop_ws_listener() -> None:
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
    chat_id = st.session_state.get("chat_id")
    if not chat_id:
        _stop_ws_listener()
        return

    thread: Optional[threading.Thread] = st.session_state.get("_ws_thread")
    if (
        st.session_state.get("_ws_chat_id") == chat_id
        and thread is not None
        and thread.is_alive()
    ):
        return

    _stop_ws_listener()
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
    t: Optional[threading.Thread] = st.session_state.get("_ws_thread")
    return t is not None and t.is_alive()


# ─── Task & Event Management ──────────────────────────────────────────────────

_TERMINAL_STATUSES = frozenset({"completed", "failed", "interrupted"})
_ACTIVE_STATUSES = frozenset({"pending", "in_progress"})


def _drain_ws_events() -> bool:
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
                    del st.session_state.bg_tasks[task_id]
                any_terminal = True

    return any_terminal


def _poll_task_statuses_fallback() -> bool:
    chat_id = st.session_state.get("chat_id")
    if not chat_id:
        return False

    any_terminal = False
    for task_id in list(st.session_state.bg_tasks.keys()):
        info = st.session_state.bg_tasks.get(task_id)
        if not info or info["status"] in _TERMINAL_STATUSES:
            continue

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
            pass

    return any_terminal


def _refresh_interactions() -> None:
    chat_id = st.session_state.get("chat_id")
    if not chat_id:
        return
    try:
        resp = api_get_interactions(chat_id)
        st.session_state.interactions = resp.get("interactions", [])
    except Exception:
        pass


def _get_task_for_interaction(interaction_id: str) -> Optional[Dict[str, Any]]:
    for task_info in st.session_state.bg_tasks.values():
        if task_info.get("interaction_id") == interaction_id:
            return task_info
    return None


def _check_and_refresh_tasks() -> None:
    if not st.session_state.bg_tasks:
        return

    needs_refresh = _drain_ws_events()
    if not needs_refresh and not _is_ws_connected():
        needs_refresh = _poll_task_statuses_fallback()

    if needs_refresh:
        _refresh_interactions()
        st.rerun()


# ─── Session State Initialization ─────────────────────────────────────────────

_STATE_DEFAULTS: Dict[str, Any] = {
    "chat_id": None,
    "user_id": "user",
    "interactions": [],
    "bg_tasks": {},
    "_ws_queue": None,
    "_ws_stop": None,
    "_ws_thread": None,
    "_ws_chat_id": None,
}

for _key, _default in _STATE_DEFAULTS.items():
    if _key not in st.session_state:
        st.session_state[_key] = _default

# Automatically start a new chat if none is active
if not st.session_state.chat_id:
    try:
        chat_resp = api_create_chat(st.session_state.user_id)
        st.session_state.chat_id = chat_resp.get("chat", {}).get("chat_id")
        st.session_state.interactions = []
        st.session_state.bg_tasks = {}
    except Exception as exc:
        st.error(f"Cannot connect to gateway at {GATEWAY_BASE_URL}: {exc}")
        st.stop()

_ensure_ws_connected()


# ─── UI Layout ────────────────────────────────────────────────────────────────

col_title, col_btn = st.columns([4, 1])
with col_title:
    st.subheader("Calculator Agent")
with col_btn:
    if st.button("New Chat", use_container_width=True):
        _stop_ws_listener()
        st.session_state.chat_id = None
        st.rerun()

chat_container = st.container()

def _render_interactions() -> None:
    responded_user_ids = {
        inter.get("user_input_interaction_id")
        for inter in st.session_state.interactions
        if inter.get("role") == "assistant" and inter.get("user_input_interaction_id")
    }

    for inter in st.session_state.interactions:
        role = inter.get("role")
        if role == "user":
            with st.chat_message("user"):
                st.write(inter.get("content", ""))
                uid = inter.get("id")
                if uid and uid not in responded_user_ids:
                    task_info = _get_task_for_interaction(uid)
                    if task_info:
                        status = task_info.get("status")
                        if status in _ACTIVE_STATUSES:
                            st.caption(f"Task status: {status}")
                        elif status in _TERMINAL_STATUSES:
                            st.error(f"Task {status}: {task_info.get('error') or 'Error'}")

        elif role == "assistant":
            with st.chat_message("assistant"):
                text = inter.get("final_text") or (inter.get("messages") or [""])[-1]
                st.write(text)

                # Minimal tool calls display
                steps = inter.get("steps", [])
                tool_entries = []
                for step in steps:
                    for tc in step.get("tool_calls", []):
                        tool_entries.append(
                            f"{tc.get('name')}: {json.dumps(tc.get('input', {}))}"
                        )
                if tool_entries:
                    with st.expander("Tools", expanded=False):
                        for entry in tool_entries:
                            st.text(entry)

# Mode selector and message input form together
with st.form("chat_form", clear_on_submit=True):
    user_input = st.text_input(
        "Message",
        placeholder="Type your message...",
        label_visibility="collapsed",
    )
    col_mode, col_send = st.columns([4, 1])
    with col_mode:
        run_mode = st.radio(
            "Execution mode",
            options=["sync", "background"],
            horizontal=True,
            label_visibility="collapsed",
        )
    with col_send:
        submitted = st.form_submit_button("Send", use_container_width=True)

if submitted and user_input.strip():
    prompt = user_input.strip()
    with chat_container:
        _render_interactions()
        with st.chat_message("user"):
            st.write(prompt)
            if run_mode == "background":
                st.caption("Task status: pending")

        if run_mode == "sync":
            with st.chat_message("assistant"):
                with st.spinner("Processing..."):
                    try:
                        api_send_message(st.session_state.chat_id, prompt, "sync")
                        _refresh_interactions()
                    except requests.exceptions.RequestException as exc:
                        st.error(f"Request failed: {exc}")
            st.rerun()
        else:
            try:
                resp = api_send_message(st.session_state.chat_id, prompt, "background")
                task = resp.get("task")
                if task:
                    task_id = task.get("id", "")
                    st.session_state.bg_tasks[task_id] = {
                        "task_id": task_id,
                        "interaction_id": task.get("interaction_id", ""),
                        "status": task.get("status", "pending"),
                        "error": task.get("error"),
                    }
                _refresh_interactions()
            except requests.exceptions.RequestException as exc:
                st.error(f"Request failed: {exc}")
            st.rerun()
else:
    with chat_container:
        _render_interactions()

# Auto-refresh task monitor for background tasks
if callable(getattr(st, "fragment", None)):
    @st.fragment(run_every=timedelta(seconds=1))
    def _task_monitor_fragment() -> None:
        _check_and_refresh_tasks()

    _task_monitor_fragment()
else:
    _check_and_refresh_tasks()
