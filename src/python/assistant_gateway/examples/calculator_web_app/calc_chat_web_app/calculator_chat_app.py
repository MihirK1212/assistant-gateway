"""
Calculator Chat Web App - Streamlit UI for the Calculator Agent Gateway

This app demonstrates:
- New chat creation (calculator agent selected by default)
- Interactions with the agent via messages
- Sync and background task modes with appropriate UI handling
- Chat history viewing with tool call details
- Background task status polling
"""

from __future__ import annotations

import os
import sys
import time
from typing import Optional

import requests
import streamlit as st

# Ensure proper imports
CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(CURRENT_DIR)))
sys.path.append(os.path.dirname(CURRENT_DIR))
sys.path.append(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR))))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR))))
)

# Configuration
GATEWAY_BASE_URL = os.environ.get("GATEWAY_BASE_URL", "http://127.0.0.1:8000")
API_PREFIX = "/api/v1"

# Custom CSS for a beautiful, distinctive UI
st.set_page_config(
    page_title="Calculator Agent Chat",
    page_icon="🧮",
    layout="wide",
    initial_sidebar_state="expanded",
)

st.markdown(
    """
<style>
    @import url('https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;500;600&family=DM+Sans:wght@400;500;600;700&display=swap');
    
    :root {
        --bg-primary: #fafbfc;
        --bg-secondary: #ffffff;
        --bg-tertiary: #f4f6f8;
        --bg-hover: #eef1f4;
        --accent-blue: #2563eb;
        --accent-green: #059669;
        --accent-orange: #d97706;
        --accent-red: #dc2626;
        --accent-purple: #7c3aed;
        --text-primary: #1e293b;
        --text-secondary: #64748b;
        --text-muted: #94a3b8;
        --border-color: #e2e8f0;
        --border-focus: #93c5fd;
    }
    
    .stApp {
        background: var(--bg-primary);
    }
    
    .main-header {
        font-family: 'DM Sans', sans-serif;
        font-size: 2.4rem;
        font-weight: 700;
        color: var(--text-primary);
        margin-bottom: 0.25rem;
        text-align: center;
    }
    
    .sub-header {
        font-family: 'DM Sans', sans-serif;
        font-size: 1rem;
        color: var(--text-secondary);
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .chat-container {
        background: var(--bg-secondary);
        border-radius: 12px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        border: 1px solid var(--border-color);
        box-shadow: 0 1px 3px rgba(0, 0, 0, 0.05);
    }
    
    .user-message {
        background: #eff6ff;
        border-left: 3px solid var(--accent-blue);
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 0.75rem 0;
        font-family: 'DM Sans', sans-serif;
        color: var(--text-primary);
    }
    
    .assistant-message {
        background: #f0fdf4;
        border-left: 3px solid var(--accent-green);
        padding: 1rem 1.25rem;
        border-radius: 8px;
        margin: 0.75rem 0;
        font-family: 'DM Sans', sans-serif;
        color: var(--text-primary);
    }
    
    .tool-call-container {
        background: #fffbeb;
        border: 1px solid #fcd34d;
        border-radius: 6px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        font-family: 'IBM Plex Mono', monospace;
        font-size: 0.85rem;
        color: var(--text-primary);
    }
    
    .tool-name {
        color: #b45309;
        font-weight: 600;
    }
    
    .tool-input {
        color: var(--text-secondary);
        margin-top: 0.25rem;
    }
    
    .tool-result {
        color: #047857;
        margin-top: 0.5rem;
        font-weight: 500;
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 6px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }
    
    .status-pending {
        background: #fef3c7;
        color: #92400e;
        border: 1px solid #fcd34d;
    }
    
    .status-completed {
        background: #d1fae5;
        color: #065f46;
        border: 1px solid #6ee7b7;
    }
    
    .status-failed {
        background: #fee2e2;
        color: #991b1b;
        border: 1px solid #fca5a5;
    }
    
    .status-in-progress {
        background: #dbeafe;
        color: #1e40af;
        border: 1px solid #93c5fd;
    }
    
    .sidebar-info {
        background: var(--bg-tertiary);
        border-radius: 8px;
        padding: 1rem;
        margin: 0.5rem 0;
        border: 1px solid var(--border-color);
    }
    
    .stTextInput > div > div > input {
        background-color: var(--bg-secondary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 8px !important;
        color: var(--text-primary) !important;
        font-family: 'DM Sans', sans-serif !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: var(--accent-blue) !important;
        box-shadow: 0 0 0 3px rgba(37, 99, 235, 0.1) !important;
    }
    
    .stButton > button {
        background: var(--accent-blue) !important;
        color: white !important;
        border: none !important;
        border-radius: 8px !important;
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important;
        padding: 0.5rem 1.5rem !important;
        transition: all 0.2s ease !important;
    }
    
    .stButton > button:hover {
        background: #1d4ed8 !important;
        box-shadow: 0 4px 12px rgba(37, 99, 235, 0.25) !important;
    }
    
    .stRadio > div {
        background: var(--bg-tertiary);
        border-radius: 8px;
        padding: 0.5rem;
    }
    
    .stRadio label {
        color: var(--text-primary) !important;
        font-family: 'DM Sans', sans-serif !important;
    }
    
    .info-card {
        background: var(--bg-secondary);
        border-radius: 8px;
        padding: 1rem;
        border: 1px solid var(--border-color);
        margin: 0.5rem 0;
    }
    
    .metric-value {
        font-family: 'IBM Plex Mono', monospace;
        font-size: 1.25rem;
        color: var(--accent-blue);
        font-weight: 600;
    }
    
    .metric-label {
        font-family: 'DM Sans', sans-serif;
        font-size: 0.8rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.05em;
        margin-bottom: 0.25rem;
    }
    
    /* Make sidebar readable */
    [data-testid="stSidebar"] {
        background: var(--bg-secondary);
    }
    
    [data-testid="stSidebar"] .sidebar-info {
        color: var(--text-primary);
    }
</style>
""",
    unsafe_allow_html=True,
)


# API Client Functions
def create_chat(user_id: str) -> dict:
    """Create a new chat with the calculator agent."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats"
    payload = {
        "user_id": user_id,
        "agent_name": "calculator",
        "metadata": {},
        "extra_metadata": {},
    }
    response = requests.post(url, json=payload, timeout=30)
    response.raise_for_status()
    return response.json()


def get_chat(chat_id: str) -> dict:
    """Get chat metadata."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def get_interactions(chat_id: str) -> dict:
    """Get all interactions for a chat."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/interactions"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def send_message(
    chat_id: str, content: str, run_mode: str = "sync"
) -> dict:
    """Send a message to the chat."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/messages"
    payload = {
        "content": content,
        "run_mode": run_mode,
        "message_metadata": {},
        "user_context": None,
        "backend_server_context": None,
    }
    response = requests.post(url, json=payload, timeout=120)
    response.raise_for_status()
    return response.json()


def get_task_status(chat_id: str, task_id: str) -> dict:
    """Get the status of a background task."""
    url = f"{GATEWAY_BASE_URL}{API_PREFIX}/chats/{chat_id}/tasks/{task_id}"
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.json()


def poll_task_until_complete(
    chat_id: str, task_id: str, placeholder, max_polls: int = 60, poll_interval: float = 1.0
) -> dict:
    """Poll a background task until it completes or fails."""
    for i in range(max_polls):
        task_response = get_task_status(chat_id, task_id)
        task = task_response.get("task", {})
        status = task.get("status", "unknown")
        
        # Update the UI with current status
        with placeholder:
            if status == "pending":
                st.markdown(
                    f"""
                    <div class="info-card">
                        <span class="status-badge status-pending">⏳ Pending</span>
                        <span style="margin-left: 1rem; color: #64748b;">
                            Waiting in queue... (Poll {i+1}/{max_polls})
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            elif status == "in_progress":
                st.markdown(
                    f"""
                    <div class="info-card">
                        <span class="status-badge status-in-progress">🔄 In Progress</span>
                        <span style="margin-left: 1rem; color: #64748b;">
                            Processing your request... (Poll {i+1}/{max_polls})
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
            elif status == "completed":
                st.markdown(
                    """
                    <div class="info-card">
                        <span class="status-badge status-completed">✓ Completed</span>
                        <span style="margin-left: 1rem; color: #64748b;">
                            Task finished successfully!
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                return task_response
            elif status == "failed":
                error_msg = task.get("error", "Unknown error")
                st.markdown(
                    f"""
                    <div class="info-card">
                        <span class="status-badge status-failed">✗ Failed</span>
                        <span style="margin-left: 1rem; color: #64748b;">
                            {error_msg}
                        </span>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                return task_response
        
        if status in ("completed", "failed"):
            return task_response
        
        time.sleep(poll_interval)
    
    return task_response


def render_tool_calls(steps: list) -> str:
    """Render tool calls from agent steps."""
    html_parts = []
    for step in steps:
        tool_calls = step.get("tool_calls", [])
        tool_results = step.get("tool_results", [])
        
        # Create a mapping of tool call ids to results
        result_map = {r.get("tool_call_id"): r for r in tool_results}
        
        for tc in tool_calls:
            tool_name = tc.get("name", "unknown")
            tool_input = tc.get("input", {})
            tool_id = tc.get("id", "")
            result = result_map.get(tool_id, {})
            
            html_parts.append(f"""
            <div class="tool-call-container">
                <div class="tool-name">🔧 {tool_name}</div>
                <div style="color: #64748b; margin-top: 0.25rem;">
                    Input: {tool_input}
                </div>
                <div class="tool-result">
                    → Result: {result.get('output', 'N/A')}
                </div>
            </div>
            """)
    
    return "".join(html_parts)


def render_interaction(interaction: dict) -> None:
    """Render a single interaction (user input or assistant output)."""
    role = interaction.get("role", "")
    
    if role == "user":
        content = interaction.get("content", "")
        st.markdown(
            f"""
            <div class="user-message">
                <strong style="color: #1d4ed8;">You:</strong><br/>
                {content}
            </div>
            """,
            unsafe_allow_html=True,
        )
    elif role == "assistant":
        final_text = interaction.get("final_text", "")
        messages = interaction.get("messages", [])
        steps = interaction.get("steps", [])
        
        # Display the main response
        display_text = final_text or (messages[-1] if messages else "No response")
        
        tool_calls_html = render_tool_calls(steps) if steps else ""
        
        st.markdown(
            f"""
            <div class="assistant-message">
                <strong style="color: #047857;">🤖 Calculator Agent:</strong><br/>
                {display_text}
                {tool_calls_html}
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_chat_history(interactions: list) -> None:
    """Render the full chat history."""
    if not interactions:
        st.markdown(
            """
            <div style="text-align: center; padding: 3rem; color: #64748b;">
                <div style="font-size: 3rem; margin-bottom: 1rem;">💬</div>
                <div style="font-family: 'DM Sans', sans-serif;">
                    Start a conversation by typing a message below!
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
        return
    
    for interaction in interactions:
        render_interaction(interaction)


# Initialize session state
if "chat_id" not in st.session_state:
    st.session_state.chat_id = None
if "user_id" not in st.session_state:
    st.session_state.user_id = "streamlit_user"
if "interactions" not in st.session_state:
    st.session_state.interactions = []
if "pending_task" not in st.session_state:
    st.session_state.pending_task = None


# Main UI
st.markdown('<h1 class="main-header">🧮 Calculator Agent Chat</h1>', unsafe_allow_html=True)
st.markdown(
    '<p class="sub-header">Powered by Claude AI with arithmetic tools</p>',
    unsafe_allow_html=True,
)

# Sidebar
with st.sidebar:
    st.markdown("### ⚙️ Configuration")
    
    st.markdown(
        f"""
        <div class="sidebar-info">
            <div class="metric-label">Gateway URL</div>
            <div style="font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; color: #1e293b; word-break: break-all;">
                {GATEWAY_BASE_URL}
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    
    if st.session_state.chat_id:
        st.markdown(
            f"""
            <div class="sidebar-info">
                <div class="metric-label">Active Chat</div>
                <div style="font-family: 'IBM Plex Mono', monospace; font-size: 0.75rem; color: #2563eb; word-break: break-all;">
                    {st.session_state.chat_id}
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )
    
    st.markdown("---")
    st.markdown("### 🚀 Actions")
    
    # New Chat Button
    if st.button("🆕 New Chat", use_container_width=True):
        try:
            response = create_chat(st.session_state.user_id)
            chat_data = response.get("chat", {})
            st.session_state.chat_id = chat_data.get("chat_id")
            st.session_state.interactions = []
            st.session_state.pending_task = None
            st.success("New chat created!")
            st.rerun()
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to create chat: {e}")
    
    # Refresh Interactions Button
    if st.session_state.chat_id:
        if st.button("🔄 Refresh History", use_container_width=True):
            try:
                response = get_interactions(st.session_state.chat_id)
                st.session_state.interactions = response.get("interactions", [])
                st.success("History refreshed!")
                st.rerun()
            except requests.exceptions.RequestException as e:
                st.error(f"Failed to refresh: {e}")
    
    st.markdown("---")
    st.markdown("### 📊 Run Mode")
    run_mode = st.radio(
        "Select execution mode:",
        options=["sync", "background"],
        format_func=lambda x: "⚡ Synchronous" if x == "sync" else "🔄 Background",
        help="Sync: Wait for response. Background: Submit and poll for result.",
    )
    
    st.markdown("---")
    st.markdown("### 📚 Available Operations")
    st.markdown(
        """
        <div class="sidebar-info" style="font-size: 0.85rem; color: #1e293b;">
            <div style="margin-bottom: 0.5rem;"><strong style="color: #2563eb;">add</strong> - Add two numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: #2563eb;">multiply</strong> - Multiply two numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: #2563eb;">divide</strong> - Divide numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: #d97706;">mihir_custom_transform</strong> - a × 28 + 12</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: #d97706;">mihir_custom_series</strong> - [a-2, a-1, a, a+1, a+2]</div>
            <div><strong style="color: #7c3aed;">mihir_custom_log</strong> - Log a message</div>
        </div>
        """,
        unsafe_allow_html=True,
    )

# Main content area
col1, col2 = st.columns([2, 1])

with col1:
    st.markdown('<div class="chat-container">', unsafe_allow_html=True)
    
    if not st.session_state.chat_id:
        st.markdown(
            """
            <div style="text-align: center; padding: 4rem 2rem;">
                <div style="font-size: 4rem; margin-bottom: 1.5rem;">🧮</div>
                <h3 style="font-family: 'DM Sans', sans-serif; color: #1e293b; margin-bottom: 1rem;">
                    Welcome to Calculator Agent Chat
                </h3>
                <p style="color: #64748b; font-family: 'DM Sans', sans-serif;">
                    Click <strong>"New Chat"</strong> in the sidebar to start a conversation with the calculator agent.
                </p>
            </div>
            """,
            unsafe_allow_html=True,
        )
    else:
        # Chat history
        st.markdown("#### 💬 Conversation")
        render_chat_history(st.session_state.interactions)
    
    st.markdown("</div>", unsafe_allow_html=True)
    
    # Message input
    if st.session_state.chat_id:
        st.markdown("---")
        
        with st.form("message_form", clear_on_submit=True):
            user_input = st.text_input(
                "Your message",
                placeholder="Ask me to calculate something... (e.g., 'What is 42 * 17?')",
                label_visibility="collapsed",
            )
            submitted = st.form_submit_button("Send 📤", use_container_width=True)
            
            if submitted and user_input:
                try:
                    if run_mode == "sync":
                        # Synchronous mode - wait for response
                        with st.spinner("🤔 Calculating..."):
                            response = send_message(
                                st.session_state.chat_id, user_input, "sync"
                            )
                        
                        # Refresh interactions to show the new message
                        interactions_response = get_interactions(st.session_state.chat_id)
                        st.session_state.interactions = interactions_response.get(
                            "interactions", []
                        )
                        st.rerun()
                    
                    else:
                        # Background mode - submit and poll
                        response = send_message(
                            st.session_state.chat_id, user_input, "background"
                        )
                        task = response.get("task")
                        
                        if task:
                            task_id = task.get("id")
                            st.session_state.pending_task = task_id
                            
                            # Create a placeholder for task status updates
                            status_placeholder = st.empty()
                            
                            # Poll for task completion
                            final_response = poll_task_until_complete(
                                st.session_state.chat_id,
                                task_id,
                                status_placeholder,
                            )
                            
                            # Refresh interactions
                            interactions_response = get_interactions(
                                st.session_state.chat_id
                            )
                            st.session_state.interactions = interactions_response.get(
                                "interactions", []
                            )
                            st.session_state.pending_task = None
                            st.rerun()
                        else:
                            st.warning("No task returned from background request")
                
                except requests.exceptions.RequestException as e:
                    st.error(f"Error sending message: {e}")

with col2:
    st.markdown("### 📊 Chat Details")
    
    if st.session_state.chat_id:
        try:
            chat_response = get_chat(st.session_state.chat_id)
            chat_data = chat_response.get("chat", {})
            
            st.markdown(
                f"""
                <div class="info-card">
                    <div class="metric-label">Status</div>
                    <div class="metric-value" style="font-size: 1rem;">
                        {"🟢 Active" if chat_data.get("status") == "active" else "🔴 Archived"}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            st.markdown(
                f"""
                <div class="info-card">
                    <div class="metric-label">Agent</div>
                    <div class="metric-value" style="font-size: 1rem;">
                        🤖 {chat_data.get("agent_name", "N/A")}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            st.markdown(
                f"""
                <div class="info-card">
                    <div class="metric-label">Messages</div>
                    <div class="metric-value">
                        {len(st.session_state.interactions)}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            created_at = chat_data.get("created_at", "N/A")
            if created_at and created_at != "N/A":
                # Format the timestamp
                try:
                    from datetime import datetime
                    dt = datetime.fromisoformat(created_at.replace("Z", "+00:00"))
                    created_at = dt.strftime("%Y-%m-%d %H:%M")
                except:
                    pass
            
            st.markdown(
                f"""
                <div class="info-card">
                    <div class="metric-label">Created</div>
                    <div style="font-family: 'IBM Plex Mono', monospace; font-size: 0.8rem; color: #1e293b;">
                        {created_at}
                    </div>
                </div>
                """,
                unsafe_allow_html=True,
            )
            
            # Show last task info if available
            last_task_id = chat_data.get("last_task_id")
            if last_task_id:
                st.markdown("---")
                st.markdown("### 🔄 Last Task")
                try:
                    task_response = get_task_status(st.session_state.chat_id, last_task_id)
                    task = task_response.get("task", {})
                    task_status = task.get("status", "unknown")
                    
                    status_class = {
                        "pending": "status-pending",
                        "in_progress": "status-in-progress", 
                        "completed": "status-completed",
                        "failed": "status-failed",
                    }.get(task_status, "status-pending")
                    
                    st.markdown(
                        f"""
                        <div class="info-card">
                            <span class="status-badge {status_class}">{task_status.upper()}</span>
                            <div style="margin-top: 0.5rem; font-family: 'IBM Plex Mono', monospace; font-size: 0.7rem; color: #64748b; word-break: break-all;">
                                {last_task_id[:20]}...
                            </div>
                        </div>
                        """,
                        unsafe_allow_html=True,
                    )
                except:
                    pass
        
        except requests.exceptions.RequestException as e:
            st.error(f"Failed to fetch chat details: {e}")
    else:
        st.markdown(
            """
            <div class="info-card" style="text-align: center; padding: 2rem;">
                <div style="font-size: 2rem; margin-bottom: 0.5rem;">📭</div>
                <div style="color: #64748b; font-family: 'DM Sans', sans-serif;">
                    No active chat
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

# Example prompts section
if st.session_state.chat_id:
    st.markdown("---")
    st.markdown("### 💡 Try These Examples")
    
    example_cols = st.columns(4)
    
    examples = [
        ("➕ Add", "What is 125 + 847?"),
        ("✖️ Multiply", "Calculate 42 times 17"),
        ("➗ Divide", "Divide 1000 by 7"),
        ("🔢 Series", "Generate a custom series for 10"),
    ]
    
    for col, (label, prompt) in zip(example_cols, examples):
        with col:
            if st.button(label, use_container_width=True, key=f"example_{label}"):
                try:
                    with st.spinner("Processing..."):
                        response = send_message(st.session_state.chat_id, prompt, "sync")
                    interactions_response = get_interactions(st.session_state.chat_id)
                    st.session_state.interactions = interactions_response.get(
                        "interactions", []
                    )
                    st.rerun()
                except requests.exceptions.RequestException as e:
                    st.error(f"Error: {e}")

# Footer
st.markdown("---")
st.markdown(
    """
    <div style="text-align: center; padding: 1rem; color: #64748b; font-family: 'DM Sans', sans-serif; font-size: 0.85rem;">
        Calculator Agent Gateway Demo • Built with Streamlit & FastAPI
    </div>
    """,
    unsafe_allow_html=True,
)

