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
    @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&family=Outfit:wght@300;400;600;700&display=swap');
    
    :root {
        --bg-primary: #0d0d12;
        --bg-secondary: #16161d;
        --bg-tertiary: #1e1e28;
        --accent-cyan: #00d4aa;
        --accent-magenta: #ff3366;
        --accent-amber: #ffb800;
        --text-primary: #e8e8ed;
        --text-secondary: #9898a6;
        --border-color: #2a2a3a;
    }
    
    .stApp {
        background: linear-gradient(145deg, var(--bg-primary) 0%, #0a0a10 100%);
    }
    
    .main-header {
        font-family: 'Outfit', sans-serif;
        font-size: 2.8rem;
        font-weight: 700;
        background: linear-gradient(135deg, var(--accent-cyan) 0%, var(--accent-amber) 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        margin-bottom: 0.5rem;
        text-align: center;
    }
    
    .sub-header {
        font-family: 'Outfit', sans-serif;
        font-size: 1rem;
        color: var(--text-secondary);
        text-align: center;
        margin-bottom: 2rem;
    }
    
    .chat-container {
        background: var(--bg-secondary);
        border-radius: 16px;
        padding: 1.5rem;
        margin-bottom: 1rem;
        border: 1px solid var(--border-color);
        box-shadow: 0 4px 24px rgba(0, 0, 0, 0.3);
    }
    
    .user-message {
        background: linear-gradient(135deg, #1a3a4a 0%, #1a2a3a 100%);
        border-left: 3px solid var(--accent-cyan);
        padding: 1rem 1.25rem;
        border-radius: 12px;
        margin: 0.75rem 0;
        font-family: 'Outfit', sans-serif;
    }
    
    .assistant-message {
        background: linear-gradient(135deg, #2a1a3a 0%, #1a1a2a 100%);
        border-left: 3px solid var(--accent-magenta);
        padding: 1rem 1.25rem;
        border-radius: 12px;
        margin: 0.75rem 0;
        font-family: 'Outfit', sans-serif;
    }
    
    .tool-call-container {
        background: var(--bg-tertiary);
        border: 1px solid var(--accent-amber);
        border-radius: 8px;
        padding: 0.75rem 1rem;
        margin: 0.5rem 0;
        font-family: 'JetBrains Mono', monospace;
        font-size: 0.85rem;
    }
    
    .tool-name {
        color: var(--accent-amber);
        font-weight: 600;
    }
    
    .tool-result {
        color: var(--accent-cyan);
        margin-top: 0.5rem;
    }
    
    .status-badge {
        display: inline-block;
        padding: 0.25rem 0.75rem;
        border-radius: 20px;
        font-size: 0.75rem;
        font-weight: 600;
        text-transform: uppercase;
        letter-spacing: 0.05em;
    }
    
    .status-pending {
        background: rgba(255, 184, 0, 0.2);
        color: var(--accent-amber);
        border: 1px solid var(--accent-amber);
    }
    
    .status-completed {
        background: rgba(0, 212, 170, 0.2);
        color: var(--accent-cyan);
        border: 1px solid var(--accent-cyan);
    }
    
    .status-failed {
        background: rgba(255, 51, 102, 0.2);
        color: var(--accent-magenta);
        border: 1px solid var(--accent-magenta);
    }
    
    .status-in-progress {
        background: rgba(100, 100, 255, 0.2);
        color: #6464ff;
        border: 1px solid #6464ff;
    }
    
    .sidebar-info {
        background: var(--bg-tertiary);
        border-radius: 12px;
        padding: 1rem;
        margin: 0.5rem 0;
        border: 1px solid var(--border-color);
    }
    
    .stTextInput > div > div > input {
        background-color: var(--bg-tertiary) !important;
        border: 1px solid var(--border-color) !important;
        border-radius: 10px !important;
        color: var(--text-primary) !important;
        font-family: 'Outfit', sans-serif !important;
    }
    
    .stTextInput > div > div > input:focus {
        border-color: var(--accent-cyan) !important;
        box-shadow: 0 0 0 2px rgba(0, 212, 170, 0.2) !important;
    }
    
    .stButton > button {
        background: linear-gradient(135deg, var(--accent-cyan) 0%, #00a080 100%) !important;
        color: var(--bg-primary) !important;
        border: none !important;
        border-radius: 10px !important;
        font-family: 'Outfit', sans-serif !important;
        font-weight: 600 !important;
        padding: 0.5rem 1.5rem !important;
        transition: all 0.3s ease !important;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px) !important;
        box-shadow: 0 4px 12px rgba(0, 212, 170, 0.3) !important;
    }
    
    .stRadio > div {
        background: var(--bg-tertiary);
        border-radius: 10px;
        padding: 0.5rem;
    }
    
    .stRadio label {
        color: var(--text-primary) !important;
        font-family: 'Outfit', sans-serif !important;
    }
    
    .info-card {
        background: linear-gradient(135deg, var(--bg-tertiary) 0%, var(--bg-secondary) 100%);
        border-radius: 12px;
        padding: 1rem;
        border: 1px solid var(--border-color);
        margin: 0.5rem 0;
    }
    
    .metric-value {
        font-family: 'JetBrains Mono', monospace;
        font-size: 1.5rem;
        color: var(--accent-cyan);
        font-weight: 700;
    }
    
    .metric-label {
        font-family: 'Outfit', sans-serif;
        font-size: 0.85rem;
        color: var(--text-secondary);
        text-transform: uppercase;
        letter-spacing: 0.1em;
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
                        <span style="margin-left: 1rem; color: var(--text-secondary);">
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
                        <span style="margin-left: 1rem; color: var(--text-secondary);">
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
                        <span style="margin-left: 1rem; color: var(--text-secondary);">
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
                        <span style="margin-left: 1rem; color: var(--text-secondary);">
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
                <div style="color: var(--text-secondary); margin-top: 0.25rem;">
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
                <strong style="color: var(--accent-cyan);">You:</strong><br/>
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
                <strong style="color: var(--accent-magenta);">🤖 Calculator Agent:</strong><br/>
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
            <div style="text-align: center; padding: 3rem; color: var(--text-secondary);">
                <div style="font-size: 3rem; margin-bottom: 1rem;">💬</div>
                <div style="font-family: 'Outfit', sans-serif;">
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
            <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: var(--text-primary); word-break: break-all;">
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
                <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.75rem; color: var(--accent-cyan); word-break: break-all;">
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
        <div class="sidebar-info" style="font-size: 0.85rem;">
            <div style="margin-bottom: 0.5rem;"><strong style="color: var(--accent-cyan);">add</strong> - Add two numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: var(--accent-cyan);">multiply</strong> - Multiply two numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: var(--accent-cyan);">divide</strong> - Divide numbers</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: var(--accent-amber);">mihir_custom_transform</strong> - a × 28 + 12</div>
            <div style="margin-bottom: 0.5rem;"><strong style="color: var(--accent-amber);">mihir_custom_series</strong> - [a-2, a-1, a, a+1, a+2]</div>
            <div><strong style="color: var(--accent-magenta);">mihir_custom_log</strong> - Log a message</div>
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
                <h3 style="font-family: 'Outfit', sans-serif; color: var(--text-primary); margin-bottom: 1rem;">
                    Welcome to Calculator Agent Chat
                </h3>
                <p style="color: var(--text-secondary); font-family: 'Outfit', sans-serif;">
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
                    <div style="font-family: 'JetBrains Mono', monospace; font-size: 0.8rem; color: var(--text-primary);">
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
                            <div style="margin-top: 0.5rem; font-family: 'JetBrains Mono', monospace; font-size: 0.7rem; color: var(--text-secondary); word-break: break-all;">
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
                <div style="color: var(--text-secondary); font-family: 'Outfit', sans-serif;">
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
    <div style="text-align: center; padding: 1rem; color: var(--text-secondary); font-family: 'Outfit', sans-serif; font-size: 0.85rem;">
        Calculator Agent Gateway Demo • Built with Streamlit & FastAPI
    </div>
    """,
    unsafe_allow_html=True,
)

