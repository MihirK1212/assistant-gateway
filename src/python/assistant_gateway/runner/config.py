import inspect
import json
from dataclasses import dataclass
from typing import Optional

from assistant_gateway.chat_orchestrator.chat.store.base import ChatStore
from assistant_gateway.chat_orchestrator.chat.store.file_system.store import FileSystemChatStore
from assistant_gateway.chat_orchestrator.core.config import GatewayConfig
from assistant_gateway.clauq_btm.instance import ClauqBTM
from assistant_gateway.rest_api.fast_api_rest_assistant.enrich import enrich_app_with_assistant_router
from assistant_gateway.runner.loader import load_attribute
from fastapi import FastAPI


@dataclass
class ParseResult:
    gateway_config: GatewayConfig
    app: Optional[FastAPI] = None


def parse_config(config_path: str) -> ParseResult:
    with open(config_path) as f:
        user_config = json.load(f)

    agents = {name: load_attribute(path) for name, path in user_config.get("agents", {}).items()}

    chat_store = None
    if user_config.get("chat_store"):
        chat_store_config = user_config.get("chat_store")
        if isinstance(chat_store_config, str):
            chat_store_attr = load_attribute(chat_store_config)
            if inspect.isfunction(chat_store_attr):
                chat_store = chat_store_attr()
            elif isinstance(chat_store_attr, ChatStore):
                chat_store = chat_store_attr
            else:
                raise ValueError(f"Unknown chat_store config: {chat_store_config!r}")
        else:
            if chat_store_config.get("type") == "file_system":
                config = chat_store_config.get("config")
                if not isinstance(config, dict):
                    raise ValueError(f"Invalid chat_store config: {config!r}")
                chat_store = FileSystemChatStore(**config)
            else:
                raise ValueError(f"Unknown chat_store type: {chat_store_config.get('type')!r}")

    clauq_btm = None
    if user_config.get("clauq_btm"):
        clauq_btm_config = user_config.get("clauq_btm")
        if isinstance(clauq_btm_config, str):
            clauq_btm_attr = load_attribute(clauq_btm_config)
            if inspect.isfunction(clauq_btm_attr):
                clauq_btm = clauq_btm_attr()
            elif isinstance(clauq_btm_attr, ClauqBTM):
                clauq_btm = clauq_btm_attr
            else:
                raise ValueError(f"Unknown clauq_btm config: {clauq_btm_config!r}")
        elif isinstance(clauq_btm_config, dict):
            clauq_btm = ClauqBTM(**clauq_btm_config)
        else:
            raise ValueError(f"Unknown clauq_btm config: {clauq_btm_config!r}")

    gateway_config = GatewayConfig(agent_configs=agents, chat_store=chat_store, clauq_btm=clauq_btm)

    app = None
    if user_config.get("rest_api"):
        rest_api_config = user_config.get("rest_api")
        if rest_api_config.get("type") == "fast_api":
            config = rest_api_config.get("config")
            if not isinstance(config, dict):
                raise ValueError(f"Invalid rest_api config: {config!r}")
            app = load_attribute(config["app"])
            api_prefix = config.get("api_prefix", "/api")
            router_tags = config.get("router_tags", [])
            app = enrich_app_with_assistant_router(
                app=app, config=gateway_config, api_prefix=api_prefix, router_tags=router_tags
            )
        else:
            raise ValueError(f"Unknown rest_api type: {rest_api_config.get('type')!r}")

    return ParseResult(
        gateway_config=gateway_config,
        app=app,
    )


    