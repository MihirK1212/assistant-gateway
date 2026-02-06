import os
import sys

CURRENT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.dirname(os.path.dirname(CURRENT_DIR)))
sys.path.append(os.path.dirname(CURRENT_DIR))
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR)))
)  # repo root
sys.path.append(
    os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(CURRENT_DIR))))
)  # repo root

from assistant_gateway.examples.calculator_web_app.calculator_chat_gateway.config.base import (  # noqa: E402
    build_gateway_config,
)

config = build_gateway_config()
