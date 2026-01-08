from __future__ import annotations

from typing import Dict, Iterable, Optional

from assistant_gateway.tools.base import Tool


class ToolRegistry:
    def __init__(self) -> None:
        self._tools: Dict[str, Tool] = {}

    def register(self, tool: Tool) -> None:
        assert isinstance(tool, Tool), "Tool must be an instance of Tool"
        assert tool.name not in self._tools, f"Tool {tool.name} already registered"
        self._tools[tool.name] = tool

    def get(self, name: str) -> Optional[Tool]:
        return self._tools.get(name)

    def all(self) -> Iterable[Tool]:
        return self._tools.values()
