class GatewayError(Exception):
	"""Base error for the Assistant Gateway."""


class ToolExecutionError(GatewayError):
	"""Raised when a tool fails to execute."""


class AgentError(GatewayError):
	"""Raised when an agent fails to generate or run."""


