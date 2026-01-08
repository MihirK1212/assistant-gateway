from __future__ import annotations

from typing import Any, Dict, Optional, Type
from urllib.parse import urljoin
import httpx
from pydantic import BaseModel, Field, ValidationError, create_model
from copy import deepcopy

from assistant_gateway.errors import ToolExecutionError
from assistant_gateway.schemas import ToolResult
from assistant_gateway.tools.base import Tool, ToolContext, ToolConfig


class RESTToolConfig(ToolConfig):
    """ "
    Configuration for a REST tool. It extends the ToolConfig class, and adds a backend_url field.
    """

    backend_url: Optional[str] = Field(
        default=None,
        description="The base URL of the backend server. If not provided, the base URL will be taken during runtime from the ToolContext.",
    )


class RestToolContext(ToolContext):
    """
    Context for a REST tool. It extends the ToolContext class, and adds a base_url field.
    """

    backend_url: Optional[str] = Field(
        default=None,
        description="Override the default base URL supplied via dynamic ToolContext.input or RESTToolConfig.",
    )

    default_headers: Dict[str, str] = Field(
        default_factory=dict,
        description="Default headers to include with the request. If not provided, the default headers will be taken during runtime from the ToolContext.",
    )

    def with_input(self, payload: Dict[str, Any]) -> "RestToolContext":
        """
        Return a cloned context embedding the tool-specific input payload.

        This avoids mutating the shared context when multiple tools are called
        within the same agent turn.
        """

        data = deepcopy(self.model_dump())
        data["input"] = payload
        return RestToolContext(**data)


class _DefaultRESTQueryAndPayloadModel(BaseModel):
    """
    Default model for the query and payload parameters to be passed inside the input of the ToolContext during runtime for a REST tool.
    """

    pass


class _BaseRESTToolInput(BaseModel):
    """
    Model for the input to be passsed inside the ToolContext during runtime for a REST tool.
    """

    path: str = Field(description="Path relative to the CRUD base URL, e.g. /todos")
    method: str = Field(description="HTTP method: GET, POST, PUT, PATCH, DELETE")
    query: Optional[_DefaultRESTQueryAndPayloadModel] = Field(
        default=_DefaultRESTQueryAndPayloadModel(),
        description="Query string parameters to include with the request. Must be a Pydantic model.",
    )
    json: Optional[_DefaultRESTQueryAndPayloadModel] = Field(
        default=_DefaultRESTQueryAndPayloadModel(),
        description="JSON payload to include with the request. Must be a Pydantic model.",
    )
    data: Optional[_DefaultRESTQueryAndPayloadModel] = Field(
        default=_DefaultRESTQueryAndPayloadModel(),
        description="Form data to include with the request. Must be a Pydantic model.",
    )
    headers: Dict[str, str] = Field(default_factory=dict)
    backend_url: Optional[str] = Field(
        default=None,
        description="Override the default backend URL supplied via dynamic ToolContext.input or RESTToolConfig.",
    )


class RESTTool(Tool):
    def __init__(
        self,
        name: str,
        description: str,
        *,
        backend_url: Optional[str] = None,
        query_params_model: Optional[Type[BaseModel]] = None,
        data_payload_model: Optional[Type[BaseModel]] = None,
        json_payload_model: Optional[Type[BaseModel]] = None,
        output_model: Optional[Type[BaseModel]] = None,
    ) -> None:
        self._query_params_model = query_params_model
        self._data_payload_model = data_payload_model
        self._json_payload_model = json_payload_model
        self._output_model = output_model

        # build input model using query_params_model, data_payload_model, and json_payload_model
        self._input_model = RESTTool.build_input_model(
            name,
            query_params_model=query_params_model,
            data_payload_model=data_payload_model,
            json_payload_model=json_payload_model,
        )

        # build config using name, description, input model, output model, and backend_url
        self._config = RESTToolConfig(
            name=name,
            description=description,
            input_model=self._input_model,
            output_description=f"{RESTTool.get_output_description(output_model)}",
            output_model=output_model,
            backend_url=backend_url,
        )

        super().__init__(self._config)

    async def run(self, context: RestToolContext) -> ToolResult:
        try:
            parsed_input = self._input_model(**context.input)
        except Exception as e:
            raise ToolExecutionError(f"{self.name}: invalid input: {e}") from e

        assert isinstance(
            parsed_input, _BaseRESTToolInput
        ), f"parsed input is not a _BaseRESTToolInput: {parsed_input}"

        backend_url = (
            parsed_input.backend_url or context.backend_url or self._config.backend_url
        )
        if not backend_url:
            raise ToolExecutionError(
                f"{self.name}: missing backend_url. Provide one in ToolContext or the tool input."
            )
        backend_url = str(backend_url)
        
        url = urljoin(backend_url.rstrip("/") + "/", parsed_input.path.lstrip("/"))
        method = parsed_input.method.upper()
        headers = {
            **context.default_headers,
            **parsed_input.headers,
        }
        query_params = self.serialize_params_for_request(
            parsed_input.query, self._query_params_model
        )
        json_payload = self.serialize_params_for_request(
            parsed_input.json, self._json_payload_model
        )
        data_payload = self.serialize_params_for_request(
            parsed_input.data, self._data_payload_model
        )

        timeout = httpx.Timeout(context.timeout_seconds)
        async with httpx.AsyncClient(timeout=timeout) as client:
            try:
                response = await client.request(
                    method=method,
                    url=url,
                    params=query_params,
                    json=json_payload,
                    data=data_payload,
                    headers=headers,
                )
            except Exception as e:
                raise ToolExecutionError(f"{self.name}: HTTP error: {e}") from e

        content_type = response.headers.get("content-type", "")
        try:
            if "application/json" in content_type:
                body = response.json()
            else:
                body = response.text
        except Exception:
            body = response.text

        if response.is_error:
            raise ToolExecutionError(
                f"{self.name}: backend returned {response.status_code}: {body}"
            )

        return ToolResult(
            name=self.name,
            output=body,
            raw_response={"status_code": response.status_code},
        )

    @classmethod
    def serialize_params_for_request(
        cls,
        payload: _DefaultRESTQueryAndPayloadModel | Dict[str, Any] | None,
        payload_model: Optional[Type[BaseModel]] = None,
    ) -> Dict[str, Any]:
        if payload is None:
            return {}

        if not payload_model:
            return payload.model_dump(mode="json", exclude_none=True)

        if isinstance(payload, payload_model):
            payload_model_instance = payload
        elif isinstance(payload, dict):
            try:
                payload_model_instance = payload_model(**payload)
            except ValidationError as e:
                raise ToolExecutionError(
                    f"{cls.name}: invalid query parameters: {e}"
                ) from e
        else:
            raise ToolExecutionError(
                f"{cls.name}: query parameters must be a dict or an instance of {payload_model.__name__}"
            )

        return payload_model_instance.model_dump(mode="json", exclude_none=True)

    @classmethod
    def build_input_model(
        cls,
        tool_name: str,
        *,
        query_params_model: Optional[Type[BaseModel]] = None,
        data_payload_model: Optional[Type[BaseModel]] = None,
        json_payload_model: Optional[Type[BaseModel]] = None,
    ) -> Type[BaseModel]:
        sanitized_name = "".join(ch if ch.isalnum() else "_" for ch in tool_name)
        class_name = f"{cls.__name__}Input_{sanitized_name}"
        return create_model(
            class_name,
            __base__=_BaseRESTToolInput,
            **(
                {
                    "query": (
                        query_params_model,
                        Field(
                            default=None,
                            description="Query parameters validated by the tool-specific model.",
                        ),
                    )
                }
                if query_params_model
                else {}
            ),
            **(
                {
                    "json": (
                        json_payload_model,
                        Field(
                            default=None,
                            description="JSON payload validated by the tool-specific model.",
                        ),
                    )
                }
                if json_payload_model
                else {}
            ),
            **(
                {
                    "data": (
                        data_payload_model,
                        Field(
                            default=None,
                            description="Form data validated by the tool-specific model.",
                        ),
                    )
                }
                if data_payload_model
                else {}
            ),
        )

    @classmethod
    def get_output_description(cls, output_model: Optional[Type[BaseModel]]) -> str:
        if output_model is None:
            return "Arbitrary JSON response from the CRUD backend."
        desc = f"Response validated by the tool-specific model: {output_model.__name__}"
        if hasattr(output_model, "model_fields") and output_model.model_fields:
            field_descriptions = []
            for name, field in output_model.model_fields.items():
                field_info = field.description or ""
                field_str = f"{name}: {field_info}" if field_info else name
                field_descriptions.append(field_str)
            if field_descriptions:
                desc += f". Fields: {', '.join(field_descriptions)}"
        return desc
