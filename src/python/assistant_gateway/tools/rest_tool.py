from __future__ import annotations

from typing import Any, Dict, Optional, Type
from urllib.parse import urljoin
import httpx
from pydantic import BaseModel, Field, ValidationError, create_model, model_validator
from copy import deepcopy

from assistant_gateway.errors import ToolExecutionError
from assistant_gateway.schemas import ToolResult
from assistant_gateway.tools.base import Tool, ToolContext, ToolConfig


class DefaultRESTQueryAndPayloadModel(BaseModel):
    """
    Default model for the query and payload parameters to be passed inside the input of the ToolContext during runtime for a REST tool.
    """

    pass


class BaseRESTToolInput(BaseModel):
    """
    Model for the input to be passsed inside the ToolContext during runtime for a REST tool.
    """

    path: str = Field(description="Path relative to the CRUD base URL, e.g. /todos")
    method: str = Field(description="HTTP method: GET, POST, PUT, PATCH, DELETE")
    query: Optional[DefaultRESTQueryAndPayloadModel] = Field(
        default=DefaultRESTQueryAndPayloadModel(),
        description="Query string parameters to include with the request. Must be a Pydantic model.",
    )
    json: Optional[DefaultRESTQueryAndPayloadModel] = Field(
        default=DefaultRESTQueryAndPayloadModel(),
        description="JSON payload to include with the request. Must be a Pydantic model.",
    )
    data: Optional[DefaultRESTQueryAndPayloadModel] = Field(
        default=DefaultRESTQueryAndPayloadModel(),
        description="Form data to include with the request. Must be a Pydantic model.",
    )
    headers: Dict[str, str] = Field(default_factory=dict)
    backend_url: Optional[str] = Field(
        default=None,
        description="The backend URL supplied inside the ToolContext input",
    )


class RESTTool(Tool):
    def __init__(
        self,
        name: str,
        *,
        description: str,
        query_params_model: Optional[Type[BaseModel]] = None,
        data_payload_model: Optional[Type[BaseModel]] = None,
        json_payload_model: Optional[Type[BaseModel]] = None,
        output_model: Optional[Type[BaseModel]] = None,
        timeout_seconds: int = 30,
        tool_level_input_overrides: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._tool_level_input_overrides = tool_level_input_overrides
        self._timeout_seconds = timeout_seconds
        self._query_params_model = query_params_model
        self._data_payload_model = data_payload_model
        self._json_payload_model = json_payload_model
        self._output_model = output_model

        # build input model using query_params_model, data_payload_model, and json_payload_model
        input_model = RESTTool.build_input_model(
            name,
            query_params_model=query_params_model,
            data_payload_model=data_payload_model,
            json_payload_model=json_payload_model,
        )
        self._input_model = input_model

        # build config using name, description, input model, output model, and backend_url
        self._config = ToolConfig(
            name=name,
            description=description,
            input_model=input_model,
            input_description=f"{RESTTool.get_field_description_from_model(input_model)}",
            output_model=output_model,
            output_description=f"{RESTTool.get_field_description_from_model(output_model)}",
            timeout_seconds=timeout_seconds,
            tool_level_input_overrides=tool_level_input_overrides,
        )

        super().__init__(self._config)

    async def run(self, context: ToolContext) -> ToolResult:
        try:
            parsed_input = self._input_model(**context.input)
        except Exception as e:
            raise ToolExecutionError(f"{self.name}: invalid input: {e}") from e

        assert isinstance(
            parsed_input, BaseRESTToolInput
        ), f"parsed input is not a BaseRESTToolInput: {parsed_input}"

        backend_url = parsed_input.backend_url
        if not backend_url:
            raise ToolExecutionError(
                f"{self.name}: missing backend_url. Provide one in ToolContext or the tool input."
            )
        backend_url = str(backend_url)

        url = urljoin(backend_url.rstrip("/") + "/", parsed_input.path.lstrip("/"))
        method = parsed_input.method.upper()
        headers = parsed_input.headers
        query_params = self.serialize_params_for_request(
            parsed_input.query, self._query_params_model
        )
        json_payload = self.serialize_params_for_request(
            parsed_input.json, self._json_payload_model
        )
        data_payload = self.serialize_params_for_request(
            parsed_input.data, self._data_payload_model
        )

        timeout = httpx.Timeout(self._config.timeout_seconds)
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
        payload: DefaultRESTQueryAndPayloadModel | Dict[str, Any] | None,
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
            __base__=BaseRESTToolInput,
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
    def get_field_description_from_model(cls, model: Optional[Type[BaseModel]]) -> str:
        if model is None:
            return "Arbitrary JSON response from the CRUD backend."
        desc = f"Model description:"
        if hasattr(model, "model_fields") and model.model_fields:
            field_descriptions = []
            for name, field in model.model_fields.items():
                field_info = field.description or ""
                field_str = f"{name}: {field_info}" if field_info else name
                field_descriptions.append(field_str)
            if field_descriptions:
                desc += f". Fields: {', '.join(field_descriptions)}"
        return desc
