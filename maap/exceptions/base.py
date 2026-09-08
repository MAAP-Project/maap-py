from typing import Any

from pydantic import BaseModel, Field, model_validator
from requests import Response


class APIErrorResponse(BaseModel):
    type: str = ""
    title: str = ""
    status: int = 0
    detail: str = ""
    instance: str = ""
    additional_properties: dict[str, Any] = Field(
        default_factory=dict, alias="additionalProperties"
    )

    model_config = {"populate_by_name": True}

    @model_validator(mode="before")
    @classmethod
    def _normalize_members_error(cls, data: Any) -> Any:
        """Map the members API's {code, message} error shape onto the OGC error fields."""
        if isinstance(data, dict) and "message" in data and "detail" not in data:
            return {**data, "detail": data["message"], "status": data.get("code", 0)}
        return data


class MAAPError(Exception):
    pass


class APIError(MAAPError):
    def __init__(self, response: Response) -> None:
        self.status_code = response.status_code
        self.response = response
        try:
            self.error = APIErrorResponse.model_validate(response.json())
        except (ValueError, Exception):
            self.error = APIErrorResponse(status=self.status_code, detail=response.text)
        super().__init__(f"[{self.status_code}] {self.error.detail}")


class AuthenticationError(APIError):
    pass


class ForbiddenError(APIError):
    pass


class NotFoundError(APIError):
    pass
