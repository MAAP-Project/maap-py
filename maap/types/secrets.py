from pydantic import BaseModel, Field


class SecretSummary(BaseModel):
    """A secret as returned in a secret listing, or when a secret is added.

    Only the secret's name is included; the value is never returned by these
    endpoints. Use ``maap.secrets.get()`` to retrieve a secret's value.
    """

    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    name: str = Field(alias="secret_name")
    """The name of the secret."""


class Secret(BaseModel):
    """A secret and its decrypted value, as returned when retrieving a single secret."""

    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    name: str = Field(alias="secret_name")
    """The name of the secret."""
    value: str = Field(alias="secret_value")
    """The decrypted value of the secret."""
