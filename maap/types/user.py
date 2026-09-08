from pydantic import BaseModel


class Organization(BaseModel):
    """An organization the user is a member of."""

    model_config = {"use_attribute_docstrings": True}

    id: int
    """Unique numeric identifier of the organization."""
    name: str
    """Human-readable name of the organization."""


class UserProfile(BaseModel):
    """The profile of the authenticated user, as returned by ``maap.user.get_profile()``."""

    model_config = {"use_attribute_docstrings": True}

    id: int
    """Unique numeric identifier of the user."""
    username: str
    """The user's MAAP username."""
    first_name: str
    """The user's first name."""
    last_name: str
    """The user's last name."""
    email: str
    """The email address associated with the user's account."""
    status: str
    """Status of the user's account (e.g. active, suspended)."""
    creation_date: str
    """Timestamp of when the account was created."""
    organizations: list[Organization] = []
    """The organizations the user belongs to."""
    public_ssh_key: str | None = None
    """The user's registered public SSH key, if one is set."""
    public_ssh_key_name: str | None = None
    """The filename the public SSH key was registered under (e.g. id_rsa.pub)."""
    public_ssh_key_modified_date: str | None = None
    """Timestamp of when the public SSH key was last changed."""
    session_key: str = ""
    """The user's session key. Empty unless the account has an active session."""
    urs_token: str = ""
    """The user's Earthdata Login (URS) token. Empty unless one has been issued."""


class QueueList(BaseModel):
    """Response model for a job queue listing."""

    model_config = {"use_attribute_docstrings": True}

    queues: list[str] = []
    """The names of the worker queues available to the user."""
