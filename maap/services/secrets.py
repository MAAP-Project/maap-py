import builtins
from collections.abc import Mapping, Sequence
from typing import overload
from urllib.parse import quote

from maap.exceptions import (
    APIError,
    NotFoundError,
    SecretAlreadyExistsError,
    SecretNotFoundError,
)
from maap.services.base import BaseService
from maap.types.secrets import Secret, SecretSummary


class SecretsService(BaseService):
    """Store and retrieve the current user's secrets."""

    _NAME_KEY = "name"
    _VALUE_KEY = "value"

    # The list() method below shadows the `list` builtin inside this class body, so
    # annotations that need the builtin qualify it as builtins.list.

    def list(self) -> list[SecretSummary]:
        """Retrieve the names of the current user's secrets.

        Secret values are not included in the listing. Use get() to retrieve the
        value of an individual secret.

        Returns:
            A list of SecretSummary objects, one per secret, ordered by name.

        Examples:
            >>> maap.secrets.list()
            >>> for secret in maap.secrets.list():
            ...     print(secret.name)
        """
        data = self._transport.get("/api/members/self/secrets")
        return [SecretSummary.model_validate(item) for item in data]

    def get(self, *, name: str) -> Secret:
        """Retrieve a single secret, including its decrypted value.

        The returned value is the secret in plain text, so avoid printing or
        logging it.

        Args:
            name: The name of the secret to retrieve.

        Returns:
            A Secret object holding the secret's name and decrypted value.

        Raises:
            ValueError: If name is empty.
            SecretNotFoundError: If the user has no secret with the given name.

        Examples:
            >>> secret = maap.secrets.get(name="my_api_key")
            >>> secret.name
            'my_api_key'
            >>> os.environ["MY_API_KEY"] = secret.value
        """
        self._validate_name(name)

        try:
            data = self._transport.get(f"/api/members/self/secrets/{quote(name, safe='')}")
        except NotFoundError as e:
            raise SecretNotFoundError(name, e.error) from None
        return Secret.model_validate(data)

    def has(self, *, name: str) -> bool:
        """Check whether the current user has a secret with the given name.

        This is answered from the secret listing, so the secret's value is never
        retrieved or decrypted.

        Args:
            name: The name of the secret to look for.

        Returns:
            True if the user has a secret with this name, False otherwise.

        Raises:
            ValueError: If name is empty.

        Examples:
            >>> maap.secrets.has(name="my_api_key")
            True
            >>> if not maap.secrets.has(name="MAAP_PGT"):
            ...     maap.secrets.add(name="MAAP_PGT", value=token)
        """
        self._validate_name(name)
        return any(secret.name == name for secret in self.list())

    @overload
    def add(self, secrets: Sequence[Mapping[str, str]]) -> builtins.list[SecretSummary]: ...
    @overload
    def add(self, *, name: str, value: str) -> SecretSummary: ...

    def add(
        self,
        secrets: Sequence[Mapping[str, str]] | None = None,
        *,
        name: str | None = None,
        value: str | None = None,
    ) -> SecretSummary | builtins.list[SecretSummary]:
        """Add one or more secrets for the current user.

        Provide either a single name and value, or a sequence of
        {"name": ..., "value": ...} mappings to add as a batch.

        Secrets are immutable: to change a secret's value, delete it and add it
        again under the same name.

        The API has no batch endpoint, so a batch is added one secret at a time,
        in order. Every entry is validated before the first request is sent, but
        if the API rejects an entry (for example because a secret with that name
        already exists), the secrets added ahead of it remain added.

        Args:
            secrets: A sequence of mappings to add as a batch, each with exactly
                a "name" and a "value" key.
            name: The name to store a single secret under.
            value: The value of the single secret to store.

        Returns:
            A SecretSummary object if name and value were provided, or a list of
            SecretSummary objects (in the same order as the requests) if a sequence
            was provided.

        Raises:
            ValueError: If neither or both of secrets and name/value are provided.
            ValueError: If an entry in secrets is not a mapping with exactly the
                keys "name" and "value", or if either is not a string.
            ValueError: If any name or value is empty.
            ValueError: If secrets contains the same name more than once.
            SecretAlreadyExistsError: If a secret already exists with a given name.

        Examples:
            >>> maap.secrets.add(name="my_api_key", value="s3cr3t")

            >>> added = maap.secrets.add([
            ...     {"name": "my_api_key", "value": "s3cr3t"},
            ...     {"name": "MAAP_PGT", "value": open("token.txt").read()},
            ... ])
            >>> [secret.name for secret in added]
            ['my_api_key', 'MAAP_PGT']
        """
        if secrets is not None:
            if name is not None or value is not None:
                raise ValueError("Provide either secrets or name and value, not both")
            return self._add_many(secrets)
        if name is None or value is None:
            raise ValueError("Provide either secrets or both name and value")

        self._validate_pair(name, value)
        return self._add_one(name, value)

    def delete(self, *, name: str) -> None:
        """Delete one of the current user's secrets.

        Args:
            name: The name of the secret to delete.

        Raises:
            ValueError: If name is empty.
            SecretNotFoundError: If the user has no secret with the given name.

        Examples:
            >>> maap.secrets.delete(name="my_api_key")
        """
        self._validate_name(name)

        try:
            self._transport.delete(f"/api/members/self/secrets/{quote(name, safe='')}")
        except NotFoundError as e:
            raise SecretNotFoundError(name, e.error) from None
        except APIError as e:
            if self._reports_missing_secret(e):
                raise SecretNotFoundError(name, e.error) from None
            raise

    def _add_many(self, secrets: Sequence[Mapping[str, str]]) -> builtins.list[SecretSummary]:
        pairs = self._parse_entries(secrets)
        return [self._add_one(name, value) for name, value in pairs]

    @classmethod
    def _parse_entries(cls, secrets: Sequence[Mapping[str, str]]) -> builtins.list[tuple[str, str]]:
        """Validate every entry up front so a bad batch adds nothing at all."""
        pairs: builtins.list[tuple[str, str]] = []
        seen: set[str] = set()

        for index, entry in enumerate(secrets):
            where = f"secrets[{index}] "
            if not isinstance(entry, Mapping):
                raise ValueError(f"{where}must be a mapping, got {type(entry).__name__}")
            if set(entry) != {cls._NAME_KEY, cls._VALUE_KEY}:
                raise ValueError(
                    f'{where}must have exactly the keys "{cls._NAME_KEY}" and '
                    f'"{cls._VALUE_KEY}", got {sorted(entry)}'
                )

            name, value = entry[cls._NAME_KEY], entry[cls._VALUE_KEY]
            if not isinstance(name, str) or not isinstance(value, str):
                raise ValueError(
                    f'{where}"{cls._NAME_KEY}" and "{cls._VALUE_KEY}" must both be strings'
                )

            cls._validate_pair(name, value, where)
            if name in seen:
                raise ValueError(f"{where}repeats an earlier secret name: {name}")
            seen.add(name)
            pairs.append((name, value))

        return pairs

    def _add_one(self, name: str, value: str) -> SecretSummary:
        try:
            data = self._transport.post(
                "/api/members/self/secrets",
                json={"secret_name": name, "secret_value": value},
            )
        except APIError as e:
            if self._reports_existing_secret(e):
                raise SecretAlreadyExistsError(name, e.error) from None
            raise
        return SecretSummary.model_validate(data)

    @staticmethod
    def _validate_name(name: str) -> None:
        if not name:
            raise ValueError("name must not be empty")

    @staticmethod
    def _validate_pair(name: str, value: str, where: str = "") -> None:
        if not name:
            raise ValueError(f"{where}name must not be empty")
        if not value:
            raise ValueError(f"{where}value must not be empty")

    @staticmethod
    def _reports_missing_secret(error: APIError) -> bool:
        # The members API reports a missing secret on delete as a 400, not a 404.
        return error.status_code == 400 and error.error.detail.startswith("No secret exists")

    @staticmethod
    def _reports_existing_secret(error: APIError) -> bool:
        # The members API reports a duplicate secret name as a 400.
        return error.status_code == 400 and error.error.detail.startswith("Secret already exists")
