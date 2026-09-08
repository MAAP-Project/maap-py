from __future__ import annotations

import inspect
import re
import textwrap
import typing
from collections.abc import Callable
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from maap.client import MAAP
    from maap.services.base import BaseService

# Help text is rendered to a fixed width rather than the terminal's, so that the
# output is the same in a notebook, a terminal, and a test.
_WIDTH = 100
_INDENT = "  "


class FunctionHelp:
    """The full documentation for a single function.

    Renders whether or not it is called, so ``maap.help.jobs.submit`` and
    ``maap.help.jobs.submit()`` show the same thing.
    """

    def __init__(self, area: str, name: str, function: Callable[..., Any]) -> None:
        self.name = name
        """The name of the function."""
        self.signatures = _signatures(f"maap.{area}.{name}", function)
        """The function's calling conventions, one per overload."""
        self.documentation = inspect.getdoc(function) or ""
        """The function's full docstring."""

    def __call__(self) -> FunctionHelp:
        return self

    def __repr__(self) -> str:
        return "\n".join([*self.signatures, "", self.documentation])

    __str__ = __repr__


class ServiceHelp:
    """A listing of the functions a service area supports.

    The listing renders whether or not it is called, so ``maap.help.jobs`` and
    ``maap.help.jobs()`` show the same thing.
    """

    def __init__(self, service: BaseService) -> None:
        cls = type(service)
        self.name = _area_name(cls)
        """The name the service area is exposed under (e.g. jobs)."""
        self.description = _summary(cls.__doc__)
        """A one-line description of the service area."""
        self.functions = _public_functions(cls)
        """The area's functions, in definition order, mapped to their summaries."""
        self._function_help = {
            name: FunctionHelp(self.name, name, member)
            for name, member in vars(cls).items()
            if name in self.functions
        }

    def __call__(self) -> ServiceHelp:
        return self

    def __repr__(self) -> str:
        lines = [f"maap.{self.name} - {self.description}", ""]
        lines += _rows(self.functions)
        if self.functions:
            example = next(iter(self.functions))
            lines += [
                "",
                f"maap.help.{self.name}.<function> shows a function's full documentation, "
                f"e.g. maap.help.{self.name}.{example}",
            ]
        return "\n".join(lines)

    __str__ = __repr__

    def __getattr__(self, name: str) -> FunctionHelp:
        # Only consulted when normal lookup fails, and reads __dict__ directly so that
        # an attribute accessed before __init__ finishes cannot recurse.
        function_help: dict[str, FunctionHelp] = self.__dict__.get("_function_help", {})
        if name in function_help:
            return function_help[name]
        area = self.__dict__.get("name", "?")
        raise AttributeError(
            f"maap.{area} has no function {name!r}. "
            f"Available: {', '.join(function_help) or '(none)'}"
        )

    def __dir__(self) -> list[str]:
        # Puts the function names in front of tab completion.
        return [*super().__dir__(), *self.functions]


class Help:
    """A listing of the service areas the MAAP client supports.

    The listing renders whether or not it is called, so ``maap.help`` and
    ``maap.help()`` show the same thing.
    """

    def __init__(self, client: MAAP) -> None:
        self.jobs = client.jobs.help()
        self.algorithms = client.algorithms.help()
        self.secrets = client.secrets.help()
        self.user = client.user.help()

    @property
    def areas(self) -> dict[str, ServiceHelp]:
        """The service areas, mapped to their help listings."""
        listings = (self.jobs, self.algorithms, self.secrets, self.user)
        return {listing.name: listing for listing in listings}

    def __call__(self) -> Help:
        return self

    def __repr__(self) -> str:
        lines = ["MAAP service areas", ""]
        lines += _rows({name: area.description for name, area in self.areas.items()})
        lines += ["", "maap.help.<area>() lists an area's functions, e.g. maap.help.jobs()"]
        return "\n".join(lines)

    __str__ = __repr__


def _area_name(cls: type[BaseService]) -> str:
    """Derive the client attribute a service is exposed under (JobsService -> jobs)."""
    return cls.__name__.removesuffix("Service").lower()


def _summary(doc: str | None) -> str:
    """Take the first line of a docstring as its summary."""
    if not doc:
        return ""
    return inspect.cleandoc(doc).split("\n", 1)[0].strip()


def _public_functions(cls: type[BaseService]) -> dict[str, str]:
    """Map a service's own public methods to their summaries, in definition order.

    Only the class's own namespace is consulted, which preserves the order the
    methods are defined in and leaves inherited helpers such as help() out of the
    listing.
    """
    return {
        name: _summary(inspect.getdoc(member))
        for name, member in vars(cls).items()
        if not name.startswith("_") and inspect.isfunction(member)
    }


def _rows(entries: dict[str, Any]) -> list[str]:
    """Render name/summary pairs as an aligned two-column block."""
    if not entries:
        return [f"{_INDENT}(none)"]

    column = max(len(name) for name in entries)
    lines: list[str] = []
    for name, summary in entries.items():
        prefix = f"{_INDENT}{name.ljust(column)}  "
        wrapped = textwrap.wrap(
            f"{prefix}{summary}", width=_WIDTH, subsequent_indent=" " * len(prefix)
        )
        lines += wrapped or [prefix.rstrip()]
    return lines


def _signatures(qualified_name: str, function: Callable[..., Any]) -> list[str]:
    """Render a function's calling conventions, one line per @overload.

    Overloads are rendered in preference to the implementation signature, which for
    a multi-form method reports every argument as independently optional and so
    misstates how the function may actually be called.
    """
    overloads = typing.get_overloads(function) or [function]
    return [f"{qualified_name}{_signature(overload)}" for overload in overloads]


def _signature(function: Callable[..., Any]) -> str:
    signature = inspect.signature(function)
    without_self = [p for p in signature.parameters.values() if p.name != "self"]
    return _shorten_annotations(str(signature.replace(parameters=without_self)))


def _shorten_annotations(signature: str) -> str:
    """Drop module paths from annotations (maap.types.jobs.JobRequest -> JobRequest).

    This is presentation only, applied to an already-rendered signature.
    """
    return re.sub(r"\b(?:[A-Za-z_]\w*\.)+(\w)", r"\1", signature)
