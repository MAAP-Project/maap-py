import pytest
import responses

from maap import MAAP
from maap.help import FunctionHelp, Help, ServiceHelp

AREAS = ["jobs", "algorithms", "secrets", "user"]

JOBS_FUNCTIONS = ["submit", "list", "get", "get_metrics", "get_results", "cancel"]


def test_help_lists_every_service_area(client: MAAP) -> None:
    listing = repr(client.help())

    assert listing.startswith("MAAP service areas")
    for area in AREAS:
        assert f"  {area}" in listing


def test_help_shows_area_descriptions(client: MAAP) -> None:
    listing = repr(client.help())

    assert "Submit, monitor, and cancel DPS jobs." in listing
    assert "The current user's profile and available job queues." in listing


def test_help_renders_whether_or_not_it_is_called(client: MAAP) -> None:
    assert repr(client.help) == repr(client.help())


def test_help_is_printable(client: MAAP) -> None:
    assert str(client.help()) == repr(client.help())


def test_help_areas_are_addressable(client: MAAP) -> None:
    assert isinstance(client.help, Help)
    assert sorted(client.help.areas) == sorted(AREAS)
    assert all(isinstance(area, ServiceHelp) for area in client.help.areas.values())


def test_service_help_lists_functions_in_definition_order(client: MAAP) -> None:
    assert list(client.help.jobs.functions) == JOBS_FUNCTIONS


def test_service_help_shows_function_summaries(client: MAAP) -> None:
    listing = repr(client.help.jobs)

    assert listing.startswith("maap.jobs - Submit, monitor, and cancel DPS jobs.")
    for function in JOBS_FUNCTIONS:
        assert f"  {function}" in listing
    assert "Submit one or more jobs for execution." in listing


def test_service_help_renders_whether_or_not_it_is_called(client: MAAP) -> None:
    assert repr(client.help.jobs) == repr(client.help.jobs())


def test_service_help_excludes_help_itself(client: MAAP) -> None:
    # help() is inherited from BaseService, so it is not one of the area's own functions.
    assert "help" not in client.help.jobs.functions


def test_service_help_excludes_private_methods(client: MAAP) -> None:
    assert not any(name.startswith("_") for name in client.help.algorithms.functions)
    assert "_resolve_id" not in repr(client.help.algorithms)


def test_service_help_lists_overloaded_methods_once(client: MAAP) -> None:
    # algorithms.get is declared with two @overload stubs plus an implementation.
    assert list(client.help.algorithms.functions).count("get") == 1


def test_service_help_is_reachable_from_the_service(client: MAAP) -> None:
    assert repr(client.jobs.help()) == repr(client.help.jobs)
    assert repr(client.user.help()) == repr(client.help.user)


def test_service_help_wraps_long_summaries_with_a_hanging_indent(client: MAAP) -> None:
    lines = repr(client.help.algorithms).split("\n")
    wrapped = [line for line in lines if line.startswith(" " * 15) and "deployer." in line]

    # get_package's summary is too long for one line and continues under the summary column.
    assert wrapped
    assert all(len(line) <= 100 for line in lines)


def test_every_service_area_has_a_description(client: MAAP) -> None:
    assert all(area.description for area in client.help.areas.values())


def test_every_function_has_a_summary(client: MAAP) -> None:
    for area in client.help.areas.values():
        assert area.functions
        assert all(area.functions.values()), area.name


def test_function_help_is_reachable_by_attribute(client: MAAP) -> None:
    assert isinstance(client.help.jobs.submit, FunctionHelp)
    assert client.help.jobs.submit.name == "submit"


def test_function_help_renders_whether_or_not_it_is_called(client: MAAP) -> None:
    assert repr(client.help.jobs.submit) == repr(client.help.jobs.submit())


def test_function_help_shows_the_full_docstring(client: MAAP) -> None:
    listing = repr(client.help.jobs.submit)

    assert "Submit one or more jobs for execution." in listing
    assert "Args:" in listing
    assert "Returns:" in listing
    assert "Examples:" in listing


def test_function_help_shows_every_overload(client: MAAP) -> None:
    # algorithms.get takes either an ID or all of name/version/deployer, never a mix.
    assert client.help.algorithms.get.signatures == [
        "maap.algorithms.get(*, algorithm_id: str | int) -> Algorithm",
        "maap.algorithms.get(*, name: str, version: str, deployer: str) -> Algorithm",
    ]


def test_function_help_shows_one_signature_when_not_overloaded(client: MAAP) -> None:
    assert client.help.user.get_queues.signatures == ["maap.user.get_queues() -> list[str]"]


def test_function_help_omits_self_and_module_paths(client: MAAP) -> None:
    signature = client.help.jobs.get.signatures[0]

    assert "self" not in signature
    assert "maap.types" not in signature
    assert signature == (
        "maap.jobs.get(*, job_id: str, get_job_details: Literal[False] = False) -> JobStatus"
    )


def test_function_help_rejects_unknown_functions(client: MAAP) -> None:
    with pytest.raises(AttributeError) as exc_info:
        client.help.jobs.nope  # noqa: B018

    assert "maap.jobs has no function 'nope'" in str(exc_info.value)
    assert "submit" in str(exc_info.value)


def test_function_help_is_offered_for_tab_completion(client: MAAP) -> None:
    listed = dir(client.help.jobs)

    assert all(function in listed for function in JOBS_FUNCTIONS)


def test_service_help_points_at_the_function_level(client: MAAP) -> None:
    assert "maap.help.jobs.submit" in repr(client.help.jobs)


def test_help_makes_no_requests(mock_api: responses.RequestsMock, client: MAAP) -> None:
    repr(client.help())
    repr(client.help.jobs())

    assert len(mock_api.calls) == 0
