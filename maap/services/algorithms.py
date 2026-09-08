from typing import Any, overload

from maap.exceptions import (
    AlgorithmNotFoundError,
    AlgorithmPermissionError,
    APIError,
    NotFoundError,
)
from maap.exceptions.base import ForbiddenError
from maap.services.base import BaseService
from maap.types.algorithms import (
    Algorithm,
    AlgorithmDeployment,
    AlgorithmList,
    AlgorithmPackage,
)


class AlgorithmsService(BaseService):
    """Deploy, inspect, and delete registered algorithms."""

    @overload
    def get(self, *, algorithm_id: str | int) -> Algorithm: ...
    @overload
    def get(self, *, name: str, version: str, deployer: str) -> Algorithm: ...

    def get(
        self,
        *,
        algorithm_id: str | int | None = None,
        name: str | None = None,
        version: str | None = None,
        deployer: str | None = None,
    ) -> Algorithm:
        """Retrieve a single algorithm by its ID or by name, version, and deployer.

        Every registered algorithm has a unique ID and may be retrieved through the ID
        alone. Alternatively, an algorithm's name, version, and deployer map to a single
        algorithm so an algorithm may be retrieved using these three fields if the ID
        is not known.

        Args:
            algorithm_id: The numeric ID of the algorithm (string or int).
            name: The algorithm name (requires version and deployer).
            version: The algorithm version (requires name and deployer).
            deployer: The deployer username (requires name and version).

        Returns:
            An Algorithm object.

        Raises:
            ValueError: If neither algorithm_id nor all of name/version/deployer are provided.
            ValueError: If algorithm_id is not greater than 0.
            AlgorithmNotFoundError: If no algorithm exists matching the criteria.

        Examples:
            >>> maap.algorithms.get(algorithm_id=96)
            >>> maap.algorithms.get(algorithm_id="96")
            >>> maap.algorithms.get(name="dps_tutorial", version="1.0", deployer="mlucas")
        """
        resolved_id = self._resolve_target_id(algorithm_id, name, version, deployer)

        try:
            data = self._transport.get(f"/api/ogc/processes/{resolved_id}", authenticated=False)
        except NotFoundError as e:
            raise AlgorithmNotFoundError(resolved_id, e.error) from None
        return Algorithm.model_validate(data)

    @overload
    def get_package(self, *, algorithm_id: str | int) -> AlgorithmPackage: ...
    @overload
    def get_package(self, *, name: str, version: str, deployer: str) -> AlgorithmPackage: ...

    def get_package(
        self,
        *,
        algorithm_id: str | int | None = None,
        name: str | None = None,
        version: str | None = None,
        deployer: str | None = None,
    ) -> AlgorithmPackage:
        """Retrieve an algorithm's application package by its ID or by name, version, and deployer.

        The object returned contains the name of the process/algorithm and the link to the
        execution unit (CWL).

        Args:
            algorithm_id: The numeric ID of the algorithm (string or int).
            name: The algorithm name (requires version and deployer).
            version: The algorithm version (requires name and deployer).
            deployer: The deployer username (requires name and version).

        Returns:
            An AlgorithmPackage object containing the process description and execution unit.

        Raises:
            ValueError: If neither algorithm_id nor all of name/version/deployer are provided.
            ValueError: If algorithm_id is not greater than 0.
            AlgorithmNotFoundError: If no algorithm exists matching the criteria.

        Examples:
            >>> maap.algorithms.get_package(algorithm_id=10)
            >>> maap.algorithms.get_package(
            ...     name="dps_tutorial", version="1.0", deployer="mlucas"
            ... )
        """
        resolved_id = self._resolve_target_id(algorithm_id, name, version, deployer)

        try:
            data = self._transport.get(
                f"/api/ogc/processes/{resolved_id}/package", authenticated=False
            )
        except NotFoundError as e:
            raise AlgorithmNotFoundError(resolved_id, e.error) from None
        return AlgorithmPackage.model_validate(data)

    def list(
        self,
        *,
        deployer: str | None = None,
        name: str | None = None,
        version: str | None = None,
    ) -> list[Algorithm]:
        """Retrieve available algorithms, optionally filtered.

        Args:
            deployer: Only include algorithms deployed by this user.
            name: Only include algorithms with this name.
            version: Only include algorithms with this version.

        Returns:
            A list of Algorithm objects. If no filters are provided, all algorithms
            are returned.

        Examples:
            >>> maap.algorithms.list()
            >>> maap.algorithms.list(deployer="mlucas")
            >>> maap.algorithms.list(name="gedi-subset", version="0.13.0")
            >>> for algo in maap.algorithms.list():
            ...     print(algo.title, algo.version)
        """
        params = {
            "deployer": deployer,
            "algorithmName": name,
            "algorithmVersion": version,
        }
        params = {key: value for key, value in params.items() if value is not None}

        data = self._transport.get("/api/ogc/processes", params=params or None, authenticated=False)
        result = AlgorithmList.model_validate(data)
        return result.processes

    @overload
    def delete(self, *, algorithm_id: str | int) -> None: ...
    @overload
    def delete(self, *, name: str, version: str, deployer: str) -> None: ...

    def delete(
        self,
        *,
        algorithm_id: str | int | None = None,
        name: str | None = None,
        version: str | None = None,
        deployer: str | None = None,
    ) -> None:
        """Delete an algorithm by its ID or by name, version, and deployer.

        Users may only delete algorithms they have registered. If attempting
        to delete an algorithm a user does not own, an error will be raised.

        Args:
            algorithm_id: The numeric ID of the algorithm (string or int).
            name: The algorithm name (requires version and deployer).
            version: The algorithm version (requires name and deployer).
            deployer: The deployer username (requires name and version).

        Raises:
            ValueError: If neither algorithm_id nor all of name/version/deployer are provided.
            ValueError: If algorithm_id is not greater than 0.
            AlgorithmNotFoundError: If no algorithm exists matching the criteria.
            AlgorithmPermissionError: If you are not the original deployer of the algorithm.

        Examples:
            >>> maap.algorithms.delete(algorithm_id=96)
            >>> maap.algorithms.delete(name="operawatermask1", version="ogc", deployer="mlucas")
        """
        resolved_id = self._resolve_target_id(algorithm_id, name, version, deployer)

        try:
            self._transport.delete(f"/api/ogc/processes/{resolved_id}")
        except NotFoundError as e:
            raise AlgorithmNotFoundError(resolved_id, e.error) from None
        except ForbiddenError as e:
            raise AlgorithmPermissionError(resolved_id, e.error) from None

    @overload
    def put(self, *, algorithm_id: str | int, cwl_href: str) -> AlgorithmDeployment: ...
    @overload
    def put(self, *, algorithm_id: str | int, cwl_raw_text: str) -> AlgorithmDeployment: ...
    @overload
    def put(
        self, *, name: str, version: str, deployer: str, cwl_href: str
    ) -> AlgorithmDeployment: ...
    @overload
    def put(
        self, *, name: str, version: str, deployer: str, cwl_raw_text: str
    ) -> AlgorithmDeployment: ...

    def put(
        self,
        *,
        algorithm_id: str | int | None = None,
        name: str | None = None,
        version: str | None = None,
        deployer: str | None = None,
        cwl_href: str | None = None,
        cwl_raw_text: str | None = None,
    ) -> AlgorithmDeployment:
        """Replace an algorithm by its ID or by name, version, and deployer.

        The replacement definition is provided either as a link to a CWL workflow
        (cwl_href) or as the raw contents of a CWL file (cwl_raw_text). Provide
        exactly one of the two.

        Args:
            algorithm_id: The numeric ID of the algorithm (string or int).
            name: The algorithm name (requires version and deployer).
            version: The algorithm version (requires name and deployer).
            deployer: The deployer username (requires name and version).
            cwl_href: A URL pointing to the CWL workflow to deploy.
            cwl_raw_text: The raw contents of the CWL file to deploy.

        Returns:
            An AlgorithmDeployment object describing the deployment, including links to
            monitor the deployment job and the resulting process.

        Raises:
            ValueError: If neither algorithm_id nor all of name/version/deployer are provided.
            ValueError: If algorithm_id is not greater than 0.
            ValueError: If neither or both of cwl_href and cwl_raw_text are provided.
            AlgorithmNotFoundError: If no algorithm exists matching the criteria.
            AlgorithmPermissionError: If you are not the original deployer of the algorithm.

        Examples:
            >>> maap.algorithms.put(
            ...     algorithm_id=96,
            ...     cwl_href="https://raw.githubusercontent.com/MAAP-Project/sardem-sarsen/refs/heads/nasa-ogc/cwl_workflows/process_sardem-sarsen_nasa-ogc.cwl",
            ... )
            >>> maap.algorithms.put(
            ...     name="operawatermask1",
            ...     version="ogc",
            ...     deployer="mlucas",
            ...     cwl_raw_text=open("process.cwl").read(),
            ... )
        """
        resolved_id = self._resolve_target_id(algorithm_id, name, version, deployer)
        payload = self._build_cwl_payload(cwl_href, cwl_raw_text)
        return self._replace(resolved_id, payload)

    @overload
    def deploy(self, *, cwl_href: str) -> AlgorithmDeployment: ...
    @overload
    def deploy(self, *, cwl_raw_text: str) -> AlgorithmDeployment: ...

    def deploy(
        self,
        *,
        cwl_href: str | None = None,
        cwl_raw_text: str | None = None,
    ) -> AlgorithmDeployment:
        """Deploy a new algorithm.

        The algorithm definition is provided either as a link to a CWL workflow
        (cwl_href) or as the raw contents of a CWL file (cwl_raw_text). Provide
        exactly one of the two.

        If the algorithm already exists, the API responds with a 409 Conflict
        identifying the existing process; in that case the deployment is retried
        as a replacement (PUT) of that process.

        Args:
            cwl_href: A URL pointing to the CWL workflow to deploy.
            cwl_raw_text: The raw contents of the CWL file to deploy.

        Returns:
            An AlgorithmDeployment object describing the deployment, including links to
            monitor the deployment job and the resulting process.

        Raises:
            ValueError: If neither or both of cwl_href and cwl_raw_text are provided.

        Examples:
            >>> maap.algorithms.deploy(
            ...     cwl_href="https://raw.githubusercontent.com/marjo-luc/ogc-app-pack-examples/refs/heads/main/stac_clip/cwl_workflows/process_stac-clip_main.cwl",
            ... )
            >>> maap.algorithms.deploy(cwl_raw_text=open("process.cwl").read())
        """
        payload = self._build_cwl_payload(cwl_href, cwl_raw_text)

        try:
            data = self._transport.post("/api/ogc/processes", json=payload)
        except APIError as e:
            process_id = e.error.additional_properties.get("processID")
            if e.status_code == 409 and process_id is not None:
                return self._replace(str(process_id), payload)
            raise
        return AlgorithmDeployment.model_validate(data)

    def _replace(self, resolved_id: str, payload: dict[str, Any]) -> AlgorithmDeployment:
        try:
            data = self._transport.put(f"/api/ogc/processes/{resolved_id}", json=payload)
        except NotFoundError as e:
            raise AlgorithmNotFoundError(resolved_id, e.error) from None
        except ForbiddenError as e:
            raise AlgorithmPermissionError(resolved_id, e.error) from None
        return AlgorithmDeployment.model_validate(data)

    @staticmethod
    def _build_cwl_payload(cwl_href: str | None, cwl_raw_text: str | None) -> dict[str, Any]:
        if cwl_href is not None and cwl_raw_text is not None:
            raise ValueError("Provide either cwl_href or cwl_raw_text, not both")
        if cwl_href is not None:
            return {"executionUnit": {"href": cwl_href}}
        if cwl_raw_text is not None:
            return {"cwlRawText": cwl_raw_text}
        raise ValueError("Provide either cwl_href or cwl_raw_text")

    def _resolve_target_id(
        self,
        algorithm_id: str | int | None,
        name: str | None,
        version: str | None,
        deployer: str | None,
    ) -> str:
        if algorithm_id is not None:
            if int(algorithm_id) <= 0:
                raise ValueError("algorithm_id must be greater than 0")
            return str(algorithm_id)
        if name is not None and version is not None and deployer is not None:
            return self._resolve_id(name, version, deployer)
        raise ValueError("Provide either algorithm_id or all of name, version, and deployer")

    def _resolve_id(self, name: str, version: str, deployer: str) -> str:
        algorithms = self.list()
        for algo in algorithms:
            if algo.title == name and algo.version == version and algo.deployed_by == deployer:
                return str(algo.process_id)
        raise AlgorithmNotFoundError(
            f"{name}/{version}/{deployer}",
            error=None,
        )
