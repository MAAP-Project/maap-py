from pydantic import BaseModel, Field


class Link(BaseModel):
    model_config = {"use_attribute_docstrings": True}

    href: str
    """The target URL of the link."""
    rel: str
    """The relationship of the link to the current resource (e.g. self, monitor)."""
    type: str
    """The media type of the linked resource (e.g. application/json)."""
    hreflang: str | None = None
    """The language of the linked resource, if specified."""
    title: str | None = None
    """A human-readable label for the link."""


class AlgorithmInput(BaseModel):
    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    name: str
    """The name of the input."""
    description: str | None = None
    """Description of what the input is for."""
    type: str | None = None
    """The input's data type (e.g. text)."""
    placeholder: str | None = None
    """An example or placeholder value for the input."""
    source: str | None = Field(None, alias="from")
    """Where the input value comes from (e.g. submitter)."""


class Algorithm(BaseModel):
    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    process_id: int = Field(alias="processID")
    """Unique numeric identifier of the algorithm (process)."""
    id: str
    """The algorithm's short name/slug identifier."""
    title: str
    """Human-readable name of the algorithm."""
    description: str | None = None
    """Description of what the algorithm does."""
    version: str | None = None
    """Version label of the algorithm."""
    keywords: list[str] = []
    """Keywords/tags associated with the algorithm."""
    metadata: list[str] = []
    """Additional metadata entries for the algorithm."""
    job_control_options: list[str] = Field(default=[], alias="jobControlOptions")
    """Supported job execution modes (e.g. sync, async)."""
    author: str | None = None
    """Author of the algorithm."""
    deployed_by: str | None = Field(None, alias="deployedBy")
    """Username of the user who deployed the algorithm."""
    last_modified_time: str | None = Field(None, alias="lastModifiedTime")
    """Timestamp of the algorithm's last modification."""
    cwl_link: str | None = Field(None, alias="cwlLink")
    """Link to the algorithm's CWL workflow definition."""
    github_url: str | None = Field(None, alias="githubUrl")
    """URL of the algorithm's source repository."""
    git_commit_hash: str | None = Field(None, alias="gitCommitHash")
    """Git commit hash the algorithm was built from."""
    ram_min: int | None = Field(None, alias="ramMin")
    """Minimum RAM (in GB) required to run the algorithm."""
    cores_min: int | None = Field(None, alias="coresMin")
    """Minimum number of CPU cores required to run the algorithm."""
    base_command: str | None = Field(None, alias="baseCommand")
    """The base command executed to run the algorithm."""
    links: list[Link] = []
    """Related hypermedia links for the algorithm."""
    inputs: dict[str, AlgorithmInput] = {}
    """Declared inputs the algorithm accepts, keyed by input name."""


class AlgorithmList(BaseModel):
    model_config = {"use_attribute_docstrings": True}

    processes: list[Algorithm]
    """The algorithms matching the request."""
    links: list[Link] = []
    """Related hypermedia links for the listing."""


class AlgorithmPackage(BaseModel):
    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    process_description: str | None = Field(None, alias="processDescription")
    """Description of the process/algorithm."""
    execution_unit: Link = Field(alias="executionUnit")
    """Link to the algorithm's execution unit (CWL)."""


class AlgorithmDeployment(BaseModel):
    model_config = {"populate_by_name": True, "use_attribute_docstrings": True}

    id: str
    """The short name/slug identifier of the deployed algorithm."""
    version: str
    """Version label of the deployed algorithm."""
    links: list[Link] = []
    """Related hypermedia links, including monitoring and process links."""
    process_pipeline_link: Link | None = Field(None, alias="processPipelineLink")
    """Link to the deployment pipeline for monitoring progress."""
