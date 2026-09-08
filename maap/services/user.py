from maap.services.base import BaseService
from maap.types.user import QueueList, UserProfile


class UserService(BaseService):
    """The current user's profile and available job queues."""

    def get_profile(self) -> UserProfile:
        """Retrieve the profile of the authenticated user.

        The profile identifies whoever the client's token belongs to, so it is a
        quick way to confirm which account a session is running as.

        Returns:
            A UserProfile object holding the user's account details, registered
            public SSH key, and organization memberships.

        Raises:
            AuthenticationError: If the client has no token, or the token is invalid.

        Examples:
            >>> profile = maap.user.get_profile()
            >>> profile.username
            'mlucas'
            >>> [org.name for org in profile.organizations]
            ['JPL Dev Team']
        """
        data = self._transport.get("/api/members/self")
        return UserProfile.model_validate(data)

    def get_queues(self) -> list[str]:
        """Retrieve the names of the worker queues the current user may submit jobs to.

        Queues are named for the resources they provide (e.g. maap-dps-worker-8gb),
        and which of them a user may use depends on the organizations they belong to.
        The names returned here are the values accepted by JobRequest's ``queue`` field.

        Returns:
            A list of queue names, in the order the API returns them.

        Raises:
            AuthenticationError: If the client has no token, or the token is invalid.

        Examples:
            >>> maap.user.get_queues()
            ['maap-dps-sandbox', 'maap-dps-worker-8gb', 'maap-dps-worker-16gb']
            >>> queues = maap.user.get_queues()
            >>> request = JobRequest(algorithm_id=96, inputs={}, queue=queues[0])
        """
        data = self._transport.get("/api/mas/algorithm/resource")
        return QueueList.model_validate(data).queues
