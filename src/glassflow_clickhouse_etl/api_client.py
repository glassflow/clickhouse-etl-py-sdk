from __future__ import annotations

import httpx

from . import errors


class APIClient:
    """
    Base class for API clients providing common HTTP functionality.
    """

    def __init__(self, url: str = "http://localhost:8080"):
        """Initialize the API client.

        Args:
            url: Base URL of the GlassFlow Clickhouse ETL service
        """
        self.client = httpx.Client(base_url=url)

    def _handle_http_error(
        self,
        error: httpx.HTTPStatusError,
        error_mappings: dict[int, tuple[type[Exception], str]] | None = None,
        default_error_type: str = "InternalServerError",
    ) -> None:
        """Handle HTTP status errors with configurable error mappings.

        Args:
            error: The HTTP status error to handle
            error_mappings: Dictionary mapping status codes to
                (exception_class, error_type) tuples
            default_error_type: Default error type for tracking when no mapping
                is found
        """
        if error_mappings and error.response.status_code in error_mappings:
            exception_class, error_type = error_mappings[
                error.response.status_code
            ]
            if hasattr(self, "_track_event"):
                self._track_event(
                    f"{self.__class__.__name__}Error", error_type=error_type
                )
            raise exception_class(f"API error: {error.response.text}") from error
        else:
            if hasattr(self, "_track_event"):
                self._track_event(
                    f"{self.__class__.__name__}Error",
                    error_type=default_error_type,
                )
            raise errors.InternalServerError(
                f"API request failed: {error.response.text}"
            ) from error

    def _handle_connection_error(
        self,
        error: httpx.RequestError,
        operation: str = "API operation",
    ) -> None:
        """Handle connection errors.

        Args:
            error: The connection error to handle
            operation: Description of the operation that failed
        """
        if hasattr(self, "_track_event"):
            self._track_event(
                f"{self.__class__.__name__}Error", error_type="ConnectionError"
            )
        service_name = (
            operation.replace("pipeline ", "")
            .replace("getting ", "")
            .replace(" pipeline", "")
        )
        raise errors.ConnectionError(
            f"Failed to connect to {service_name} service: {error}"
        ) from error
