from __future__ import annotations

from typing import Any, Dict, Optional, Union

import httpx

from . import errors
from .tracking import Tracking


class Client:
    """
    Base client for HTTP operations with centralized error handling.
    """

    _tracking = Tracking()

    def __init__(self, url: str = "http://localhost:8080"):
        """Initialize the Client class.

        Args:
            url: URL of the GlassFlow Clickhouse ETL service
        """
        self.url = url
        self.client = httpx.Client(base_url=url)

    def _request(
        self,
        method: str,
        endpoint: str,
        json: Optional[Dict[str, Any]] = None,
        **kwargs: Any
    ) -> httpx.Response:
        """
        Generic request method with centralized error handling.

        Args:
            method: HTTP method (GET, POST, DELETE, etc.)
            endpoint: API endpoint
            json: JSON data to send
            **kwargs: Additional arguments to pass to httpx

        Returns:
            httpx.Response: The response object

        Raises:
            ConnectionError: If there is a network error
            InternalServerError: If the API request fails with non-specific errors
        """
        try:
            response = self.client.request(method, endpoint, json=json, **kwargs)
            response.raise_for_status()
            return response
        except httpx.RequestError as e:
            self._track_event("RequestError", error_type="ConnectionError")
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def disable_tracking(self) -> None:
        """Disable tracking of pipeline events."""
        self._tracking.enabled = False

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        """Track an event with the given name and properties."""
        self._tracking.track_event(event_name, kwargs)