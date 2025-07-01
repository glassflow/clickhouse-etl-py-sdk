from __future__ import annotations

from typing import Any, Dict, List

import httpx

from .api_client import APIClient


class DLQ(APIClient):
    """
    Dead Letter Queue client for managing failed messages.
    """

    ENDPOINT = "/api/v1/dlq"

    def consume(self, batch_size: int = 100) -> List[Dict[str, Any]]:
        """
        Consume messages from the Dead Letter Queue.

        Args:
            batch_size: Number of messages to consume (between 1 and 1000)

        Returns:
            List of messages from the DLQ

        Raises:
            ValueError: If batch_size is not within valid range
            ConnectionError: If there is a network error
            InternalServerError: If the API request fails
        """
        if not isinstance(batch_size, int) or batch_size < 1 or batch_size > 1000:
            raise ValueError("batch_size must be an integer between 1 and 1000")

        try:
            response = self.client.get(
                f"{self.ENDPOINT}/consume",
                params={"batch_size": batch_size}
            )
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            error_mappings = {
                422: (ValueError, "InvalidBatchSize"),
            }
            self._handle_http_error(e, error_mappings, "DLQConsumeError")
        except httpx.RequestError as e:
            self._handle_connection_error(e, "DLQ")

    def state(self) -> Dict[str, Any]:
        """
        Get the current state of the Dead Letter Queue.

        Returns:
            Dictionary containing DLQ state information

        Raises:
            ConnectionError: If there is a network error
            InternalServerError: If the API request fails
        """
        try:
            response = self.client.get(f"{self.ENDPOINT}/state")
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as e:
            self._handle_http_error(e, None, "DLQStateError")
        except httpx.RequestError as e:
            self._handle_connection_error(e, "DLQ")
