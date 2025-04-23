from __future__ import annotations

from typing import Any

import httpx
from pydantic import ValidationError

from . import errors, models


class Pipeline:
    """
    Main class for managing Kafka to ClickHouse pipelines.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(
        self,
        config: models.PipelineConfig | dict[str, Any] | None = None,
        url: str = "http://localhost:8080",
    ):
        """Initialize the Pipeline class.

        Args:
            config: Pipeline configuration
            url: URL of the GlassFlow Clickhouse ETL service
        """
        if isinstance(config, dict):
            config = models.PipelineConfig.model_validate(config)

        self.config = config
        self.client = httpx.Client(base_url=url)

    def create(self) -> None:
        """Create a new pipeline with the given configuration."""
        if self.config is None:
            raise ValueError("Pipeline configuration is required")

        try:
            response = self.client.post(
                self.ENDPOINT,
                json=self.config.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
            )
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                raise errors.PipelineAlreadyExistsError(
                    f"Pipeline with id {self.config.pipeline_id} already active; "
                    "shutdown to start another"
                ) from e
            elif e.response.status_code == 422:
                raise errors.InvalidPipelineConfigError(
                    f"Invalid pipeline configuration: {e.response.text}"
                ) from e
            elif e.response.status_code == 400:
                raise ValueError(f"Bad request: {e.response.text}") from e
            else:
                raise errors.InternalServerError(
                    f"Failed to create pipeline: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def delete(self) -> None:
        """Shutdown the active pipeline.

        Raises:
            PipelineNotFoundError: If no active pipeline is found.
            httpx.HTTPStatusError: If the API request fails.
            httpx.RequestError: If there is a network error.
        """
        try:
            response = self.client.delete(f"{self.ENDPOINT}/shutdown")
            response.raise_for_status()
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise errors.PipelineNotFoundError(
                    "No active pipeline to shutdown"
                ) from e
            else:
                raise errors.InternalServerError(
                    f"Failed to shutdown pipeline: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    @staticmethod
    def validate_config(config: dict[str, Any]) -> bool:
        """
        Validate a pipeline configuration.

        Args:
            config: Dictionary containing the pipeline configuration

        Returns:
            True if the configuration is valid

        Raises:
            ValueError: If the configuration is invalid
            ValidationError: If the configuration fails Pydantic validation
        """
        try:
            models.PipelineConfig.model_validate(config)
            return True
        except ValidationError as e:
            raise e
        except ValueError as e:
            raise e

    def get_running_pipeline(self) -> str:
        """
        Get the ID of a running pipeline if it exists.

        Returns:
            str: The ID of the running pipeline.

        Raises:
            PipelineNotFoundError: If no running pipeline is found.
            httpx.HTTPStatusError: If the API request fails.
        """
        try:
            response = self.client.get(self.ENDPOINT)
            response.raise_for_status()
            return response.json().get("id")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                raise errors.PipelineNotFoundError("No running pipeline found") from e
            raise errors.InternalServerError(
                f"Failed to get running pipeline: {e.response.text}"
            ) from e
        except httpx.RequestError as e:
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e
