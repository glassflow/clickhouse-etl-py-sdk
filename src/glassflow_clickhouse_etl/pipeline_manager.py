from __future__ import annotations

from typing import Any, List

import httpx

from . import errors, models
from .pipeline import Pipeline
from .tracking import Tracking


class PipelineManager:
    """
    Manager class for handling multiple Pipeline instances.
    """

    ENDPOINT = "/api/v1/pipeline"
    _tracking = Tracking()

    def __init__(self, url: str = "http://localhost:8080"):
        """Initialize the PipelineManager class.

        Args:
            url: URL of the GlassFlow Clickhouse ETL service
        """
        self.url = url
        self.client = httpx.Client(base_url=url)

    def get(self, pipeline_id: str) -> Pipeline:
        """Fetch a pipeline by its ID.

        Args:
            pipeline_id: The ID of the pipeline to fetch

        Returns:
            Pipeline: A Pipeline instance for the given ID

        Raises:
            PipelineNotFoundError: If pipeline is not found
            InternalServerError: If the API request fails
            ConnectionError: If there is a network error
        """
        try:
            response = self.client.get(f"{self.ENDPOINT}/{pipeline_id}")
            response.raise_for_status()
            pipeline_data = response.json()
            
            # Create a PipelineConfig from the response data
            config = models.PipelineConfig.model_validate(pipeline_data)
            return Pipeline(config=config, url=self.url, pipeline_id=pipeline_id)
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._track_event("PipelineGetError", error_type="PipelineNotFound")
                raise errors.PipelineNotFoundError(
                    f"Pipeline with id '{pipeline_id}' not found"
                ) from e
            else:
                self._track_event("PipelineGetError", error_type="InternalServerError")
                raise errors.InternalServerError(
                    f"Failed to get pipeline {pipeline_id}: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            self._track_event("PipelineGetError", error_type="ConnectionError")
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def list(self) -> List[str]:
        """Returns a list of available pipeline IDs.

        Returns:
            List[str]: List of pipeline IDs

        Raises:
            InternalServerError: If the API request fails
            ConnectionError: If there is a network error
        """
        try:
            response = self.client.get(self.ENDPOINT)
            response.raise_for_status()
            data = response.json()
            
            # Handle different response formats
            if isinstance(data, list):
                # If response is a list of pipeline objects
                return [pipeline.get("id", pipeline.get("pipeline_id")) for pipeline in data if "id" in pipeline or "pipeline_id" in pipeline]
            elif isinstance(data, dict) and "pipelines" in data:
                # If response is wrapped in a "pipelines" key
                return [pipeline.get("id", pipeline.get("pipeline_id")) for pipeline in data["pipelines"] if "id" in pipeline or "pipeline_id" in pipeline]
            elif isinstance(data, dict) and "id" in data:
                # If response is a single pipeline (current behavior)
                return [data["id"]]
            else:
                return []
                
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                # No pipelines found, return empty list
                return []
            else:
                self._track_event("PipelineListError", error_type="InternalServerError")
                raise errors.InternalServerError(
                    f"Failed to list pipelines: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            self._track_event("PipelineListError", error_type="ConnectionError")
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def create(self, pipeline_config: dict[str, Any] | models.PipelineConfig) -> Pipeline:
        """Creates a new pipeline with the given config.

        Args:
            pipeline_config: Dictionary or PipelineConfig object containing the pipeline configuration

        Returns:
            Pipeline: A Pipeline instance for the created pipeline

        Raises:
            PipelineAlreadyExistsError: If pipeline already exists
            InvalidPipelineConfigError: If configuration is invalid
            InternalServerError: If the API request fails
            ConnectionError: If there is a network error
        """
        # Validate and create PipelineConfig object
        if isinstance(pipeline_config, dict):
            config = models.PipelineConfig.model_validate(pipeline_config)
        else:
            config = pipeline_config

        try:
            response = self.client.post(
                self.ENDPOINT,
                json=config.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
            )
            response.raise_for_status()

            self._track_event("PipelineDeployed")
            return Pipeline(config=config, url=self.url, pipeline_id=config.pipeline_id)
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                self._track_event(
                    "PipelineCreateError", error_type="PipelineAlreadyExists"
                )
                raise errors.PipelineAlreadyExistsError(
                    f"Pipeline with id {config.pipeline_id} already active; "
                    "shutdown to start another"
                ) from e
            elif e.response.status_code == 422:
                self._track_event(
                    "PipelineCreateError", error_type="InvalidPipelineConfig"
                )
                raise errors.InvalidPipelineConfigError(
                    f"Invalid pipeline configuration: {e.response.text}"
                ) from e
            elif e.response.status_code == 400:
                self._track_event("PipelineCreateError", error_type="BadRequest")
                raise ValueError(f"Bad request: {e.response.text}") from e
            else:
                self._track_event(
                    "PipelineCreateError", error_type="InternalServerError"
                )
                raise errors.InternalServerError(
                    f"Failed to create pipeline: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            self._track_event("PipelineCreateError", error_type="ConnectionError")
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def delete(self, pipeline_id: str) -> None:
        """Deletes the pipeline with the given ID.

        Args:
            pipeline_id: The ID of the pipeline to delete

        Raises:
            PipelineNotFoundError: If pipeline is not found
            InternalServerError: If the API request fails
            ConnectionError: If there is a network error
        """
        try:
            response = self.client.delete(f"{self.ENDPOINT}/{pipeline_id}")
            response.raise_for_status()

            self._track_event("PipelineDeleted")
            
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._track_event("PipelineDeleteError", error_type="PipelineNotFound")
                raise errors.PipelineNotFoundError(
                    f"Pipeline with id '{pipeline_id}' not found"
                ) from e
            else:
                self._track_event(
                    "PipelineDeleteError", error_type="InternalServerError"
                )
                raise errors.InternalServerError(
                    f"Failed to delete pipeline {pipeline_id}: {e.response.text}"
                ) from e
        except httpx.RequestError as e:
            self._track_event("PipelineDeleteError", error_type="ConnectionError")
            raise errors.ConnectionError(
                f"Failed to connect to pipeline service: {e}"
            ) from e

    def disable_tracking(self) -> None:
        """Disable tracking of pipeline events."""
        self._tracking.enabled = False

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        """Track an event with the given name and properties."""
        self._tracking.track_event(event_name, kwargs)