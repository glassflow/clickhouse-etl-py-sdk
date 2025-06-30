from __future__ import annotations

from typing import Any, List

from . import errors, models
from .pipeline import Pipeline
from .api_client import APIClient


class Client(APIClient):
    """
    Manager class for handling multiple Pipeline instances.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(self, host: str | None = None) -> None:
        """Initialize the PipelineManager class.

        Args:
            host: GlassFlow API host
        """
        super().__init__(host=host)

    def get_pipeline(self, pipeline_id: str):
        """Fetch a pipeline by its ID.

        Args:
            pipeline_id: The ID of the pipeline to fetch

        Returns:
            Pipeline: A Pipeline instance for the given ID

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        return Pipeline(host=self.host, pipeline_id=pipeline_id).get()

    def list_pipelines(self) -> List[str]:
        """Returns a list of available pipeline IDs.

        Returns:
            List[str]: List of pipeline IDs

        Raises:
            APIError: If the API request fails
        """
        try:
            response = self._request("GET", self.ENDPOINT)
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

        except errors.NotFoundError as e:
            # No pipelines found, return empty list
            return []
        except errors.APIError as e:
            self._track_event("PipelineListError", error_type="InternalServerError")
            raise errors.APIError(
                f"Failed to list pipelines: {e.response.text}"
            ) from e

    def create_pipeline(self, pipeline_config: dict[str, Any] | models.PipelineConfig):
        """Creates a new pipeline with the given config.

        Args:
            pipeline_config: Dictionary or PipelineConfig object containing the pipeline configuration

        Returns:
            Pipeline: A Pipeline instance for the created pipeline

        Raises:
            PipelineAlreadyExistsError: If pipeline already exists
            PipelineInvalidConfigurationError: If configuration is invalid
            APIError: If the API request fails
        """
        return Pipeline(config=pipeline_config, host=self.host).create()

    def delete_pipeline(self, pipeline_id: str) -> None:
        """Deletes the pipeline with the given ID.

        Args:
            pipeline_id: The ID of the pipeline to delete

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        Pipeline(host=self.host, pipeline_id=pipeline_id).delete()

    def disable_tracking(self) -> None:
        """Disable tracking of pipeline events."""
        self._tracking.enabled = False
