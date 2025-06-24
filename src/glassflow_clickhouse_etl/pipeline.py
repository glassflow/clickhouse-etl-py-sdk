from __future__ import annotations

from typing import Any, TYPE_CHECKING

import httpx
from pydantic import ValidationError

from . import errors, models
from .client import Client

if TYPE_CHECKING:
    from .pipeline_manager import PipelineManager


class Pipeline(Client):
    """
    Main class for managing Kafka to ClickHouse pipelines.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(
        self,
        config: models.PipelineConfig | dict[str, Any] | str | None = None,
        url: str = "http://localhost:8080",
        pipeline_id: str | None = None,
        manager: PipelineManager | None = None,
    ):
        """Initialize the Pipeline class.

        Args:
            config: Pipeline configuration or pipeline_id string (for backwards compatibility)
            url: URL of the GlassFlow Clickhouse ETL service
            pipeline_id: Explicit pipeline ID (when config is not a string)
            manager: Optional PipelineManager instance for delegating operations
        """
        super().__init__(url)
        
        # Handle backwards compatibility
        if isinstance(config, str):
            # New style: Pipeline("pipeline-id", ...)
            self.pipeline_id = config
            self.config = None
        elif config is not None:
            # Old style: Pipeline(config=config, ...)
            if isinstance(config, dict):
                config = models.PipelineConfig.model_validate(config)
            self.config = config
            self.pipeline_id = pipeline_id or config.pipeline_id
        else:
            # Legacy style: Pipeline(pipeline_id="...", ...)
            if pipeline_id is None:
                raise ValueError("pipeline_id is required when config is not provided")
            self.pipeline_id = pipeline_id
            self.config = None

        self.manager = manager

    def create(self) -> None:
        """Create a new pipeline with the given configuration."""
        if self.config is None:
            raise ValueError("Pipeline configuration is required")

        # Delegate to manager if available
        if self.manager:
            self.manager.create(self.config)
            return

        try:
            self._request(
                "POST",
                self.ENDPOINT,
                json=self.config.model_dump(
                    mode="json",
                    by_alias=True,
                    exclude_none=True,
                ),
            )

            self._track_event("PipelineDeployed")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 403:
                self._track_event(
                    "PipelineCreateError", error_type="PipelineAlreadyExists"
                )
                raise errors.PipelineAlreadyExistsError(
                    f"Pipeline with id {self.config.pipeline_id} already active; "
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

    def delete(self) -> None:
        """Shutdown the active pipeline.

        Raises:
            PipelineNotFoundError: If no active pipeline is found.
            httpx.HTTPStatusError: If the API request fails.
            httpx.RequestError: If there is a network error.
        """
        # Delegate to manager if available
        if self.manager:
            self.manager.delete(self.pipeline_id)
            return

        try:
            # Use pipeline-specific endpoint since pipeline_id is required
            endpoint = f"{self.ENDPOINT}/{self.pipeline_id}"
            self._request("DELETE", endpoint)
            self._track_event("PipelineDeleted")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._track_event("PipelineDeleteError", error_type="PipelineNotFound")
                raise errors.PipelineNotFoundError(
                    f"Pipeline with id '{self.pipeline_id}' not found"
                ) from e
            else:
                self._track_event(
                    "PipelineDeleteError", error_type="InternalServerError"
                )
                raise errors.InternalServerError(
                    f"Failed to shutdown pipeline: {e.response.text}"
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
            response = self._request("GET", self.ENDPOINT)
            return response.json().get("id")
        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                self._track_event("PipelineGetError", error_type="PipelineNotFound")
                raise errors.PipelineNotFoundError("No running pipeline found") from e
            else:
                self._track_event("PipelineGetError", error_type="InternalServerError")
                raise errors.InternalServerError(
                    f"Failed to get running pipeline: {e.response.text}"
                ) from e



    def to_dict(self) -> dict[str, Any]:
        """Convert the pipeline configuration to a dictionary.

        Returns:
            dict: Pipeline configuration as a dictionary
        """
        if self.config is None:
            return {}
        
        return self.config.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )

    def _tracking_info(self) -> dict[str, Any]:
        """Get information about the active pipeline."""
        if self.config is not None:
            if self.config.join is not None:
                join_enabled = self.config.join.enabled
            else:
                join_enabled = False

            for topic in self.config.source.topics:
                if topic.deduplication is not None:
                    deduplication_enabled = topic.deduplication.enabled
                    break
            else:
                deduplication_enabled = False

            if self.config.source.connection_params.root_ca is not None:
                root_ca_provided = True
            else:
                root_ca_provided = False

            if self.config.source.connection_params.skip_auth is not None:
                skip_auth = self.config.source.connection_params.skip_auth
            else:
                skip_auth = False
            protocol = self.config.source.connection_params.protocol
            mechanism = self.config.source.connection_params.mechanism

            return {
                "pipeline_id": self.pipeline_id,
                "join_enabled": join_enabled,
                "deduplication_enabled": deduplication_enabled,
                "source_auth_method": mechanism,
                "source_security_protocol": protocol,
                "source_root_ca_provided": root_ca_provided,
                "source_skip_auth": skip_auth,
            }
        else:
            return {"pipeline_id": self.pipeline_id}

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        pipeline_properties = self._tracking_info()
        properties = {**pipeline_properties, **kwargs}
        self._tracking.track_event(event_name, properties)
