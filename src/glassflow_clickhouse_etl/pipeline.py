from __future__ import annotations

from typing import Any

from pydantic import ValidationError

from . import errors, models
from .api_client import APIClient
from .dlq import DLQ


class Pipeline(APIClient):
    """
    Main class for managing Kafka to ClickHouse pipelines.
    """

    ENDPOINT = "/api/v1/pipeline"

    def __init__(
        self,
        host: str | None = None,
        pipeline_id: str | None = None,
        config: models.PipelineConfig | dict[str, Any] | None = None,
    ):
        """Initialize the Pipeline class.

        Args:
            host: GlassFlow API host
            pipeline_id: ID of the pipeline to create
            config: Pipeline configuration
        """
        super().__init__(host=host)

        if not config and not pipeline_id:
            raise ValueError("Either config or pipeline_id must be provided")
        elif config and pipeline_id:
            raise ValueError("Only one of config or pipeline_id can be provided")

        if pipeline_id is not None:
            self.pipeline_id = pipeline_id

        if config is not None:
            if isinstance(config, dict):
                self.config = models.PipelineConfig.model_validate(config)
            else:
                self.config = config
            self.pipeline_id = self.config.pipeline_id
        else:
            self.config = None

        self._dlq = DLQ(pipeline_id=pipeline_id, host=host)

    def get(self) -> Pipeline:
        """Fetch a pipeline by its ID.

        Returns:
            Pipeline: A Pipeline instance for the given ID

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        try:
            response = self._request("GET", f"{self.ENDPOINT}/{self.pipeline_id}")
            pipeline_data = response.json()

            self.config = models.PipelineConfig.model_validate(pipeline_data)
            return self
        except errors.NotFoundError as e:
            self._track_event("PipelineGetError", error_type="PipelineNotFound")
            raise errors.PipelineNotFoundError(
                f"Pipeline with id '{self.pipeline_id}' not found"
            ) from e
        except errors.APIError as e:
            self._track_event("PipelineGetError", error_type="InternalServerError")
            raise e

    def create(self) -> Pipeline:
        """Creates a new pipeline with the given config.

        Returns:
            Pipeline: A Pipeline instance for the created pipeline

        Raises:
            PipelineAlreadyExistsError: If pipeline already exists
            PipelineInvalidConfigurationError: If configuration is invalid
            APIError: If the API request fails
        """
        if self.config is None:
            raise ValueError("Pipeline configuration must be provided in constructor")
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
            return self

        except errors.ForbiddenError as e:
            self._track_event("PipelineCreateError", error_type="PipelineAlreadyExists")
            raise errors.PipelineAlreadyExistsError(
                f"Pipeline with ID {self.config.pipeline_id} already exists;"
                "delete it first before creating new pipeline or use a"
                "different pipeline ID"
            ) from e
        except errors.APIError as e:
            if e.status_code == 422:
                self._track_event(
                    "PipelineCreateError", error_type="InvalidPipelineConfig"
                )
                raise errors.PipelineInvalidConfigurationError(
                    f"Invalid pipeline configuration: {e.response.text}"
                ) from e
            else:
                self._track_event(
                    "PipelineCreateError", error_type="InternalServerError"
                )
                raise e

    def delete(self) -> None:
        """Deletes the pipeline with the given ID.

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        if self.config is None:
            self.get()
        try:
            endpoint = f"{self.ENDPOINT}/{self.pipeline_id}"
            self._request("DELETE", endpoint)
            self._track_event("PipelineDeleted")
        except errors.NotFoundError as e:
            self._track_event("PipelineDeleteError", error_type="PipelineNotFound")
            raise errors.PipelineNotFoundError(
                f"Pipeline with id '{self.pipeline_id}' not found"
            ) from e
        except errors.APIError as e:
            self._track_event("PipelineDeleteError", error_type="InternalServerError")
            raise e

    def pause(self) -> Pipeline:
        """Pauses the pipeline with the given ID.

        Returns:
            Pipeline: A Pipeline instance for the paused pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        try:
            endpoint = f"{self.ENDPOINT}/{self.pipeline_id}/pause"
            self._request("POST", endpoint)
            self._track_event("PipelinePaused")
            return self
        except errors.NotFoundError as e:
            self._track_event("PipelinePauseError", error_type="PipelineNotFound")
            raise errors.PipelineNotFoundError(
                f"Pipeline with id '{self.pipeline_id}' not found"
            ) from e
        except errors.APIError as e:
            self._track_event("PipelinePauseError", error_type="InternalServerError")
            raise e

    def resume(self) -> Pipeline:
        """Resumes the pipeline with the given ID.

        Returns:
            Pipeline: A Pipeline instance for the resumed pipeline

        Raises:
            PipelineNotFoundError: If pipeline is not found
            APIError: If the API request fails
        """
        try:
            endpoint = f"{self.ENDPOINT}/{self.pipeline_id}/resume"
            self._request("POST", endpoint)
            self._track_event("PipelineResumed")
            return self
        except errors.NotFoundError as e:
            self._track_event("PipelineResumeError", error_type="PipelineNotFound")
            raise errors.PipelineNotFoundError(
                f"Pipeline with id '{self.pipeline_id}' not found"
            ) from e
        except errors.APIError as e:
            self._track_event("PipelineResumeError", error_type="InternalServerError")
            raise e

    def to_dict(self) -> dict[str, Any]:
        """Convert the pipeline configuration to a dictionary.

        Returns:
            dict: Pipeline configuration as a dictionary
        """
        if not hasattr(self, "config") or self.config is None:
            return {"pipeline_id": self.pipeline_id}

        return self.config.model_dump(
            mode="json",
            by_alias=True,
            exclude_none=True,
        )

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

    @property
    def dlq(self) -> DLQ:
        """Get the DLQ (Dead Letter Queue) client for this pipeline.

        Returns:
            DLQ: The DLQ client instance
        """
        return self._dlq

    def _tracking_info(self) -> dict[str, Any]:
        """Get information about the active pipeline."""
        # If config is not set, return minimal info
        if not hasattr(self, "config") or self.config is None:
            return {
                "pipeline_id": getattr(self, "pipeline_id", "unknown"),
            }

        # Extract join info
        if hasattr(self.config, "join") and self.config.join is not None:
            join_enabled = self.config.join.enabled
        else:
            join_enabled = False

        # Extract deduplication info
        deduplication_enabled = False
        if hasattr(self.config, "source") and hasattr(self.config.source, "topics"):
            for topic in self.config.source.topics:
                if hasattr(topic, "deduplication") and topic.deduplication is not None:
                    deduplication_enabled = topic.deduplication.enabled
                    break

        # Extract connection params
        if hasattr(self.config, "source") and hasattr(
            self.config.source, "connection_params"
        ):
            conn_params = self.config.source.connection_params

            if hasattr(conn_params, "root_ca") and conn_params.root_ca is not None:
                root_ca_provided = True
            else:
                root_ca_provided = False

            if hasattr(conn_params, "skip_auth") and conn_params.skip_auth is not None:
                skip_auth = conn_params.skip_auth
            else:
                skip_auth = False

            protocol = getattr(conn_params, "protocol", "unknown")
            mechanism = getattr(conn_params, "mechanism", "unknown")
        else:
            root_ca_provided = False
            skip_auth = False
            protocol = "unknown"
            mechanism = "unknown"

        # Get pipeline_id from config or instance variable
        pipeline_id = getattr(
            self.config, "pipeline_id", getattr(self, "pipeline_id", "unknown")
        )

        return {
            "pipeline_id": pipeline_id,
            "join_enabled": join_enabled,
            "deduplication_enabled": deduplication_enabled,
            "source_auth_method": mechanism,
            "source_security_protocol": protocol,
            "source_root_ca_provided": root_ca_provided,
            "source_skip_auth": skip_auth,
        }

    def _track_event(self, event_name: str, **kwargs: Any) -> None:
        pipeline_properties = self._tracking_info()
        properties = {**pipeline_properties, **kwargs}
        super()._track_event(event_name, **properties)
