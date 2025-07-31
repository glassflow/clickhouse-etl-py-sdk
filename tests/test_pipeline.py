from unittest.mock import patch

import pytest
from pydantic import ValidationError

from glassflow_clickhouse_etl import errors
from glassflow_clickhouse_etl.models import PipelineConfig, PipelineConfigPatch
from glassflow_clickhouse_etl.pipeline import Pipeline


class TestPipelineCreation:
    """Tests for pipeline creation operations."""

    def test_create_success(self, pipeline, mock_success_response):
        """Test successful pipeline creation."""
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            result = pipeline.create()
            mock_request.assert_called_once_with(
                "POST",
                pipeline.ENDPOINT,
                json=pipeline.config.model_dump(mode="json", by_alias=True),
            )
            assert result == pipeline

    def test_create_already_exists(self, pipeline, mock_forbidden_response):
        """Test pipeline creation when a pipeline is already active."""
        with patch("httpx.Client.request", return_value=mock_forbidden_response):
            with pytest.raises(errors.PipelineAlreadyExistsError):
                pipeline.create()

    def test_create_invalid_config(self, invalid_config):
        """Test pipeline creation with invalid configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline(host="http://localhost:8080", config=invalid_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)

    def test_create_bad_request(self, pipeline, mock_bad_request_response):
        """Test pipeline creation with bad request."""
        with patch("httpx.Client.request", return_value=mock_bad_request_response):
            with pytest.raises(errors.ValidationError) as exc_info:
                pipeline.create()
            assert "Bad request" in str(exc_info.value)

    def test_create_connection_error(self, pipeline, mock_connection_error):
        """Test pipeline creation with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.create()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineLifecycle:
    """Tests for pause, resume, delete operations."""

    @pytest.mark.parametrize(
        "operation,method,endpoint",
        [
            ("pause", "POST", "/pause"),
            ("resume", "POST", "/resume"),
            ("delete", "DELETE", ""),
        ],
    )
    def test_lifecycle_operations(
        self, pipeline, mock_success_response, operation, method, endpoint
    ):
        """Test common pipeline lifecycle operations."""
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            result = getattr(pipeline, operation)()
            expected_endpoint = f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}{endpoint}"
            mock_request.assert_called_once_with(method, expected_endpoint)
            if operation == "delete":
                assert result is None
            else:
                assert result == pipeline

    @pytest.mark.parametrize("operation", ["pause", "resume", "delete"])
    def test_lifecycle_not_found(self, pipeline, mock_not_found_response, operation):
        """Test lifecycle operations when pipeline is not found."""
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                getattr(pipeline, operation)()

    @pytest.mark.parametrize("operation", ["pause", "resume", "delete"])
    def test_lifecycle_connection_error(
        self, pipeline, mock_connection_error, operation
    ):
        """Test lifecycle operations with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                getattr(pipeline, operation)()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineModification:
    """Tests for update, rename operations."""

    def test_rename_success(self, pipeline, mock_success_response):
        """Test successful pipeline rename."""
        new_name = "renamed-pipeline"
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            result = pipeline.rename(new_name)
            mock_request.assert_called_once_with(
                "PATCH",
                f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}",
                json={"name": new_name},
            )
            assert result == pipeline
            # After rename, the config should be loaded and name should be updated

    def test_rename_not_found(self, pipeline, mock_not_found_response):
        """Test pipeline rename when pipeline is not found."""
        new_name = "renamed-pipeline"
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.rename(new_name)

    def test_rename_connection_error(self, pipeline, mock_connection_error):
        """Test pipeline rename with connection error."""
        new_name = "renamed-pipeline"
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.rename(new_name)
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

            # After rename, the config should be loaded and name should be updated

    def test_update_success(self, pipeline, mock_success_response):
        """Test successful pipeline update."""
        config = pipeline.config.model_dump(mode="json", by_alias=True)
        patch_data = {"sink": config["sink"]}
        updated_config = config.copy()
        updated_config["sink"]["table"] = "updated_table"
        mock_success_response.json.return_value = updated_config

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline.update(patch_data)
            mock_request.assert_called_with(
                "UPDATE",
                f"{pipeline.ENDPOINT}/{pipeline.pipeline_id}",
                json=PipelineConfigPatch(**patch_data).model_dump(
                    mode="json", by_alias=True, exclude_none=True
                ),
            )
            assert pipeline.config.sink.table == "updated_table"

    def test_update_not_found(self, pipeline, mock_not_found_response):
        """Test pipeline update when pipeline is not found."""
        config = pipeline.config.model_dump(mode="json", by_alias=True)
        patch_data = {"sink": config["sink"]}

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.update(patch_data)

    def test_update_invalid_config(self, pipeline, mock_bad_request_response):
        """Test pipeline update with invalid configuration."""
        patch_data = {"sink": {"invalid": "data"}}
        mock_bad_request_response.json.return_value = {
            "message": "Invalid configuration"
        }
        mock_bad_request_response.status_code = 422

        with patch("httpx.Client.request", return_value=mock_bad_request_response):
            with pytest.raises(errors.PipelineInvalidConfigurationError):
                pipeline.update(patch_data)

    def test_update_connection_error(self, pipeline, mock_connection_error):
        """Test pipeline update with connection error."""
        config = pipeline.config.model_dump(mode="json", by_alias=True)
        patch_data = {"sink": config["sink"]}

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.update(patch_data)
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineValidation:
    """Tests for config validation."""

    def test_validate_config_valid(self, valid_config):
        """Test validation of a valid pipeline configuration."""
        config = PipelineConfig(**valid_config)
        Pipeline.validate_config(config)
        # No exception should be raised

    def test_validate_config_invalid(self, invalid_config):
        """Test validation of an invalid pipeline configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline.validate_config(invalid_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)


class TestPipelineTracking:
    """Tests for tracking functionality."""

    def test_tracking_info(
        self,
        valid_config,
        valid_config_with_dedup_disabled,
        valid_config_without_joins,
        valid_config_without_joins_and_dedup_disabled,
    ):
        """Test tracking info."""
        pipeline = Pipeline(host="http://localhost:8080", config=valid_config)
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080",
            config=valid_config_with_dedup_disabled,
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config_with_dedup_disabled["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080", config=valid_config_without_joins
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_config_without_joins["pipeline_id"],
            "join_enabled": False,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080",
            config=valid_config_without_joins_and_dedup_disabled,
        )
        pipeline_id = valid_config_without_joins_and_dedup_disabled[
            "pipeline_id"
        ]
        assert pipeline._tracking_info() == {
            "pipeline_id": pipeline_id,
            "join_enabled": False,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }
