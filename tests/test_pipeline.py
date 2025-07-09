from unittest.mock import patch

import pytest
from pydantic import ValidationError

from glassflow_clickhouse_etl import errors
from glassflow_clickhouse_etl.models import PipelineConfig
from glassflow_clickhouse_etl.pipeline import Pipeline


class TestPipeline:
    """Tests for the Pipeline class."""

    def test_create_pipeline_success(self, valid_pipeline_config, mock_success_response):
        """Test successful pipeline creation."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_success_response) as mock_post:
            pipeline.create()
            mock_post.assert_called_once_with(
                "POST",
                pipeline.ENDPOINT,
                json=config.model_dump(mode="json", by_alias=True),
            )

    def test_create_pipeline_already_exists(self, valid_pipeline_config, mock_forbidden_response):
        """Test pipeline creation when a pipeline is already active."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_forbidden_response):
            with pytest.raises(errors.PipelineAlreadyExistsError):
                pipeline.create()

    def test_create_pipeline_invalid_config(self, invalid_pipeline_config):
        """Test pipeline creation with invalid configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline(host="http://localhost:8080", config=invalid_pipeline_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)

    def test_create_pipeline_bad_request(self, valid_pipeline_config, mock_bad_request_response):
        """Test pipeline creation with bad request."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_bad_request_response):
            with pytest.raises(errors.ValidationError) as exc_info:
                pipeline.create()
            assert "Bad request" in str(exc_info.value)

    def test_create_pipeline_connection_error(self, valid_pipeline_config, mock_connection_error):
        """Test pipeline creation with connection error."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.create()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_delete_pipeline_success(self, valid_pipeline_config, mock_success_response):
        """Test successful pipeline shutdown."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_delete:
            pipeline.delete()
            mock_delete.assert_called_once_with(
                "DELETE", f"{pipeline.ENDPOINT}/{config.pipeline_id}"
            )

    def test_delete_pipeline_not_found(self, valid_pipeline_config, mock_not_found_response):
        """Test pipeline shutdown when no pipeline is active."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.delete()

    def test_delete_pipeline_connection_error(self, valid_pipeline_config, mock_connection_error):
        """Test pipeline shutdown with connection error."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.delete()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_validate_config_valid(self, valid_pipeline_config):
        """Test validation of a valid pipeline configuration."""
        config = PipelineConfig(**valid_pipeline_config)
        Pipeline.validate_config(config)
        # No exception should be raised

    def test_validate_config_invalid(self, invalid_pipeline_config):
        """Test validation of an invalid pipeline configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline.validate_config(invalid_pipeline_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)

    def test_pause_pipeline_success(self, valid_pipeline_config, mock_success_response):
        """Test successful pipeline pause."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_pause:
            pipeline.pause()
            mock_pause.assert_called_once_with(
                "POST", f"{pipeline.ENDPOINT}/{config.pipeline_id}/pause"
            )

    def test_pause_pipeline_not_found(self, valid_pipeline_config, mock_not_found_response):
        """Test pipeline pause when no pipeline is active."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.pause()

    def test_pause_pipeline_connection_error(self, valid_pipeline_config, mock_connection_error):
        """Test pipeline pause with connection error."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.pause()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_resume_pipeline_success(self, valid_pipeline_config, mock_success_response):
        """Test successful pipeline resume."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_resume:
            pipeline.resume()
            mock_resume.assert_called_once_with(
                "POST", f"{pipeline.ENDPOINT}/{config.pipeline_id}/resume"
            )

    def test_resume_pipeline_not_found(self, valid_pipeline_config, mock_not_found_response):
        """Test pipeline resume when no pipeline is active."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline.resume()

    def test_resume_pipeline_connection_error(self, valid_pipeline_config, mock_connection_error):
        """Test pipeline resume with connection error."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(host="http://localhost:8080", config=config)

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline.resume()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_tracking_info(
        self,
        valid_pipeline_config,
        valid_pipeline_config_with_dedup_disabled,
        valid_pipeline_config_without_joins,
        valid_pipeline_config_without_joins_and_dedup_disabled,
    ):
        """Test tracking info."""
        pipeline = Pipeline(host="http://localhost:8080", config=valid_pipeline_config)
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_pipeline_config["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080", config=valid_pipeline_config_with_dedup_disabled
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_pipeline_config_with_dedup_disabled["pipeline_id"],
            "join_enabled": True,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080", config=valid_pipeline_config_without_joins
        )
        assert pipeline._tracking_info() == {
            "pipeline_id": valid_pipeline_config_without_joins["pipeline_id"],
            "join_enabled": False,
            "deduplication_enabled": True,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }

        pipeline = Pipeline(
            host="http://localhost:8080",
            config=valid_pipeline_config_without_joins_and_dedup_disabled,
        )
        pipeline_id = valid_pipeline_config_without_joins_and_dedup_disabled["pipeline_id"]
        assert pipeline._tracking_info() == {
            "pipeline_id": pipeline_id,
            "join_enabled": False,
            "deduplication_enabled": False,
            "source_auth_method": "SCRAM-SHA-256",
            "source_security_protocol": "SASL_SSL",
            "source_root_ca_provided": True,
            "source_skip_auth": False,
        }
