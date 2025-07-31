from unittest.mock import patch

import pytest
from pydantic import ValidationError

from glassflow_clickhouse_etl import errors
from glassflow_clickhouse_etl.models import PipelineConfig, PipelineConfigPatch
from glassflow_clickhouse_etl.pipeline import Pipeline


class TestPipeline:
    """Tests for the Pipeline class."""

    def test_create_pipeline_success(
        self, pipeline_with_config, test_pipeline_method, valid_pipeline_config
    ):
        """Test successful pipeline creation."""
        result, _ = test_pipeline_method(
            pipeline_with_config,
            "create",
            "POST",
            pipeline_with_config.ENDPOINT,
            expected_request_kwargs={
                "json": pipeline_with_config.config.model_dump(
                    mode="json", by_alias=True
                )
            },
        )
        assert result == pipeline_with_config

    def test_create_pipeline_already_exists(
        self, pipeline_with_config, mock_forbidden_response
    ):
        """Test pipeline creation when a pipeline is already active."""
        with patch("httpx.Client.request", return_value=mock_forbidden_response):
            with pytest.raises(errors.PipelineAlreadyExistsError):
                pipeline_with_config.create()

    def test_create_pipeline_invalid_config(self, invalid_pipeline_config):
        """Test pipeline creation with invalid configuration."""
        with pytest.raises((ValueError, ValidationError)) as exc_info:
            Pipeline(host="http://localhost:8080", config=invalid_pipeline_config)
        assert "pipeline_id cannot be empty" in str(exc_info.value)

    def test_create_pipeline_bad_request(
        self, pipeline_with_config, mock_bad_request_response
    ):
        """Test pipeline creation with bad request."""
        with patch("httpx.Client.request", return_value=mock_bad_request_response):
            with pytest.raises(errors.ValidationError) as exc_info:
                pipeline_with_config.create()
            assert "Bad request" in str(exc_info.value)

    def test_create_pipeline_connection_error(
        self, pipeline_with_config, mock_connection_error
    ):
        """Test pipeline creation with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline_with_config.create()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_delete_pipeline_success(self, pipeline_with_config, test_pipeline_method):
        """Test successful pipeline shutdown."""
        result, _ = test_pipeline_method(
            pipeline_with_config,
            "delete",
            "DELETE",
            f"{pipeline_with_config.ENDPOINT}/{pipeline_with_config.pipeline_id}",
        )
        assert result is None

    def test_delete_pipeline_not_found(
        self, pipeline_with_config, mock_not_found_response
    ):
        """Test pipeline shutdown when no pipeline is active."""
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline_with_config.delete()

    def test_delete_pipeline_connection_error(
        self, pipeline_with_config, mock_connection_error
    ):
        """Test pipeline shutdown with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline_with_config.delete()
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

    def test_pause_pipeline_success(self, pipeline_with_config, test_pipeline_method):
        """Test successful pipeline pause."""
        result, _ = test_pipeline_method(
            pipeline_with_config,
            "pause",
            "POST",
            f"{pipeline_with_config.ENDPOINT}/{pipeline_with_config.pipeline_id}/pause",
        )
        assert result == pipeline_with_config

    def test_pause_pipeline_not_found(
        self, pipeline_with_config, mock_not_found_response
    ):
        """Test pipeline pause when no pipeline is active."""
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline_with_config.pause()

    def test_pause_pipeline_connection_error(
        self, pipeline_with_config, mock_connection_error
    ):
        """Test pipeline pause with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline_with_config.pause()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_resume_pipeline_success(self, pipeline_with_config, test_pipeline_method):
        """Test successful pipeline resume."""
        result, _ = test_pipeline_method(
            pipeline_with_config,
            "resume",
            "POST",
            f"{pipeline_with_config.ENDPOINT}/{pipeline_with_config.pipeline_id}/resume",
        )
        assert result == pipeline_with_config

    def test_resume_pipeline_not_found(
        self, pipeline_with_config, mock_not_found_response
    ):
        """Test pipeline resume when no pipeline is active."""
        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline_with_config.resume()

    def test_resume_pipeline_connection_error(
        self, pipeline_with_config, mock_connection_error
    ):
        """Test pipeline resume with connection error."""
        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline_with_config.resume()
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_rename_pipeline_success(self, pipeline_with_config, test_pipeline_method):
        """Test successful pipeline rename."""
        new_name = "renamed-pipeline"
        result, _ = test_pipeline_method(
            pipeline_with_config,
            "rename",
            "PATCH",
            f"{pipeline_with_config.ENDPOINT}/{pipeline_with_config.pipeline_id}",
            method_kwargs={"name": new_name},
            expected_request_kwargs={"json": {"name": new_name}},
        )
        assert result == pipeline_with_config
        assert pipeline_with_config.config.name == new_name

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
            host="http://localhost:8080",
            config=valid_pipeline_config_with_dedup_disabled,
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
        pipeline_id = valid_pipeline_config_without_joins_and_dedup_disabled[
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

    def test_update_pipeline_success(
        self, pipeline_with_config, mock_success_response
    ):
        """Test successful pipeline update."""
        config = pipeline_with_config.config.model_dump(mode="json", by_alias=True)

        patch_data = {"sink": config["sink"]}
        updated_config = config.copy()
        updated_config["sink"]["table"] = "updated_table"
        mock_success_response.json.return_value = updated_config

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_patch:
            pipeline_with_config.update(patch_data)
            mock_patch.assert_called_with(
                "UPDATE",
                f"{pipeline_with_config.ENDPOINT}/{pipeline_with_config.config.pipeline_id}",
                json=PipelineConfigPatch(**patch_data).model_dump(
                    mode="json", by_alias=True, exclude_none=True
                ),
            )
            assert pipeline_with_config.config.sink.table == "updated_table"

    def test_update_pipeline_not_found(
        self, pipeline_with_config, mock_not_found_response
    ):
        """Test pipeline update when pipeline is not found."""
        config = pipeline_with_config.config.model_dump(mode="json", by_alias=True)
        patch_data = {"sink": config["sink"]}

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError):
                pipeline_with_config.update(patch_data)

    def test_update_pipeline_invalid_config(
        self, pipeline_with_config, mock_bad_request_response
    ):
        """Test pipeline update with invalid configuration."""
        patch_data = {"sink": {"invalid": "data"}}
        mock_bad_request_response.json.return_value = {
            "message": "Invalid configuration"
        }
        mock_bad_request_response.status_code = 422

        with patch("httpx.Client.request", return_value=mock_bad_request_response):
            with pytest.raises(errors.PipelineInvalidConfigurationError):
                pipeline_with_config.update(patch_data)

    def test_update_pipeline_connection_error(
        self, pipeline_with_config, mock_connection_error
    ):
        """Test pipeline update with connection error."""
        config = pipeline_with_config.config.model_dump(mode="json", by_alias=True)
        patch_data = {"sink": config["sink"]}

        with patch("httpx.Client.request", side_effect=mock_connection_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                pipeline_with_config.update(patch_data)
            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)
