from unittest.mock import MagicMock, patch

import pytest

from glassflow_clickhouse_etl import errors
from glassflow_clickhouse_etl.client import Client
from glassflow_clickhouse_etl.models import PipelineConfig
from glassflow_clickhouse_etl.pipeline import Pipeline


class TestClient:
    """Tests for the Client class."""

    def test_client_init(self):
        """Test Client initialization."""
        client = Client(host="https://example.com")
        assert client.host == "https://example.com"
        assert client.http_client.base_url == "https://example.com"

    def test_client_get_pipeline_success(
        self, valid_pipeline_config, mock_success_response
    ):
        """Test successful pipeline retrieval by ID."""
        client = Client()
        pipeline_id = "test-pipeline-id"

        mock_success_response.json.return_value = valid_pipeline_config

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline = client.get_pipeline(pipeline_id)
            mock_request.assert_called_once_with(
                "GET", f"{client.ENDPOINT}/{pipeline_id}"
            )
            assert isinstance(pipeline, Pipeline)
            assert pipeline.pipeline_id == pipeline_id

    def test_client_get_pipeline_not_found(self, mock_not_found_response):
        """Test pipeline retrieval when pipeline is not found."""
        client = Client()
        pipeline_id = "non-existent-pipeline"

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError) as exc_info:
                client.get_pipeline(pipeline_id)
            assert "not found" in str(exc_info.value)

    def test_client_list_pipelines_success_list_format(self):
        """Test successful pipeline listing with list format response."""
        client = Client()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = [
            {"id": "pipeline-1"},
            {"id": "pipeline-2"},
            {"pipeline_id": "pipeline-3"},
        ]

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipeline_ids = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert pipeline_ids == ["pipeline-1", "pipeline-2", "pipeline-3"]

    def test_client_list_pipeline_success_single_format(self):
        """Test successful pipeline listing with single pipeline format response."""
        client = Client()
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.raise_for_status.return_value = None
        mock_response.json.return_value = {"id": "single-pipeline"}

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipeline_ids = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert pipeline_ids == ["single-pipeline"]

    def test_client_list_pipelines_empty(self):
        """Test pipeline listing when no pipelines exist."""
        client = Client()
        mock_response = MagicMock()
        mock_response.status_code = 404
        mock_response.raise_for_status.side_effect = None  # Don't raise for 404

        with patch("httpx.Client.request", return_value=mock_response) as mock_request:
            pipeline_ids = client.list_pipelines()
            mock_request.assert_called_once_with("GET", client.ENDPOINT)
            assert pipeline_ids == []

    def test_client_create_pipeline_success(
        self, valid_pipeline_config, mock_success_response
    ):
        """Test successful pipeline creation."""
        client = Client()

        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline = client.create_pipeline(valid_pipeline_config)
            mock_request.assert_called_once_with(
                "POST", client.ENDPOINT, json=mock_request.call_args[1]["json"]
            )
            assert isinstance(pipeline, Pipeline)
            assert pipeline.pipeline_id == valid_pipeline_config["pipeline_id"]

    def test_client_create_pipeline_already_exists(
        self, valid_pipeline_config, mock_forbidden_response
    ):
        """Test pipeline creation when pipeline already exists."""
        client = Client()

        with patch("httpx.Client.request", return_value=mock_forbidden_response):
            with pytest.raises(errors.PipelineAlreadyExistsError):
                client.create_pipeline(valid_pipeline_config)

    def test_client_delete_pipeline_success(
        self, mock_success_response, mock_success_get_pipeline
    ):
        """Test successful pipeline deletion."""
        client = Client()
        pipeline_id = "test-pipeline-id"

        with patch("glassflow_clickhouse_etl.pipeline.Pipeline.get") as pipeline_get:
            with patch(
                "httpx.Client.request", return_value=mock_success_response
            ) as mock_delete_request:
                client.delete_pipeline(pipeline_id)
                pipeline_get.assert_called_once_with()
                mock_delete_request.assert_called_once_with(
                    "DELETE", f"{client.ENDPOINT}/{pipeline_id}"
                )

    def test_client_delete_pipeline_not_found(self, mock_not_found_response):
        """Test pipeline deletion when pipeline is not found."""
        client = Client()
        pipeline_id = "non-existent-pipeline"

        with patch("httpx.Client.request", return_value=mock_not_found_response):
            with pytest.raises(errors.PipelineNotFoundError) as exc_info:
                client.delete_pipeline(pipeline_id)
            assert "not found" in str(exc_info.value)

    def test_pipeline_to_dict(self, valid_pipeline_config):
        """Test Pipeline to_dict method."""
        config = PipelineConfig(**valid_pipeline_config)
        pipeline = Pipeline(config=config)

        pipeline_dict = pipeline.to_dict()
        assert isinstance(pipeline_dict, dict)
        assert pipeline_dict["pipeline_id"] == valid_pipeline_config["pipeline_id"]

    def test_pipeline_delete(self, pipeline_from_id, mock_success_response):
        """Test Pipeline delete with explicit pipeline_id."""
        with patch(
            "httpx.Client.request", return_value=mock_success_response
        ) as mock_request:
            pipeline_from_id.delete()
            mock_request.assert_called_once_with(
                "DELETE", f"{pipeline_from_id.ENDPOINT}/{pipeline_from_id.pipeline_id}"
            )
