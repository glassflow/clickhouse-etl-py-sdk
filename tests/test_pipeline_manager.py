from unittest.mock import MagicMock, patch

import pytest

from glassflow_clickhouse_etl import errors
from glassflow_clickhouse_etl.models import PipelineConfig
from glassflow_clickhouse_etl.pipeline import Pipeline
from glassflow_clickhouse_etl.pipeline_manager import PipelineManager


def test_pipeline_manager_init():
    """Test PipelineManager initialization."""
    manager = PipelineManager(url="http://example.com")
    assert manager.url == "http://example.com"
    assert manager.client.base_url == "http://example.com"


def test_pipeline_manager_get_success(valid_pipeline_config, mock_success_response):
    """Test successful pipeline retrieval by ID."""
    manager = PipelineManager()
    pipeline_id = "test-pipeline-id"
    
    mock_success_response.json.return_value = valid_pipeline_config
    
    with patch("httpx.Client.get", return_value=mock_success_response) as mock_get:
        pipeline = manager.get(pipeline_id)
        mock_get.assert_called_once_with(f"{manager.ENDPOINT}/{pipeline_id}")
        assert isinstance(pipeline, Pipeline)
        assert pipeline.pipeline_id == pipeline_id


def test_pipeline_manager_get_not_found(mock_not_found_response):
    """Test pipeline retrieval when pipeline is not found."""
    manager = PipelineManager()
    pipeline_id = "non-existent-pipeline"
    
    with patch("httpx.Client.get", return_value=mock_not_found_response):
        with pytest.raises(errors.PipelineNotFoundError) as exc_info:
            manager.get(pipeline_id)
        assert "not found" in str(exc_info.value)


def test_pipeline_manager_list_success_list_format():
    """Test successful pipeline listing with list format response."""
    manager = PipelineManager()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {"id": "pipeline-1"},
        {"id": "pipeline-2"},
        {"pipeline_id": "pipeline-3"}
    ]
    
    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        pipeline_ids = manager.list()
        mock_get.assert_called_once_with(manager.ENDPOINT)
        assert pipeline_ids == ["pipeline-1", "pipeline-2", "pipeline-3"]


def test_pipeline_manager_list_success_single_format():
    """Test successful pipeline listing with single pipeline format response."""
    manager = PipelineManager()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"id": "single-pipeline"}
    
    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        pipeline_ids = manager.list()
        mock_get.assert_called_once_with(manager.ENDPOINT)
        assert pipeline_ids == ["single-pipeline"]


def test_pipeline_manager_list_empty():
    """Test pipeline listing when no pipelines exist."""
    manager = PipelineManager()
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.raise_for_status.side_effect = None  # Don't raise for 404
    
    with patch("httpx.Client.get", return_value=mock_response) as mock_get:
        pipeline_ids = manager.list()
        mock_get.assert_called_once_with(manager.ENDPOINT)
        assert pipeline_ids == []


def test_pipeline_manager_create_success(valid_pipeline_config, mock_success_response):
    """Test successful pipeline creation."""
    manager = PipelineManager()
    
    with patch("httpx.Client.post", return_value=mock_success_response) as mock_post:
        pipeline = manager.create(valid_pipeline_config)
        mock_post.assert_called_once()
        assert isinstance(pipeline, Pipeline)
        assert pipeline.pipeline_id == valid_pipeline_config["pipeline_id"]


def test_pipeline_manager_create_already_exists(valid_pipeline_config, mock_forbidden_response):
    """Test pipeline creation when pipeline already exists."""
    manager = PipelineManager()
    
    with patch("httpx.Client.post", return_value=mock_forbidden_response):
        with pytest.raises(errors.PipelineAlreadyExistsError):
            manager.create(valid_pipeline_config)


def test_pipeline_manager_delete_success(mock_success_response):
    """Test successful pipeline deletion."""
    manager = PipelineManager()
    pipeline_id = "test-pipeline-id"
    
    with patch("httpx.Client.delete", return_value=mock_success_response) as mock_delete:
        manager.delete(pipeline_id)
        mock_delete.assert_called_once_with(f"{manager.ENDPOINT}/{pipeline_id}")


def test_pipeline_manager_delete_not_found(mock_not_found_response):
    """Test pipeline deletion when pipeline is not found."""
    manager = PipelineManager()
    pipeline_id = "non-existent-pipeline"
    
    with patch("httpx.Client.delete", return_value=mock_not_found_response):
        with pytest.raises(errors.PipelineNotFoundError) as exc_info:
            manager.delete(pipeline_id)
        assert "not found" in str(exc_info.value)


def test_pipeline_with_explicit_id(valid_pipeline_config):
    """Test Pipeline initialization with explicit pipeline_id."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline_id = "custom-pipeline-id"
    pipeline = Pipeline(config=config, pipeline_id=pipeline_id)
    
    assert pipeline.pipeline_id == pipeline_id
    assert pipeline.config.pipeline_id == valid_pipeline_config["pipeline_id"]


def test_pipeline_to_dict(valid_pipeline_config):
    """Test Pipeline to_dict method."""
    config = PipelineConfig(**valid_pipeline_config)
    pipeline = Pipeline(config=config)
    
    pipeline_dict = pipeline.to_dict()
    assert isinstance(pipeline_dict, dict)
    assert pipeline_dict["pipeline_id"] == valid_pipeline_config["pipeline_id"]


def test_pipeline_to_dict_no_config():
    """Test Pipeline to_dict method with no config."""
    pipeline = Pipeline()
    
    pipeline_dict = pipeline.to_dict()
    assert pipeline_dict == {}


def test_pipeline_delete_with_id(mock_success_response):
    """Test Pipeline delete with explicit pipeline_id."""
    pipeline_id = "test-pipeline-id"
    pipeline = Pipeline(pipeline_id=pipeline_id)
    
    with patch("httpx.Client.delete", return_value=mock_success_response) as mock_delete:
        pipeline.delete()
        mock_delete.assert_called_once_with(f"{pipeline.ENDPOINT}/{pipeline_id}")


def test_pipeline_delete_without_id(mock_success_response):
    """Test Pipeline delete without explicit pipeline_id (legacy behavior)."""
    pipeline = Pipeline()
    
    with patch("httpx.Client.delete", return_value=mock_success_response) as mock_delete:
        pipeline.delete()
        mock_delete.assert_called_once_with(f"{pipeline.ENDPOINT}/shutdown")