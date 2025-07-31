"""Mock response test data."""

from unittest.mock import MagicMock

import httpx

from tests.data.pipeline_configs import get_valid_pipeline_config


def create_mock_success_response():
    """Create a mock successful HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = {"status": "success"}
    return mock_response


def create_mock_not_found_response():
    """Create a mock 404 HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 404
    mock_response.text = "Pipeline not found"
    mock_response.json.return_value = {"message": "not found"}
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Not Found", request=MagicMock(), response=mock_response
    )
    return mock_response


def create_mock_forbidden_response():
    """Create a mock 403 HTTP response."""
    valid_config = get_valid_pipeline_config()
    mock_response = MagicMock()
    mock_response.status_code = 403
    mock_response.text = (
        f"Pipeline with id {valid_config['pipeline_id']} already active"
    )
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Forbidden", request=MagicMock(), response=mock_response
    )
    return mock_response


def create_mock_bad_request_response():
    """Create a mock 400 HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 400
    mock_response.text = "Bad request"
    mock_response.json.return_value = {"message": "Bad request"}
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Bad Request", request=MagicMock(), response=mock_response
    )
    return mock_response


def create_mock_server_error_response():
    """Create a mock 500 HTTP response."""
    mock_response = MagicMock()
    mock_response.status_code = 500
    mock_response.text = "Internal server error"
    mock_response.raise_for_status.side_effect = httpx.HTTPStatusError(
        "Internal Server Error", request=MagicMock(), response=mock_response
    )
    return mock_response


def create_mock_connection_error():
    """Create a mock connection error."""
    return httpx.ConnectError("Connection failed")


def create_mock_success_get_pipeline():
    """Create a mock successful pipeline GET response."""
    valid_config = get_valid_pipeline_config()
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = valid_config
    return mock_response


def create_mock_pipeline_list_response():
    """Create a mock successful pipeline list response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {
            "pipeline_id": "loadtest",
            "name": "loadtest",
            "transformation_type": "Deduplication",
            "created_at": "2025-07-28T11:50:05.478766129Z",
            "state": "",
        },
        {
            "pipeline_id": "loadtest-4",
            "name": "loadtest-4",
            "transformation_type": "Ingest Only",
            "created_at": "2025-07-28T11:52:53.210108151Z",
            "state": "",
        },
        {
            "pipeline_id": "loadtest-5",
            "name": "loadtest-5",
            "transformation_type": "Join",
            "created_at": "2025-07-28T11:54:46.270842151Z",
            "state": "",
        },
    ]
    return mock_response


def create_mock_single_pipeline_response():
    """Create a mock response with a single pipeline."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = [
        {
            "pipeline_id": "single-pipeline",
            "name": "single-pipeline",
            "transformation_type": "Deduplication",
            "created_at": "2025-07-28T11:50:05.478766129Z",
            "state": "",
        }
    ]
    return mock_response


def create_mock_empty_pipeline_response():
    """Create a mock response with no pipelines."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.raise_for_status.return_value = None
    mock_response.json.return_value = []
    return mock_response


def create_mock_dlq_consume_response():
    """Create a mock DLQ consume response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = [
        {"id": "msg1", "content": "test message 1"},
        {"id": "msg2", "content": "test message 2"},
    ]
    return mock_response


def create_mock_dlq_empty_response():
    """Create a mock empty DLQ response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = []
    return mock_response


def create_mock_dlq_state_response():
    """Create a mock DLQ state response."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "total_messages": 10,
        "processed_messages": 5,
        "failed_messages": 2,
    }
    return mock_response
