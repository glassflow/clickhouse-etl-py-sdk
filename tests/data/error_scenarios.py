"""Error scenario test data."""

import httpx

from glassflow_clickhouse_etl import errors


def get_validation_error_scenarios():
    """Get validation error test scenarios."""
    return [
        {
            "name": "empty_pipeline_id",
            "config": {"pipeline_id": ""},
            "expected_error": ValueError,
            "error_message": "pipeline_id cannot be empty",
        },
        {
            "name": "invalid_pipeline_id_format",
            "config": {"pipeline_id": "Test_Pipeline"},
            "expected_error": ValueError,
            "error_message": (
                "pipeline_id can only contain lowercase letters, numbers, and hyphens"
            ),
        },
        {
            "name": "pipeline_id_too_long",
            "config": {
                "pipeline_id": "test-pipeline-1234567890123456789012345678901234567890"
            },
            "expected_error": ValueError,
            "error_message": "pipeline_id cannot be longer than 40 characters",
        },
        {
            "name": "pipeline_id_starts_with_hyphen",
            "config": {"pipeline_id": "-test-pipeline"},
            "expected_error": ValueError,
            "error_message": "pipeline_id must start with a lowercase letter",
        },
        {
            "name": "pipeline_id_ends_with_hyphen",
            "config": {"pipeline_id": "test-pipeline-"},
            "expected_error": ValueError,
            "error_message": "pipeline_id must end with a lowercase letter",
        },
    ]


def get_http_error_scenarios():
    """Get HTTP error test scenarios."""
    return [
        {
            "name": "not_found",
            "status_code": 404,
            "text": "Pipeline not found",
            "expected_error": errors.PipelineNotFoundError,
            "error_message": "not found",
        },
        {
            "name": "forbidden",
            "status_code": 403,
            "text": "Pipeline already active",
            "expected_error": errors.PipelineAlreadyExistsError,
            "error_message": "already active",
        },
        {
            "name": "bad_request",
            "status_code": 400,
            "text": "Bad request",
            "expected_error": errors.ValidationError,
            "error_message": "Bad request",
        },
        {
            "name": "server_error",
            "status_code": 500,
            "text": "Internal server error",
            "expected_error": errors.ServerError,
            "error_message": "Internal server error",
        },
    ]


def get_connection_error_scenarios():
    """Get connection error test scenarios."""
    return [
        {
            "name": "connection_timeout",
            "exception": httpx.ConnectTimeout("Connection timeout"),
            "expected_error": errors.ConnectionError,
            "error_message": "Failed to connect to GlassFlow ETL API",
        },
        {
            "name": "connection_refused",
            "exception": httpx.ConnectError("Connection refused"),
            "expected_error": errors.ConnectionError,
            "error_message": "Failed to connect to GlassFlow ETL API",
        },
        {
            "name": "network_unreachable",
            "exception": httpx.NetworkError("Network unreachable"),
            "expected_error": errors.ConnectionError,
            "error_message": "Failed to connect to GlassFlow ETL API",
        },
    ]


def get_dlq_error_scenarios():
    """Get DLQ error test scenarios."""
    return [
        {
            "name": "invalid_batch_size_negative",
            "batch_size": -1,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_zero",
            "batch_size": 0,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_too_large",
            "batch_size": 101,
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "invalid_batch_size_non_integer",
            "batch_size": "invalid",
            "expected_error": ValueError,
            "error_message": "batch_size must be an integer between 1 and 100",
        },
        {
            "name": "validation_error_422",
            "status_code": 422,
            "text": "Invalid batch size",
            "expected_error": errors.InvalidBatchSizeError,
            "error_message": "Invalid batch size",
        },
    ]


def get_join_validation_error_scenarios():
    """Get join validation error test scenarios."""
    return [
        {
            "name": "source_id_not_found",
            "source_id": "non-existent-topic",
            "expected_error": ValueError,
            "error_message": "does not exist in any topic",
        },
        {
            "name": "join_key_not_found",
            "join_key": "non-existent-field",
            "expected_error": ValueError,
            "error_message": "does not exist in source",
        },
        {
            "name": "same_orientation",
            "orientation1": "left",
            "orientation2": "left",
            "expected_error": ValueError,
            "error_message": "join sources must have opposite orientations",
        },
    ]


def get_sink_validation_error_scenarios():
    """Get sink validation error test scenarios."""
    return [
        {
            "name": "source_id_not_found",
            "source_id": "non-existent-topic",
            "expected_error": ValueError,
            "error_message": "does not exist in any topic",
        },
        {
            "name": "field_name_not_found",
            "field_name": "non-existent-field",
            "expected_error": ValueError,
            "error_message": "does not exist in source",
        },
    ]
