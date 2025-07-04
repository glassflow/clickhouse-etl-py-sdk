"""Tests for DLQ (Dead Letter Queue) functionality."""

from unittest.mock import Mock, patch

import httpx
import pytest

from glassflow_clickhouse_etl import DLQ, Pipeline, errors


class TestDLQ:
    """Test cases for DLQ class."""

    def test_dlq_initialization(self, dlq_test):
        """Test DLQ initialization."""
        assert dlq_test.http_client.base_url == "http://localhost:8080"
        assert dlq_test.endpoint == "/api/v1/pipeline/test-pipeline/dlq"

    def test_consume_success(self, dlq_test):
        """Test successful DLQ consume operation."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "msg1", "content": "test message 1"},
            {"id": "msg2", "content": "test message 2"}
        ]

        with patch("httpx.Client.request", return_value=mock_response) as mock_get:
            result = dlq_test.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "GET",
                f"{dlq_test.endpoint}/consume",
                params={"batch_size": 50}
            )
            assert result == [
                {"id": "msg1", "content": "test message 1"},
                {"id": "msg2", "content": "test message 2"}
            ]

    def test_consume_default_batch_size(self, dlq_test):
        """Test DLQ consume with default batch size."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch("httpx.Client.request", return_value=mock_response) as mock_get:
            result = dlq_test.consume()

            mock_get.assert_called_once_with(
                "GET",
                f"{dlq_test.endpoint}/consume",
                params={"batch_size": 100}
            )
            assert result == []

    def test_consume_invalid_batch_size_negative(self, dlq_test):
        """Test DLQ consume with negative batch size."""
        with pytest.raises(ValueError) as exc_info:
            dlq_test.consume(batch_size=-1)

        assert "batch_size must be an integer between 1 and 100" in str(exc_info.value)

    def test_consume_invalid_batch_size_zero(self, dlq_test):
        """Test DLQ consume with zero batch size."""
        with pytest.raises(ValueError) as exc_info:
            dlq_test.consume(batch_size=0)

        assert "batch_size must be an integer between 1 and 100" in str(exc_info.value)

    def test_consume_invalid_batch_size_too_large(self, dlq_test):
        """Test DLQ consume with batch size too large."""
        with pytest.raises(ValueError) as exc_info:
            dlq_test.consume(batch_size=101)

        assert "batch_size must be an integer between 1 and 100" in str(exc_info.value)

    def test_consume_invalid_batch_size_non_integer(self, dlq_test):
        """Test DLQ consume with non-integer batch size."""
        with pytest.raises(ValueError) as exc_info:
            dlq_test.consume(batch_size="invalid")

        assert "batch_size must be an integer between 1 and 100" in str(exc_info.value)

    def test_consume_422_error(self, dlq_test):
        """Test DLQ consume with 422 validation error."""
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Invalid batch size"

        mock_error = httpx.HTTPStatusError(
            "422 Unprocessable Entity",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.request", side_effect=mock_error):
            with pytest.raises(errors.InvalidBatchSizeError) as exc_info:
                dlq_test.consume(batch_size=50)

            assert "Invalid batch size" in str(exc_info.value)

    def test_consume_server_error(self, dlq_test):
        """Test DLQ consume with server error."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.request", side_effect=mock_error):
            with pytest.raises(errors.ServerError) as exc_info:
                dlq_test.consume(batch_size=50)

            assert "Server error" in str(exc_info.value)

    def test_consume_connection_error(self, dlq_test):
        """Test DLQ consume with connection error."""
        mock_error = httpx.ConnectError("Connection failed")

        with patch("httpx.Client.request", side_effect=mock_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                dlq_test.consume(batch_size=50)

            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)

    def test_state_success(self, dlq_test):
        """Test successful DLQ state operation."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total_messages": 42,
            "pending_messages": 5,
            "last_updated": "2023-01-01T00:00:00Z"
        }

        with patch("httpx.Client.request", return_value=mock_response) as mock_get:
            result = dlq_test.state()

            mock_get.assert_called_once_with(
                "GET",
                f"{dlq_test.endpoint}/state"
            )
            assert result == {
                "total_messages": 42,
                "pending_messages": 5,
                "last_updated": "2023-01-01T00:00:00Z"
            }

    def test_state_server_error(self, dlq_test):
        """Test DLQ state with server error."""
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.request", side_effect=mock_error):
            with pytest.raises(errors.ServerError) as exc_info:
                dlq_test.state()

            assert "Server error" in str(exc_info.value)

    def test_state_connection_error(self, dlq_test):
        """Test DLQ state with connection error."""
        mock_error = httpx.ConnectError("Connection failed")

        with patch("httpx.Client.request", side_effect=mock_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                dlq_test.state()

            assert "Failed to connect to GlassFlow ETL API" in str(exc_info.value)


class TestPipelineDLQIntegration:
    """Test cases for Pipeline-DLQ integration."""

    def test_pipeline_dlq_property(self, pipeline_test):
        """Test that Pipeline has a DLQ property."""
        assert hasattr(pipeline_test, "dlq")
        assert isinstance(pipeline_test.dlq, DLQ)

    def test_pipeline_dlq_property_same_url(self):
        """Test that Pipeline DLQ uses the same base URL."""
        custom_url = "http://custom-url:9000"
        pipeline = Pipeline(host=custom_url, pipeline_id="test-pipeline-id")

        assert pipeline.http_client.base_url == custom_url
        assert pipeline.dlq.http_client.base_url == custom_url

    def test_pipeline_dlq_consume_integration(self, pipeline_test):
        """Test Pipeline DLQ consume functionality."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "msg1", "content": "test"}]

        with patch("httpx.Client.request", return_value=mock_response):
            result = pipeline_test.dlq.consume(batch_size=10)

            assert result == [{"id": "msg1", "content": "test"}]

    def test_pipeline_dlq_state_integration(self, pipeline_test):
        """Test Pipeline DLQ state functionality."""
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"total_messages": 10}

        with patch("httpx.Client.request", return_value=mock_response):
            result = pipeline_test.dlq.state()

            assert result == {"total_messages": 10}
