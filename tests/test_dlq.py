"""Tests for DLQ (Dead Letter Queue) functionality."""

from unittest.mock import Mock, patch

import httpx
import pytest

from glassflow_clickhouse_etl import DLQ, Pipeline, errors


class TestDLQ:
    """Test cases for DLQ class."""

    def test_dlq_initialization(self):
        """Test DLQ initialization."""
        dlq = DLQ(url="http://localhost:8080")
        assert dlq.client.base_url == "http://localhost:8080"
        assert dlq.ENDPOINT == "/api/v1/dlq"

    def test_consume_success(self):
        """Test successful DLQ consume operation."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [
            {"id": "msg1", "content": "test message 1"},
            {"id": "msg2", "content": "test message 2"}
        ]

        with patch("httpx.Client.get", return_value=mock_response) as mock_get:
            result = dlq.consume(batch_size=50)

            mock_get.assert_called_once_with(
                "/api/v1/dlq/consume",
                params={"batch_size": 50}
            )
            assert result == [
                {"id": "msg1", "content": "test message 1"},
                {"id": "msg2", "content": "test message 2"}
            ]

    def test_consume_default_batch_size(self):
        """Test DLQ consume with default batch size."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = []

        with patch("httpx.Client.get", return_value=mock_response) as mock_get:
            result = dlq.consume()

            mock_get.assert_called_once_with(
                "/api/v1/dlq/consume",
                params={"batch_size": 100}
            )
            assert result == []

    def test_consume_invalid_batch_size_negative(self):
        """Test DLQ consume with negative batch size."""
        dlq = DLQ()

        with pytest.raises(ValueError) as exc_info:
            dlq.consume(batch_size=-1)

        assert "batch_size must be an integer between 1 and 1000" in str(exc_info.value)

    def test_consume_invalid_batch_size_zero(self):
        """Test DLQ consume with zero batch size."""
        dlq = DLQ()

        with pytest.raises(ValueError) as exc_info:
            dlq.consume(batch_size=0)

        assert "batch_size must be an integer between 1 and 1000" in str(exc_info.value)

    def test_consume_invalid_batch_size_too_large(self):
        """Test DLQ consume with batch size too large."""
        dlq = DLQ()

        with pytest.raises(ValueError) as exc_info:
            dlq.consume(batch_size=1001)

        assert "batch_size must be an integer between 1 and 1000" in str(exc_info.value)

    def test_consume_invalid_batch_size_non_integer(self):
        """Test DLQ consume with non-integer batch size."""
        dlq = DLQ()

        with pytest.raises(ValueError) as exc_info:
            dlq.consume(batch_size="invalid")

        assert "batch_size must be an integer between 1 and 1000" in str(exc_info.value)

    def test_consume_422_error(self):
        """Test DLQ consume with 422 validation error."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 422
        mock_response.text = "Invalid batch size"

        mock_error = httpx.HTTPStatusError(
            "422 Unprocessable Entity",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.get", side_effect=mock_error):
            with pytest.raises(ValueError) as exc_info:
                dlq.consume(batch_size=50)

            assert "API error: Invalid batch size" in str(exc_info.value)

    def test_consume_server_error(self):
        """Test DLQ consume with server error."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.get", side_effect=mock_error):
            with pytest.raises(errors.InternalServerError) as exc_info:
                dlq.consume(batch_size=50)

            assert "API request failed: Internal server error" in str(exc_info.value)

    def test_consume_connection_error(self):
        """Test DLQ consume with connection error."""
        dlq = DLQ()
        mock_error = httpx.ConnectError("Connection failed")

        with patch("httpx.Client.get", side_effect=mock_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                dlq.consume(batch_size=50)

            assert "Failed to connect to DLQ service" in str(exc_info.value)

    def test_state_success(self):
        """Test successful DLQ state operation."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "total_messages": 42,
            "pending_messages": 5,
            "last_updated": "2023-01-01T00:00:00Z"
        }

        with patch("httpx.Client.get", return_value=mock_response) as mock_get:
            result = dlq.state()

            mock_get.assert_called_once_with("/api/v1/dlq/state")
            assert result == {
                "total_messages": 42,
                "pending_messages": 5,
                "last_updated": "2023-01-01T00:00:00Z"
            }

    def test_state_server_error(self):
        """Test DLQ state with server error."""
        dlq = DLQ()
        mock_response = Mock()
        mock_response.status_code = 500
        mock_response.text = "Internal server error"

        mock_error = httpx.HTTPStatusError(
            "500 Internal Server Error",
            request=Mock(),
            response=mock_response
        )

        with patch("httpx.Client.get", side_effect=mock_error):
            with pytest.raises(errors.InternalServerError) as exc_info:
                dlq.state()

            assert "API request failed: Internal server error" in str(exc_info.value)

    def test_state_connection_error(self):
        """Test DLQ state with connection error."""
        dlq = DLQ()
        mock_error = httpx.ConnectError("Connection failed")

        with patch("httpx.Client.get", side_effect=mock_error):
            with pytest.raises(errors.ConnectionError) as exc_info:
                dlq.state()

            assert "Failed to connect to DLQ service" in str(exc_info.value)


class TestPipelineDLQIntegration:
    """Test cases for Pipeline-DLQ integration."""

    def test_pipeline_dlq_property(self):
        """Test that Pipeline has a DLQ property."""
        pipeline = Pipeline()

        assert hasattr(pipeline, "dlq")
        assert isinstance(pipeline.dlq, DLQ)

    def test_pipeline_dlq_property_same_url(self):
        """Test that Pipeline DLQ uses the same base URL."""
        custom_url = "http://custom-url:9000"
        pipeline = Pipeline(url=custom_url)

        assert pipeline.client.base_url == custom_url
        assert pipeline.dlq.client.base_url == custom_url

    def test_pipeline_dlq_consume_integration(self):
        """Test Pipeline DLQ consume functionality."""
        pipeline = Pipeline()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = [{"id": "msg1", "content": "test"}]

        with patch("httpx.Client.get", return_value=mock_response):
            result = pipeline.dlq.consume(batch_size=10)

            assert result == [{"id": "msg1", "content": "test"}]

    def test_pipeline_dlq_state_integration(self):
        """Test Pipeline DLQ state functionality."""
        pipeline = Pipeline()
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"total_messages": 10}

        with patch("httpx.Client.get", return_value=mock_response):
            result = pipeline.dlq.state()

            assert result == {"total_messages": 10}
