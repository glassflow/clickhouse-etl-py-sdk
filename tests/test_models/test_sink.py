import pytest

from glassflow_clickhouse_etl import models


class TestSinkConfig:
    """Tests for SinkConfig validation."""

    def test_validate_sink_config_source_id_not_found(self, valid_config):
        """Test sink config validation when source_id does not exist in topics."""
        sink = valid_config["sink"]
        sink["table_mapping"] = [
            models.TableMapping(
                source_id="non-existent-topic",  # This topic doesn't exist
                field_name="id",
                column_name="id",
                column_type="String",
            )
        ]
        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                sink=sink,
            )
        assert "does not exist in any topic" in str(exc_info.value)

    def test_validate_sink_config_field_name_not_found(self, valid_config):
        """Test sink config validation when field_name does not exist in schema."""
        sink = valid_config["sink"]
        sink["table_mapping"] = [
            models.TableMapping(
                source_id=valid_config["source"]["topics"][0]["name"],
                field_name="non-existent-field",  # This field doesn't exist
                column_name="id",
                column_type="String",
            )
        ]
        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                sink=sink,
            )
        assert "does not exist in source" in str(exc_info.value)
        assert "event schema" in str(exc_info.value)
