import pytest

from glassflow_clickhouse_etl import models


class TestJoinConfig:
    """Tests for JoinConfig model."""

    def test_join_config_enabled_true(self):
        """Test JoinConfig when enabled is True."""
        with pytest.raises(ValueError) as exc_info:
            models.JoinConfig(
                enabled=True,
                type=None,
                sources=None,
            )
        assert "type is required when join is enabled" in str(exc_info.value)

        # Test with only one source
        with pytest.raises(ValueError) as exc_info:
            models.JoinConfig(
                enabled=True,
                type=models.JoinType.TEMPORAL,
                sources=[
                    models.JoinSourceConfig(
                        source_id="test-topic",
                        join_key="id",
                        time_window="1h",
                        orientation=models.JoinOrientation.LEFT,
                    )
                ],
            )
        assert "join must have exactly two sources when enabled" in str(exc_info.value)

        # Test with two sources but same orientation
        with pytest.raises(ValueError) as exc_info:
            models.JoinConfig(
                enabled=True,
                type=models.JoinType.TEMPORAL,
                sources=[
                    models.JoinSourceConfig(
                        source_id="test-topic-1",
                        join_key="id",
                        time_window="1h",
                        orientation=models.JoinOrientation.LEFT,
                    ),
                    models.JoinSourceConfig(
                        source_id="test-topic-2",
                        join_key="id",
                        time_window="1h",
                        orientation=models.JoinOrientation.LEFT,
                    ),
                ],
            )
        assert "join sources must have opposite orientations" in str(exc_info.value)

        # Test with valid configuration
        config = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id="test-topic-1",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
                models.JoinSourceConfig(
                    source_id="test-topic-2",
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.RIGHT,
                ),
            ],
        )
        assert config.enabled is True
        assert config.type == models.JoinType.TEMPORAL
        assert len(config.sources) == 2
        assert config.sources[0].orientation == models.JoinOrientation.LEFT
        assert config.sources[1].orientation == models.JoinOrientation.RIGHT

    def test_join_config_enabled_false(self):
        """Test JoinConfig when enabled is False."""
        # All fields should be optional when enabled is False
        config = models.JoinConfig(
            enabled=False,
            type=None,
            sources=None,
        )
        assert config.enabled is False
        assert config.type is None
        assert config.sources is None

    def test_validate_join_config_source_id_not_found(self, valid_config):
        """Test join config validation when source_id does not exist in topics."""
        join = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id="non-existent-topic",  # This topic doesn't exist
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
                models.JoinSourceConfig(
                    source_id=valid_config["source"]["topics"][1]["name"],
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.RIGHT,
                ),
            ],
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                join=join,
                sink=valid_config["sink"],
            )
        assert "does not exist in any topic" in str(exc_info.value)

    def test_validate_join_config_join_key_not_found(self, valid_config):
        """Test join config validation when join_key does not exist in schema."""
        join = models.JoinConfig(
            enabled=True,
            type=models.JoinType.TEMPORAL,
            sources=[
                models.JoinSourceConfig(
                    source_id=valid_config["source"]["topics"][0]["name"],
                    join_key="non-existent-field",  # This field doesn't exist
                    time_window="1h",
                    orientation=models.JoinOrientation.LEFT,
                ),
                models.JoinSourceConfig(
                    source_id=valid_config["source"]["topics"][1]["name"],
                    join_key="id",
                    time_window="1h",
                    orientation=models.JoinOrientation.RIGHT,
                ),
            ],
        )

        with pytest.raises(ValueError) as exc_info:
            models.PipelineConfig(
                pipeline_id="test-pipeline",
                source=valid_config["source"],
                join=join,
                sink=valid_config["sink"],
            )
        assert "does not exist in source" in str(exc_info.value)
        assert "schema" in str(exc_info.value)
