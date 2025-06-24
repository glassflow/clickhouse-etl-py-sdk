"""
GlassFlow SDK for creating data pipelines between Kafka and ClickHouse.
"""

from .client import Client
from .models import JoinConfig, PipelineConfig, SinkConfig, SourceConfig
from .pipeline import Pipeline
from .pipeline_manager import PipelineManager

__version__ = "0.1.0"
__all__ = ["Client", "Pipeline", "PipelineManager", "PipelineConfig", "SourceConfig", "SinkConfig", "JoinConfig"]
