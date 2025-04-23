from enum import Enum
from typing import Any, List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator


class CaseInsensitiveStrEnum(str, Enum):
    @classmethod
    def _missing_(cls, value):
        for member in cls:
            if member.value.lower() == value.lower():
                return member
        raise ValueError(f"Invalid value: {value}")

    def __str__(self):
        return str(self.value)


class KafkaProtocol(CaseInsensitiveStrEnum):
    SASL_SSL = "SASL_SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    PLAINTEXT = "PLAINTEXT"


class KafkaMechanism(CaseInsensitiveStrEnum):
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    PLAIN = "PLAIN"


class SchemaFieldType(CaseInsensitiveStrEnum):
    STRING = "string"
    INT = "int"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    UINT = "uint"
    UINT8 = "uint8"
    UINT16 = "uint16"
    UINT32 = "uint32"
    UINT64 = "uint64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    BOOL = "bool"


class SchemaField(BaseModel):
    name: str
    type: SchemaFieldType


class SchemaType(CaseInsensitiveStrEnum):
    JSON = "json"


class Schema(BaseModel):
    type: SchemaType = SchemaType.JSON
    fields: List[SchemaField]


class DeduplicationConfig(BaseModel):
    enabled: bool
    id_field: Optional[str] = Field(default=None)
    id_field_type: Optional[SchemaFieldType] = Field(default=None)
    time_window: Optional[str] = Field(default=None)

    @field_validator("id_field", "id_field_type", "time_window")
    @classmethod
    def validate_required_fields(cls, v: Any, info: ValidationInfo) -> Any:
        if info.data.get("enabled", False):
            if v is None:
                raise ValueError(
                    f"{info.field_name} is required when deduplication is enabled"
                )
        return v


class ConsumerGroupOffset(CaseInsensitiveStrEnum):
    LATEST = "latest"
    EARLIEST = "earliest"


class TopicConfig(BaseModel):
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.EARLIEST
    name: str
    event_schema: Schema = Field(alias="schema")
    deduplication: DeduplicationConfig

    @field_validator("deduplication")
    @classmethod
    def validate_deduplication_id_field(
        cls, v: DeduplicationConfig, info: ValidationInfo
    ) -> DeduplicationConfig:
        """
        Validate that the deduplication ID field exists in the
        schema and has matching type.
        """
        if not v.enabled:
            return v

        # Get the schema from the parent model's data
        schema = info.data.get("event_schema", {})
        if isinstance(schema, dict):
            fields = schema.get("fields", [])
        else:
            fields = schema.fields

        # Find the field in the schema
        field = next((f for f in fields if f.name == v.id_field), None)
        if not field:
            raise ValueError(
                f"Deduplication ID field '{v.id_field}' does not exist in "
                "the event schema"
            )

        # Check if the field type matches the deduplication ID field type
        if field.type.value != v.id_field_type.value:
            raise ValueError(
                f"Deduplication ID field type '{v.id_field_type.value}' does not match "
                f"schema field type '{field.type.value}' for field '{v.id_field}'"
            )

        return v


class KafkaConnectionParams(BaseModel):
    brokers: List[str]
    protocol: KafkaProtocol
    mechanism: KafkaMechanism
    username: str
    password: str
    root_ca: Optional[str] = Field(default=None)


class SourceType(CaseInsensitiveStrEnum):
    KAFKA = "kafka"


class SourceConfig(BaseModel):
    type: SourceType = SourceType.KAFKA
    provider: Optional[str] = Field(default=None)
    connection_params: KafkaConnectionParams
    topics: List[TopicConfig]


class JoinOrientation(CaseInsensitiveStrEnum):
    LEFT = "left"
    RIGHT = "right"


class JoinSourceConfig(BaseModel):
    source_id: str
    join_key: str
    time_window: str
    orientation: JoinOrientation


class JoinType(CaseInsensitiveStrEnum):
    TEMPORAL = "temporal"


class JoinConfig(BaseModel):
    """Configuration for joining multiple sources."""

    enabled: bool = False
    type: Optional[JoinType] = None
    sources: Optional[List[JoinSourceConfig]] = None

    @field_validator("sources")
    @classmethod
    def validate_sources(
        cls, v: Optional[List[JoinSourceConfig]], info: ValidationInfo
    ) -> Optional[List[JoinSourceConfig]]:
        """
        Validate that when join is enabled, there are exactly two sources
        with opposite orientations.
        """
        if not info.data.get("enabled", False):
            return v

        if not v:
            raise ValueError("sources are required when join is enabled")

        if len(v) != 2:
            raise ValueError("join must have exactly two sources when enabled")

        orientations = {source.orientation for source in v}
        if orientations != {JoinOrientation.LEFT, JoinOrientation.RIGHT}:
            raise ValueError(
                "join sources must have opposite orientations (one LEFT and one RIGHT)"
            )

        return v

    @field_validator("type")
    @classmethod
    def validate_type(
        cls, v: Optional[JoinType], info: ValidationInfo
    ) -> Optional[JoinType]:
        """Validate that type is required when join is enabled."""
        if info.data.get("enabled", False) and not v:
            raise ValueError("type is required when join is enabled")
        return v


class ClickhouseDataType(CaseInsensitiveStrEnum):
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"
    DECIMAL32 = "Decimal(32)"
    DECIMAL64 = "Decimal(64)"
    DECIMAL128 = "Decimal(128)"
    DECIMAL256 = "Decimal(256)"
    STRING = "String"
    FIXEDSTRING = "FixedString"
    DATE = "Date"
    DATE32 = "Date32"
    DATETIME = "DateTime"
    DATETIME64 = "DateTime64"
    BOOL = "Bool"
    ARRAY = "Array"
    NULLABLE = "Nullable"
    LOWCARDINALITY = "LowCardinality"
    UUID = "UUID"
    IPV4 = "IPv4"
    IPV6 = "IPv6"
    JSON = "JSON"
    MAP = "Map"
    TUPLE = "Tuple"
    NESTED = "Nested"
    AGGREGATEFUNCTION = "AggregateFunction"
    SIMPLEAGGREGATEFUNCTION = "SimpleAggregateFunction"
    ENUM8 = "Enum8"
    ENUM16 = "Enum16"
    POINT = "Point"
    RING = "Ring"
    POLYGON = "Polygon"
    MULTIPOLYGON = "MultiPolygon"


class TableMapping(BaseModel):
    source_id: str
    field_name: str
    column_name: str
    column_type: ClickhouseDataType


class SinkType(CaseInsensitiveStrEnum):
    CLICKHOUSE = "clickhouse"


class SinkConfig(BaseModel):
    type: SinkType = SinkType.CLICKHOUSE
    provider: Optional[str] = Field(default=None)
    host: str
    port: str
    database: str
    username: str
    password: str
    secure: bool = Field(default=False)
    max_batch_size: int = Field(default=1000)
    max_delay_time: str = Field(default="10m")
    table: str
    table_mapping: List[TableMapping]


class PipelineConfig(BaseModel):
    pipeline_id: str
    source: SourceConfig
    join: Optional[JoinConfig] = Field(default=None)
    sink: SinkConfig

    @field_validator("pipeline_id")
    @classmethod
    def validate_pipeline_id(cls, v: str) -> str:
        if not v:
            raise ValueError("pipeline_id cannot be empty")
        return v

    @field_validator("join")
    @classmethod
    def validate_join_config(
        cls,
        v: Optional[JoinConfig],
        info: Any,
    ) -> Optional[JoinConfig]:
        if not v or not v.enabled:
            return v

        # Get the source topics from the parent model's data
        source = info.data.get("source", {})
        if isinstance(source, dict):
            source_topics = source.get("topics", [])
        else:
            source_topics = source.topics
        if not source_topics:
            return v

        # Validate each source in the join config
        for source in v.sources:
            # Check if source_id exists in any topic
            source_exists = any(
                topic.name == source.source_id for topic in source_topics
            )
            if not source_exists:
                raise ValueError(
                    f"Source ID '{source.source_id}' does not exist in any topic"
                )

            # Find the topic and check if join_key exists in its schema
            topic = next((t for t in source_topics if t.name == source.source_id), None)
            if not topic:
                continue

            field_exists = any(
                field.name == source.join_key for field in topic.event_schema.fields
            )
            if not field_exists:
                raise ValueError(
                    f"Join key '{source.join_key}' does not exist in source "
                    f"'{source.source_id}' schema"
                )

        return v

    @field_validator("sink")
    @classmethod
    def validate_sink_config(cls, v: SinkConfig, info: Any) -> SinkConfig:
        # Get the source topics from the parent model's data
        source = info.data.get("source", {})
        if isinstance(source, dict):
            source_topics = source.get("topics", [])
        else:
            source_topics = source.topics
        if not source_topics:
            return v

        # Validate each table mapping
        for mapping in v.table_mapping:
            # Check if source_id exists in any topic
            source_exists = any(
                topic.name == mapping.source_id for topic in source_topics
            )
            if not source_exists:
                raise ValueError(
                    f"Source ID '{mapping.source_id}' does not exist in any topic"
                )

            # Find the topic and check if field_name exists in its schema
            topic = next(
                (t for t in source_topics if t.name == mapping.source_id), None
            )
            if not topic:
                continue

            field_exists = any(
                field.name == mapping.field_name for field in topic.event_schema.fields
            )
            if not field_exists:
                raise ValueError(
                    f"Field '{mapping.field_name}' does not exist in source "
                    f"'{mapping.source_id}' event schema"
                )

        return v
