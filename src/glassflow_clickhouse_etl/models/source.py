from typing import Any, List, Optional

from pydantic import BaseModel, Field, ValidationInfo, field_validator

from .base import CaseInsensitiveStrEnum
from .data_types import KafkaDataType


class KafkaProtocol(CaseInsensitiveStrEnum):
    SSL = "SSL"
    SASL_SSL = "SASL_SSL"
    SASL_PLAINTEXT = "SASL_PLAINTEXT"
    PLAINTEXT = "PLAINTEXT"


class KafkaMechanism(CaseInsensitiveStrEnum):
    SCRAM_SHA_256 = "SCRAM-SHA-256"
    SCRAM_SHA_512 = "SCRAM-SHA-512"
    PLAIN = "PLAIN"


class SchemaField(BaseModel):
    name: str
    type: KafkaDataType


class SchemaType(CaseInsensitiveStrEnum):
    JSON = "json"


class Schema(BaseModel):
    type: SchemaType = SchemaType.JSON
    fields: List[SchemaField]


class DeduplicationConfig(BaseModel):
    enabled: bool = False
    id_field: Optional[str] = Field(default=None)
    id_field_type: Optional[KafkaDataType] = Field(default=None)
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

    @field_validator("id_field_type")
    @classmethod
    def validate_id_field_type(
        cls, v: KafkaDataType, info: ValidationInfo
    ) -> KafkaDataType:
        if info.data.get("enabled", False):
            if v not in [
                KafkaDataType.STRING,
                KafkaDataType.INT32,
                KafkaDataType.INT64,
            ]:
                raise ValueError(
                    f"{info.field_name} must be a string, int32, or int64 when "
                    "deduplication is enabled"
                )
        return v


class ConsumerGroupOffset(CaseInsensitiveStrEnum):
    LATEST = "latest"
    EARLIEST = "earliest"


class TopicConfig(BaseModel):
    consumer_group_initial_offset: ConsumerGroupOffset = ConsumerGroupOffset.EARLIEST
    name: str
    event_schema: Schema = Field(alias="schema")
    deduplication: Optional[DeduplicationConfig] = Field(default=DeduplicationConfig())

    @field_validator("deduplication")
    @classmethod
    def validate_deduplication_id_field(
        cls, v: DeduplicationConfig, info: ValidationInfo
    ) -> DeduplicationConfig:
        """
        Validate that the deduplication ID field exists in the
        schema and has matching type.
        """
        if v is None or not v.enabled:
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
    mechanism: Optional[KafkaMechanism] = Field(default=None)
    username: Optional[str] = Field(default=None)
    password: Optional[str] = Field(default=None)
    root_ca: Optional[str] = Field(default=None)
    skip_auth: bool = Field(default=False)


class SourceType(CaseInsensitiveStrEnum):
    KAFKA = "kafka"


class SourceConfig(BaseModel):
    type: SourceType = SourceType.KAFKA
    provider: Optional[str] = Field(default=None)
    connection_params: KafkaConnectionParams
    topics: List[TopicConfig]
