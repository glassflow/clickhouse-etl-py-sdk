from .base import CaseInsensitiveStrEnum


class KafkaDataType(CaseInsensitiveStrEnum):
    STRING = "string"
    INT8 = "int8"
    INT16 = "int16"
    INT32 = "int32"
    INT64 = "int64"
    FLOAT32 = "float32"
    FLOAT64 = "float64"
    BOOL = "bool"
    BYTES = "bytes"


class ClickhouseDataType(CaseInsensitiveStrEnum):
    INT8 = "Int8"
    INT16 = "Int16"
    INT32 = "Int32"
    INT64 = "Int64"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
    STRING = "String"
    FIXEDSTRING = "FixedString"
    DATETIME = "DateTime"
    DATETIME64 = "DateTime64"
    BOOL = "Bool"
    UUID = "UUID"
    ENUM8 = "Enum8"
    ENUM16 = "Enum16"


kafka_to_clickhouse_data_type_mappings = {
    KafkaDataType.STRING: [
        ClickhouseDataType.STRING,
        ClickhouseDataType.FIXEDSTRING,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
        ClickhouseDataType.UUID,
        ClickhouseDataType.ENUM8,
        ClickhouseDataType.ENUM16,
    ],
    KafkaDataType.INT8: [ClickhouseDataType.INT8],
    KafkaDataType.INT16: [ClickhouseDataType.INT16],
    KafkaDataType.INT32: [ClickhouseDataType.INT32],
    KafkaDataType.INT64: [
        ClickhouseDataType.INT64,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
    ],
    KafkaDataType.FLOAT32: [ClickhouseDataType.FLOAT32],
    KafkaDataType.FLOAT64: [
        ClickhouseDataType.FLOAT64,
        ClickhouseDataType.DATETIME,
        ClickhouseDataType.DATETIME64,
    ],
    KafkaDataType.BOOL: [ClickhouseDataType.BOOL],
    KafkaDataType.BYTES: [ClickhouseDataType.STRING],
}
