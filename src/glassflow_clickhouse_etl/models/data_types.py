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
    UINT8 = "UInt8"
    UINT16 = "UInt16"
    UINT32 = "UInt32"
    UINT64 = "UInt64"
    FLOAT32 = "Float32"
    FLOAT64 = "Float64"
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
