from typing import List, Optional

from pydantic import BaseModel, Field

from .base import CaseInsensitiveStrEnum
from .data_types import ClickhouseDataType


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
    skip_certificate_verification: bool = Field(default=False)
    max_batch_size: int = Field(default=1000)
    max_delay_time: str = Field(default="10m")
    table: str
    table_mapping: List[TableMapping]
