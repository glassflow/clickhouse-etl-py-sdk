# Clickhouse ETL Python SDK

<!-- Pytest Coverage Comment:Begin -->
![Coverage](https://img.shields.io/badge/coverage-95%25-brightgreen)
<!-- Pytest Coverage Comment:End -->

A Python SDK for creating and managing data pipelines between Kafka and ClickHouse.

## Features

- Create and manage data pipelines between Kafka and ClickHouse
- Deduplication of events during a time window based on a key
- Temporal joins between topics based on a common key with a given time window
- Schema validation and configuration management

## Installation

```bash
pip install glassflow-clickhouse-etl
```

## Quick Start

```python
from glassflow_clickhouse_etl import Pipeline


pipeline_config = {
  "pipeline_id": "test-pipeline",
  "source": {
    "type": "kafka",
    "provider": "aiven",
    "connection_params": {
      "brokers": ["localhoust:9092"],
      "protocol": "SASL_SSL",
      "mechanism": "SCRAM-SHA-256",
      "username": "user",
      "password": "pass"
    }
    "topics": [
      {
        "consumer_group_initial_offset": "earliest",
        "id": "test-topic",
        "name": "test-topic",
        "schema": {
          "type": "json",
          "fields": [
            {"name": "id", "type": "string" },
            {"name": "email", "type": "string"}
          ]
        },
        "deduplication": {
          "id_field": "id",
          "id_field_type": "string",
          "time_window": "1h",
          "enabled": True
        }
      }
    ],
  },
  "sink": {
    "type": "clickhouse",
    "host": "localhost:8443",
    "port": 8443,
    "database": "test",
    "username": "default",
    "password": "pass",
    "table_mapping": [
      {
        "source_id": "test_table",
        "field_name": "id",
        "column_name": "user_id",
        "column_type": "UUID"
      },
      {
        "source_id": "test_table",
        "field_name": "email",
        "column_name": "email",
        "column_type": "String"
      }
    ]
  }
}

# Create a pipeline from a JSON configuration
pipeline = Pipeline(pipeline_config)

# Create the pipeline
pipeline.create()
```

## Configuration

For detailed information about the pipeline configuration, see [CONFIGURATION](CONFIGURATION.md).

## Development

### Setup

1. Clone the repository
2. Create a virtual environment
3. Install dependencies:

```bash
uv venv
source .venv/bin/activate
uv pip install -e .[dev]
```

### Testing

```bash
pytest
```
