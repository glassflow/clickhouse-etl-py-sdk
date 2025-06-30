# Clickhouse ETL Python SDK

<p align="left">
  <a target="_blank" href="https://pypi.python.org/pypi/glassflow-clickhouse-etl">
    <img src="https://img.shields.io/pypi/v/glassflow-clickhouse-etl.svg?labelColor=&color=e69e3a">
  </a>
  <a target="_blank" href="https://github.com/glassflow/clickhouse-etl-py-sdk/blob/main/LICENSE">
    <img src="https://img.shields.io/pypi/l/glassflow-clickhouse-etl.svg?labelColor=&color=e69e3a">
  </a>
  <a target="_blank" href="https://pypi.python.org/pypi/glassflow-clickhouse-etl">
    <img src="https://img.shields.io/pypi/pyversions/glassflow-clickhouse-etl.svg?labelColor=&color=e69e3a">
  </a>
  <br />
  <a target="_blank" href="(https://github.com/glassflow/clickhouse-etl-py-sdk/actions">
    <img src="https://github.com/glassflow/clickhouse-etl-py-sdk/workflows/Test/badge.svg?labelColor=&color=e69e3a">
  </a>
<!-- Pytest Coverage Comment:Begin -->
  <img src=https://img.shields.io/badge/coverage-90%25-brightgreen>
<!-- Pytest Coverage Comment:End -->
</p>

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

### Initialize client

```python
from glassflow_clickhouse_etl import Client

# Initialize GlassFlow client
client = Client(host="your-glassflow-etl-url")
```

### Create a pipeline

```python
pipeline_config = {
    "pipeline_id": "deduplication-demo-pipeline",
    "source": {
      "type": "kafka",
      "provider": "confluent",
      "connection_params": {
        "brokers": [
          "kafka:9093"
        ],
        "protocol": "PLAINTEXT",
        "skip_auth": True
      },
      "topics": [
        {
          "consumer_group_initial_offset": "latest",
          "name": "users",
          "schema": {
            "type": "json",
            "fields": [
              {
                "name": "event_id",
                "type": "string"
              },
              {
                "name": "user_id",
                "type": "string"
              },
              {
                "name": "name",
                "type": "string"
              },
              {
                "name": "email",
                "type": "string"
              },
              {
                "name": "created_at",
                "type": "string"
              }
            ]
          },
          "deduplication": {
            "enabled": True,
            "id_field": "event_id",
            "id_field_type": "string",
            "time_window": "1h"
          }
        }
      ]
    },
    "join": {
      "enabled": False
    },
    "sink": {
      "type": "clickhouse",
      "provider": "localhost",
      "host": "clickhouse",
      "port": "9000",
      "database": "default",
      "username": "default",
      "password": "c2VjcmV0",
      "secure": False,
      "max_batch_size": 1000,
      "max_delay_time": "30s",
      "table": "users_dedup",
      "table_mapping": [
        {
          "source_id": "users",
          "field_name": "event_id",
          "column_name": "event_id",
          "column_type": "UUID"
        },
        {
          "source_id": "users",
          "field_name": "user_id",
          "column_name": "user_id",
          "column_type": "UUID"
        },
        {
          "source_id": "users",
          "field_name": "created_at",
          "column_name": "created_at",
          "column_type": "DateTime"
        },
        {
          "source_id": "users",
          "field_name": "name",
          "column_name": "name",
          "column_type": "String"
        },
        {
          "source_id": "users",
          "field_name": "email",
          "column_name": "email",
          "column_type": "String"
        }
      ]
    }
}

# Create a pipeline
pipeline = client.create_pipeline(pipeline_config)
```

or get an existing pipeline using the pipeline ID:

```python
# Get a pipeline by ID
pipeline = client.get_pipeline("my-pipeline-id")
```

### List pipelines

```python
pipeline_ids = client.list_pipelines()
```

### Delete pipeline

```python
# Delete a pipeline
client.delete_pipeline("my-pipeline-id")

# Or delete via pipeline instance
pipeline.delete()
```

## Pipeline Configuration

For detailed information about the pipeline configuration, see [GlassFlow docs](https://docs.glassflow.dev/pipeline/pipeline-configuration).

## Tracking

The SDK includes anonymous usage tracking to help improve the product. Tracking is enabled by default but can be disabled in two ways:

1. Using an environment variable:
```bash
export GF_TRACKING_ENABLED=false
```

2. Programmatically using the `disable_tracking` method:
```python
from glassflow_clickhouse_etl import Client

client = Client(host="my-glassflow-host")
client.disable_tracking()
```

The tracking collects anonymous information about:
- SDK version
- Platform (operating system)
- Python version
- Pipeline ID
- Whether joins or deduplication are enabled
- Kafka security protocol, auth mechanism used and whether authentication is disabled
- Errors during pipeline creation and deletion

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
