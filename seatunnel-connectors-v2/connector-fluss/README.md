# Fluss Connector

Apache Fluss connector for SeaTunnel, providing both source and sink capabilities for the Apache Fluss streaming storage system.

## Features

- **Source Connector**: Read data from Fluss tables with support for both batch and streaming modes
- **Sink Connector**: Write data to Fluss tables with configurable write modes and transaction support
- **Exactly-Once Semantics**: Support for exactly-once processing with transactional writes
- **Schema Evolution**: Automatic schema detection and type conversion
- **High Performance**: Optimized for high-throughput data processing

## Supported Versions

- Apache Fluss: 0.6.0+
- SeaTunnel: 2.3.12+

## Configuration

### Source Configuration

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap.servers` | String | Yes | - | Comma-separated list of Fluss bootstrap servers |
| `database` | String | Yes | - | Fluss database name |
| `table` | String | Yes | - | Fluss table name |
| `schema` | Config | No | - | Table schema definition (if not provided, uses simple text schema) |
| `scan.startup.mode` | String | No | earliest | Startup mode: earliest, latest, timestamp |
| `scan.startup.timestamp` | Long | No | - | Startup timestamp for timestamp mode |
| `scan.parallelism` | Integer | No | 1 | Parallelism for scanning |
| `fetch.size` | Integer | No | 1000 | Number of records to fetch in each batch |
| `poll.timeout.ms` | Long | No | 5000 | Timeout for polling records |
| `enable.changelog` | Boolean | No | false | Enable changelog mode for streaming |
| `consumer.group` | String | No | - | Consumer group ID |

### Sink Configuration

| Option | Type | Required | Default | Description |
|--------|------|----------|---------|-------------|
| `bootstrap.servers` | String | Yes | - | Comma-separated list of Fluss bootstrap servers |
| `database` | String | Yes | - | Fluss database name |
| `table` | String | Yes | - | Fluss table name |
| `write.mode` | String | No | append | Write mode: append, upsert |
| `batch.size` | Integer | No | 1000 | Number of records to batch |
| `batch.timeout.ms` | Long | No | 5000 | Maximum time to wait before flushing |
| `enable.transaction` | Boolean | No | true | Enable transactional writes |
| `enable.exactly.once` | Boolean | No | false | Enable exactly-once semantics |
| `transactional.id.prefix` | String | No | seatunnel-fluss | Prefix for transactional IDs |

## Examples

### Basic Source Example

```hocon
source {
  Fluss {
    bootstrap.servers = "localhost:9123"
    database = "test_db"
    table = "test_table"
    schema = {
      fields {
        id = "bigint"
        name = "string"
        timestamp = "bigint"
      }
    }
    scan.startup.mode = "earliest"
    fetch.size = 1000
  }
}
```

### Startup Mode Examples

```hocon
# Read from earliest records
source {
  Fluss {
    bootstrap.servers = "localhost:9123"
    database = "test_db"
    table = "test_table"
    scan.startup.mode = "earliest"
  }
}

# Read from latest records (for real-time processing)
source {
  Fluss {
    bootstrap.servers = "localhost:9123"
    database = "test_db"
    table = "test_table"
    scan.startup.mode = "latest"
  }
}

# Read from specific timestamp
source {
  Fluss {
    bootstrap.servers = "localhost:9123"
    database = "test_db"
    table = "test_table"
    scan.startup.mode = "timestamp"
    scan.startup.timestamp = 1640995200000  # 2022-01-01 00:00:00 UTC
  }
}
```

### Basic Sink Example

```hocon
sink {
  Fluss {
    bootstrap.servers = "localhost:9092"
    database = "test_db"
    table = "test_table"
    write.mode = "append"
    batch.size = 1000
    enable.transaction = true
  }
}
```

### Streaming with Exactly-Once

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  Fluss {
    bootstrap.servers = "localhost:9092"
    database = "source_db"
    table = "source_table"
    scan.startup.mode = "latest"
    enable.changelog = true
    consumer.group = "seatunnel-group"
  }
}

sink {
  Fluss {
    bootstrap.servers = "localhost:9092"
    database = "target_db"
    table = "target_table"
    write.mode = "upsert"
    enable.transaction = true
    enable.exactly.once = true
    transactional.id.prefix = "seatunnel-etl"
  }
}
```

## Data Types

The connector supports automatic conversion between SeaTunnel and Fluss data types:

| SeaTunnel Type | Fluss Type | Notes |
|----------------|------------|-------|
| BOOLEAN | BOOLEAN | - |
| TINYINT | TINYINT | - |
| SMALLINT | SMALLINT | - |
| INT | INT | - |
| BIGINT | BIGINT | - |
| FLOAT | FLOAT | - |
| DOUBLE | DOUBLE | - |
| DECIMAL | DECIMAL | Precision and scale preserved |
| STRING | STRING | - |
| BYTES | BYTES | - |
| DATE | DATE | - |
| TIME | TIME | - |
| TIMESTAMP | TIMESTAMP | - |
| ARRAY | ARRAY | Nested type conversion supported |
| MAP | MAP | Key-value type conversion supported |
| ROW | ROW | Nested row type conversion supported |

## Performance Tuning

### Source Performance

- Increase `fetch.size` for higher throughput
- Adjust `scan.parallelism` based on table buckets
- Use appropriate `poll.timeout.ms` for your use case

### Sink Performance

- Increase `batch.size` for higher throughput
- Adjust `batch.timeout.ms` to balance latency and throughput
- Consider disabling transactions for higher performance if exactly-once is not required

## Configuration Validation

The connector performs comprehensive configuration validation at startup:

### Source Configuration Validation
- **Timestamp Mode**: When using `scan.startup.mode = "timestamp"`, `scan.startup.timestamp` must be provided
- **Timestamp Value**: Must be a positive number representing milliseconds since epoch
- **Required Fields**: `bootstrap.servers`, `database`, and `table` cannot be null or empty

### Sink Configuration Validation
- **Write Mode**: Must be either "append" or "upsert"
- **Batch Settings**: `batch.size` and `batch.timeout.ms` must be positive numbers
- **Exactly-Once**: Requires `enable.transaction = true` when `enable.exactly.once = true`
- **Required Fields**: `bootstrap.servers`, `database`, and `table` cannot be null or empty

### Error Handling
- **Configuration Errors**: Throw `FlussConnectorException` with detailed error messages
- **No Fallback**: Configuration errors are not silently ignored or auto-corrected
- **Early Detection**: Validation occurs during connector initialization, not at runtime

## Troubleshooting

### Common Issues

1. **Connection Failed**: Check bootstrap servers and network connectivity
2. **Table Not Found**: Verify database and table names
3. **Schema Mismatch**: Ensure source and sink schemas are compatible
4. **Transaction Timeout**: Increase transaction timeout for large batches
5. **Configuration Error**: Check the detailed error message for specific configuration issues

### Logging

Enable debug logging for the Fluss connector:

```
logger.org.apache.seatunnel.connectors.seatunnel.fluss.level = DEBUG
```

## Implementation Details

### Reading from Fluss

The connector uses Fluss's `LogScanner` API to read data:

- Creates a connection to the Fluss cluster using bootstrap servers
- Subscribes to table buckets based on the configured startup mode
- Polls for records and converts them to SeaTunnel rows
- Supports both bounded (batch) and unbounded (streaming) modes

#### Startup Modes

- **earliest**: Start reading from the beginning of the log using `subscribeFromBeginning()`
- **latest**: Start reading from the latest records using Admin `listOffsets()` with `OffsetSpec.latest()`
- **timestamp**: Start reading from a specific timestamp using Admin `listOffsets()` with `OffsetSpec.forTimestamp()`

**Implementation Details**:
- **Type Safety**: Uses `StartupMode` enum for type-safe configuration and validation
- **Latest Mode**: Uses `Admin.listOffsets(tablePath, buckets, OffsetSpec.latest())` to query the exact latest offset for each bucket
- **Timestamp Mode**: Uses `Admin.listOffsets(tablePath, buckets, OffsetSpec.forTimestamp(timestamp))` to find the offset corresponding to the specified timestamp
- **Fallback**: If Admin API fails, automatically falls back to earliest mode with appropriate logging
- **Accuracy**: This implementation provides precise offset positioning using Fluss's official Admin API
- **Switch-based Logic**: Uses clean switch statements for better code maintainability and performance

### Writing to Fluss

The connector supports two write modes:

- **Append Mode**: Uses Fluss's `AppendWriter` for log tables
- **Upsert Mode**: Uses Fluss's `UpsertWriter` for primary key tables

Data is buffered and flushed based on batch size and timeout configurations.

## Dependencies

The connector requires the following dependencies:

```xml
<dependency>
    <groupId>com.alibaba.fluss</groupId>
    <artifactId>fluss-client</artifactId>
    <version>0.7.0</version>
</dependency>
```

## License

This connector is licensed under the Apache License 2.0.
