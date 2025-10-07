# DSE Pool Metrics Parser

This Python script parses pool metrics from DSE system logs and outputs them to CSV format.

## Features

- Parses the DSE pool statistics pattern from system logs
- Extracts timestamps from log entries
- Filters pools by name (case-insensitive)
- Outputs to CSV with specified columns: timestamp, pool_name, shared, stolen, completed, blocked, all_time_blocked
- Supports multiple input files
- Handles both stdout and file output

## Usage

### Basic Usage

```bash
python3 parse_pool_metrics.py system.log
```

### Filter Specific Pools (Column-based Output)

```bash
python3 parse_pool_metrics.py --pools "CompactionExecutor,GossipStage" system.log
```

### Filter TPC Pools (Column-based Output)

```bash
python3 parse_pool_metrics.py --pools "TPC" system.log
```

### Select Specific Metrics

```bash
python3 parse_pool_metrics.py --metrics "Active,Completed,Blocked" system.log
```

### Combine Pool and Metric Filtering

```bash
python3 parse_pool_metrics.py --pools "CompactionExecutor" --metrics "Active,Completed" system.log
```

### Multiple Files

```bash
python3 parse_pool_metrics.py system1.log system2.log system3.log
```

### Save to Custom File

```bash
python3 parse_pool_metrics.py --output custom_name.csv system.log
```

### Default Output

By default, the script creates a CSV file with a timestamp in the filename:

```bash
python3 parse_pool_metrics.py system.log
# Creates: pool_metrics_20251007_134328.csv
```

### Command Line Options

- `files`: One or more log files to parse (required)
- `--pools`: Comma-separated list of pool names to filter (optional)
- `--metrics`: Comma-separated list of metrics to include (optional)
- `--output`, `-o`: Output CSV file (optional, defaults to pool_metrics_<timestamp>.csv)

## Output Format

The script supports two output formats:

### Row-based Format (when no pools are specified)
When you don't specify `--pools`, each pool gets its own row:

```csv
timestamp,pool_name,shared,stolen,completed,blocked,all_time_blocked
"2025-10-03 10:08:32,368",CompactionExecutor,N/A,N/A,311,0,0
"2025-10-03 10:08:32,368",GossipStage,N/A,N/A,665,0,0
"2025-10-03 10:13:32,520",CompactionExecutor,N/A,N/A,636,0,0
```

### Column-based Format (when pools are specified)
When you specify `--pools`, each pool gets its own set of columns:

```csv
timestamp,CacheCleanupExecutor-Active,CacheCleanupExecutor-Pending,CacheCleanupExecutor-Backpressure,CacheCleanupExecutor-Delayed,CacheCleanupExecutor-Shared,CacheCleanupExecutor-Stolen,CacheCleanupExecutor-Completed,CacheCleanupExecutor-Blocked,CacheCleanupExecutor-All_Time_Blocked
"2025-10-03 10:08:32,368",0,0,N/A,N/A,N/A,N/A,0,0,0
"2025-10-03 10:13:32,520",0,0,N/A,N/A,N/A,N/A,0,0,0
```

Each selected pool gets columns for each selected statistic:
- `{PoolName}-Active`
- `{PoolName}-Pending`
- `{PoolName}-Backpressure`
- `{PoolName}-Delayed`
- `{PoolName}-Shared`
- `{PoolName}-Stolen`
- `{PoolName}-Completed`
- `{PoolName}-Blocked`
- `{PoolName}-All_Time_Blocked`

## Metric Selection

You can select which metrics to include in the output using the `--metrics` parameter:

**Available metrics:**
- `Active` - Active tasks
- `Pending` - Pending tasks  
- `Backpressure` - Backpressure status
- `Delayed` - Delayed tasks
- `Shared` - Shared tasks
- `Stolen` - Stolen tasks
- `Completed` - Completed tasks
- `Blocked` - Blocked tasks
- `All_Time_Blocked` - All time blocked tasks

**Examples:**
```bash
# Only include Active and Completed metrics
python3 parse_pool_metrics.py --metrics "Active,Completed" system.log

# Include all metrics (default behavior)
python3 parse_pool_metrics.py system.log
```

## Pattern Recognition

The script looks for the following pattern in log files:

```
Pool Name                                       Active        Pending   Backpressure   Delayed      Shared      Stolen      Completed   Blocked  All Time Blocked
```

It extracts the timestamp from the line immediately preceding this header and parses all subsequent pool data lines until it encounters an empty line or a new log entry.

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## Examples

See `example_usage.py` for programmatic usage examples.
