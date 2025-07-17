# Export Tool

A flexible Python tool for querying and exporting PostgreSQL sensor data and Loki log data in multiple formats. Designed for time series data from multiple sensor tables with different schemas and log aggregation from Loki.

## Features

- **Multiple Data Sources**: PostgreSQL sensor data + Loki log data
- **Multiple Export Formats**: CSV, JSON, Excel, BUFR, GRIB
- **Three Export Modes**: 
  - Merged time series (wide format)
  - Separate files per sensor
  - Combined long format
- **Flexible Filtering**: Time ranges, specific sensors, custom queries
- **Schema Intelligence**: Handles tables with different column structures
- **Time Series Optimization**: Merge all sensors on common timestamp
- **Log Data Integration**: Query and export Loki logs alongside sensor data

## Installation

```bash
pyenv local 3.10
```

```bash
poetry install
```

## 🔒 Secure Credential Setup (Required)

The tool requires database credentials to be stored securely. It supports two methods:

### 1. Secret Files (Recommended)
```bash
poetry run python src/export_tool/setup_secrets.py
```

This creates:
- `secrets/db_user.txt` - Contains your username
- `secrets/db_password.txt` - Contains your password  
- `.gitignore` entry - Prevents credential files from being committed

### 2. Environment Variables
```bash
export DB_USER="your_username"
```

```bash
export DB_PASSWORD="your_password"
```

**🔒 Security**: Credentials are never exposed in command line arguments, process lists, or shell history.

## Quick Start

### 1. Set Up Credentials (One Time)
```bash
poetry run python src/export_tool/setup_secrets.py
```

### 2. Test PostgreSQL Connection
```bash
poetry run export-tool --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --test-connection
```

### 3. Test Loki Connection
```bash
poetry run export-tool --loki-url http://localhost:3100 --loki-labels
```

### 4. List Available Sensors
```bash
poetry run export-tool --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --list-sensors
```

### 5. Export All Sensor Data (Merged Time Series)
```bash
poetry run export-tool --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --merge-on-timestamp --format csv
```

## PostgreSQL Export Modes

### 🔥 Merged Time Series (Recommended for Analysis)
Creates a wide-format table with timestamp as the key and all sensor readings as columns.

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --start-time "2024-01-01" --end-time "2024-12-31" --format csv
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv json excel
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --limit 1000 --format csv
```

### 📁 Separate Files (Best for Different Schemas)
Each sensor gets its own file with all original columns preserved.

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --separate-files --format csv
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --sensors pca9548_bme280_data tsys01_data --separate-files --format csv
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --separate-files --start-time "2024-12-01" --format csv json
```

**Output**: Multiple files like `sensor_pca9548_bme280_data_20241230T120000Z.csv`

### 📊 Combined Long Format (Common Columns Only)
All sensors in one file, long format, using only columns common to all tables.

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --format csv
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --sensors pca9548_bme280_data --format csv
```

## Loki Log Export

### 🔍 Explore Available Labels
See what log data is available in your Loki instance:

```bash
poetry run export-tool --loki-url http://localhost:3100 --loki-labels
```

### 📋 Export System Logs
Export systemd journal logs:

```bash
poetry run export-tool --loki-query '{job="systemd-journal"}' --format csv
```

```bash
poetry run export-tool --loki-query '{job="systemd-journal"}' --start-time "2024-01-01T00:00:00Z" --end-time "2024-01-02T00:00:00Z" --format csv json
```

### 🐳 Export Container Logs
Export Docker container logs:

```bash
poetry run export-tool --loki-query '{container_name="postgres"}' --loki-limit 5000 --format csv
```

```bash
poetry run export-tool --loki-query '{compose_project="myproject", compose_service="web"}' --format json
```

### ⚠️ Export Error Logs Only
Filter for specific log levels or content:

```bash
poetry run export-tool --loki-query '{job="myapp"} |= "ERROR"' --format csv
```

```bash
poetry run export-tool --loki-query '{job="systemd-journal", unit="ssh"} |= "Failed password"' --format csv
```

### 📊 Export Log Metrics
Export aggregated metrics from logs:

```bash
poetry run export-tool --loki-metrics 'rate({job="myapp", level="error"}[5m])' --loki-step 30s --format csv
```

```bash
poetry run export-tool --loki-metrics 'sum(rate({job=~".+"}[5m])) by (job)' --format csv
```

### 🔗 Combined PostgreSQL + Loki Export
Export both sensor data and logs together:

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --sensors temperature,pressure --loki-query '{job="systemd-journal"}' --format csv excel
```

```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --loki-query '{container_name="sensor-app"}' --start-time "2024-01-01T00:00:00Z" --format csv
```

## Common Loki LogQL Queries

### System Monitoring
```bash
# System journal logs
--loki-query '{job="systemd-journal"}'

# Kernel messages
--loki-query '{job="systemd-journal", unit="kernel"}'

# SSH login attempts
--loki-query '{job="systemd-journal", unit="ssh"} |= "Failed password"'

# Service failures
--loki-query '{job="systemd-journal"} |= "failed"'

# Disk space warnings
--loki-query '{job="systemd-journal"} |= "disk" |= "space"'
```

### Container Monitoring
```bash
# All container logs
--loki-query '{job="containerd"}'

# Specific container
--loki-query '{container_name="myapp"}'

# Docker Compose services
--loki-query '{compose_project="myproject", compose_service="web"}'

# Container restarts
--loki-query '{job="containerd"} |= "restart"'
```

### Application Monitoring
```bash
# Application errors
--loki-query '{job="myapp", level="error"}'

# Pattern matching
--loki-query '{job="nginx"} |= "404"'

# JSON log parsing
--loki-query '{job="myapp"} | json | level="error"'

# Regular expressions
--loki-query '{job="app"} | regexp "(?P<ip>\\d+\\.\\d+\\.\\d+\\.\\d+)"'
```

### Metrics Queries
```bash
# Error rate per minute
--loki-metrics 'rate({job="myapp", level="error"}[1m])'

# Log volume by service
--loki-metrics 'sum(rate({job=~".+"}[5m])) by (job)'

# Failed login rate
--loki-metrics 'rate({job="systemd-journal", unit="ssh"} |= "Failed password"[5m])'
```

## Command Line Options

### Connection Parameters
```bash
--host          # Database host (default: localhost)
--port          # Database port (default: 5432)
--db            # Database name
--sslmode       # SSL mode (default: disable)
--secrets-dir   # Directory containing credential files (default: secrets)
```

### Loki Parameters
```bash
--loki-url              # Loki server URL (default: http://localhost:3100)
--loki-query            # LogQL query for logs
--loki-metrics          # LogQL metrics query  
--loki-labels           # List available Loki labels and exit
--loki-limit            # Limit number of log entries (default: 1000)
--loki-step             # Step size for metrics queries (default: 1m)
```

### Query & Filtering
```bash
--query                 # Custom SQL query (overrides all filters)
--sensors SENSOR1 SENSOR2  # Specific sensor names (table names)
--sensors all              # Explicitly specify all sensors
--start-time "2024-01-01"  # Start time (ISO 8601)
--end-time "2024-12-31"    # End time (ISO 8601)
--limit 1000               # Limit number of rows
--timestamp-col "timestamp" # Custom timestamp column name
```

### Export Modes
```bash
--merge-on-timestamp    # Create wide-format time series table
--separate-files        # Export each sensor to separate files
# (default)             # Combined long format with common columns
```

### Output Control
```bash
--format csv json excel bufr grib  # Output format(s)
--out-dir ./exports               # Output directory
--no-timestamp                    # Omit timestamp from filenames
```

### Utilities
```bash
--test-connection    # Test PostgreSQL connection and show sample data
--list-sensors      # List all available sensors (tables)
--loki-labels       # Test Loki connection and show available labels
```

## Example Workflows

### Time Series Analysis Workflow

**1. Set up credentials (one time)**
```bash
poetry run python setup_secrets.py
```

**2. Check your data structure**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --test-connection
poetry run export-tool --loki-url http://localhost:3100 --loki-labels
```

**3. Export small sample to verify format**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --limit 10 --format csv
poetry run export-tool --loki-query '{job="systemd-journal"}' --loki-limit 10 --format csv
```

**4. Export full dataset for analysis**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv json
```

**5. Export specific time period with logs**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --loki-query '{job="sensor-app"}' --start-time "2024-12-01" --end-time "2024-12-31" --format excel
```

### System Monitoring Workflow

**1. Export system health logs**
```bash
poetry run export-tool --loki-query '{job="systemd-journal"} |= "error"' --start-time "2024-12-01T00:00:00Z" --format csv
```

**2. Export container performance logs**
```bash
poetry run export-tool --loki-query '{job="containerd"}' --loki-limit 5000 --format json
```

**3. Correlate sensor data with system events**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --sensors temperature --loki-query '{job="systemd-journal"} |= "thermal"' --start-time "2024-12-01T00:00:00Z" --format csv
```

### Security Monitoring Workflow

**1. Export SSH login attempts**
```bash
poetry run export-tool --loki-query '{job="systemd-journal", unit="ssh"} |= "Failed password"' --start-time "2024-12-01T00:00:00Z" --format csv
```

**2. Export application security events**
```bash
poetry run export-tool --loki-query '{job="myapp"} |= "authentication" |= "failed"' --format json
```

**3. Export error rate metrics**
```bash
poetry run export-tool --loki-metrics 'rate({job="myapp", level="error"}[5m])' --loki-step 1m --format csv
```

### Data Backup Workflow

**Export each sensor separately with full schema**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --separate-files --format csv json --out-dir ./backups
```

**Export all logs by service**
```bash
poetry run export-tool --loki-query '{job="systemd-journal"}' --format json --out-dir ./log-backups
poetry run export-tool --loki-query '{job="containerd"}' --format json --out-dir ./log-backups
```

### Custom Analysis Workflow

**Use custom SQL for complex queries**
```bash
poetry run export-tool --host 192.168.1.12 --port 8001 --db data --query "SELECT * FROM pca9548_bme280_data WHERE temperature > 25 ORDER BY timestamp DESC LIMIT 100" --format csv
```

**Use complex LogQL for filtered logs**
```bash
poetry run export-tool --loki-query '{job="nginx"} | logfmt | status_code="500"' --format csv
```

## Output Files

### PostgreSQL Data
- `merged_timeseries_S_sensor1_sensor2_20241230T120000Z.csv` - Merged time series
- `sensor_pca9548_bme280_data_20241230T120000Z.csv` - Separate sensor files
- `output_S_sensor1_sensor2_20241230T120000Z.csv` - Combined format

### Loki Log Data
- `loki_export_20241230T120000Z.csv` - Log entries with labels
- `loki_export_20241230T120000Z.json` - Log entries in JSON format

### Combined Exports
When using both PostgreSQL and Loki options:
- `postgres_export_20241230T120000Z.csv` - Sensor data
- `loki_export_20241230T120000Z.csv` - Log data

## File Formats

- **CSV**: Standard comma-separated values
- **JSON**: Line-delimited JSON records
- **Excel**: .xlsx format with headers
- **BUFR**: Binary Universal Form for Representation (meteorological data format) - PostgreSQL only
- **GRIB**: GRIdded Binary format (meteorological data format) - PostgreSQL only

*Note: BUFR and GRIB formats are only supported for structured sensor data, not for log data.*

## Tips

1. **For time series analysis**: Use `--merge-on-timestamp` to get all sensor data aligned by time
2. **For data backup**: Use `--separate-files` to preserve complete schemas
3. **For large datasets**: Use `--limit` and `--loki-limit` for testing, then remove for full export
4. **For specific periods**: Use `--start-time` and `--end-time` with ISO 8601 format
5. **Check schemas first**: Always run `--test-connection` and `--loki-labels` to understand your data structure
6. **For log analysis**: Start with broad queries, then narrow down with LogQL filters
7. **For metrics**: Use `--loki-metrics` for aggregated data instead of individual log lines

## Troubleshooting

### PostgreSQL Connection Issues

**Test your connection parameters**
```bash
poetry run export-tool --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --test-connection
```

### Loki Connection Issues

**Test your Loki connection**
```bash
poetry run export-tool --loki-url http://YOUR_LOKI_HOST:3100 --loki-labels
```

**Common Loki URLs:**
- Local: `http://localhost:3100`
- Docker: `http://loki:3100` (if running in same network)
- Remote: `http://your-loki-server:3100`

### Missing Credentials

**Set up credentials properly**
```bash
poetry run python setup_secrets.py
```

**Or use environment variables (username)**
```bash
export DB_USER="your_username"
```

**Or use environment variables (password)**
```bash
export DB_PASSWORD="your_password"
```

### Schema Conflicts
If you get "UNION query must have the same number of columns":

**Use separate files instead**
```bash
poetry run export-tool --separate-files --format csv
```

**OR use merged time series**
```bash
poetry run export-tool --merge-on-timestamp --format csv
```

### Large Dataset Performance

**Test with small sample first**
```bash
poetry run export-tool --limit 100 --loki-limit 100 --format csv
```

**Then export specific time ranges**
```bash
poetry run export-tool --start-time "2024-12-01" --format csv
```

### No Loki Data Returned

**Check your LogQL syntax**
```bash
# Start simple
poetry run export-tool --loki-query '{job=~".+"}'

# Then add filters
poetry run export-tool --loki-query '{job="systemd-journal"}'
```

**Verify time ranges**
```bash
# Check if data exists in your time range
poetry run export-tool --loki-query '{job="systemd-journal"}' --loki-limit 1
```
