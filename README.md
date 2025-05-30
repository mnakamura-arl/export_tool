# Export Tool

A flexible Python tool for querying and exporting PostgreSQL sensor data in multiple formats. Designed for time series data from multiple sensor tables with different schemas.

## Features

- **Multiple Export Formats**: CSV, JSON, Excel, BUFR, GRIB
- **Three Export Modes**: 
  - Merged time series (wide format)
  - Separate files per sensor
  - Combined long format
- **Flexible Filtering**: Time ranges, specific sensors, custom queries
- **Schema Intelligence**: Handles tables with different column structures
- **Time Series Optimization**: Merge all sensors on common timestamp

## Installation

```bash
poetry install
```

```bash
poetry run python setup_secrets.py
```

## 🔒 Secure Credential Setup (Required)

The tool requires database credentials to be stored securely. It supports two methods:

### 1. Secret Files (Recommended)
```bash
poetry run python setup_secrets.py
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
poetry run python setup_secrets.py
```

### 2. Test Connection
```bash
poetry run python src/export_tool/__main__.py --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --test-connection
```

### 3. List Available Sensors
```bash
poetry run python src/export_tool/__main__.py --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --list-sensors
```

### 4. Export All Data (Merged Time Series)
```bash
poetry run python src/export_tool/__main__.py --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --merge-on-timestamp --format csv
```

## Export Modes

### 🔥 Merged Time Series (Recommended for Analysis)
Creates a wide-format table with timestamp as the key and all sensor readings as columns.

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --start-time "2024-01-01" --end-time "2024-12-31" --format csv
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv json excel
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --limit 1000 --format csv
```

**Output**: Single file with columns like:
```
timestamp | pca9548_bme280_data_temperature | pca9548_bme280_data_humidity | tsys01_data_temperature
```

### 📁 Separate Files (Best for Different Schemas)
Each sensor gets its own file with all original columns preserved.

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --separate-files --format csv
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --sensors pca9548_bme280_data tsys01_data --separate-files --format csv
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --separate-files --start-time "2024-12-01" --format csv json
```

**Output**: Multiple files like `sensor_pca9548_bme280_data_20241230T120000Z.csv`

### 📊 Combined Long Format (Common Columns Only)
All sensors in one file, long format, using only columns common to all tables.

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --format csv
```

```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --sensors pca9548_bme280_data --format csv
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
--test-connection    # Test connection and show sample data
--list-sensors      # List all available sensors (tables)
```

## Example Workflows

### Time Series Analysis Workflow

**1. Set up credentials (one time)**
```bash
poetry run python setup_secrets.py
```

**2. Check your data structure**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --test-connection
```

**3. Export small sample to verify format**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --limit 10 --format csv
```

**4. Export full dataset for analysis**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --format csv json
```

**5. Export specific time period**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --merge-on-timestamp --start-time "2024-12-01" --end-time "2024-12-31" --format excel
```

### Data Backup Workflow

**Export each sensor separately with full schema**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --separate-files --format csv json --out-dir ./backups
```

### Custom Analysis Workflow

**Use custom SQL for complex queries**
```bash
poetry run python src/export_tool/__main__.py --host 192.168.1.12 --port 8001 --db data --query "SELECT * FROM pca9548_bme280_data WHERE temperature > 25 ORDER BY timestamp DESC LIMIT 100" --format csv
```

## Output Files

### Merged Time Series
- `merged_timeseries_S_sensor1_sensor2_20241230T120000Z.csv`
- Contains all sensor data aligned by timestamp
- Wide format: one row per timestamp, one column per sensor measurement

### Separate Files
- `sensor_pca9548_bme280_data_20241230T120000Z.csv`
- `sensor_tsys01_data_20241230T120000Z.csv`
- Each file contains complete schema for that sensor

### Combined Files
- `output_S_sensor1_sensor2_20241230T120000Z.csv`
- Long format with sensor_id column
- Only common columns included

## File Formats

- **CSV**: Standard comma-separated values
- **JSON**: Line-delimited JSON records
- **Excel**: .xlsx format with headers
- **BUFR**: Binary Universal Form for Representation (meteorological data format)
- **GRIB**: GRIdded Binary format (meteorological data format)

## Tips

1. **For time series analysis**: Use `--merge-on-timestamp` to get all sensor data aligned by time
2. **For data backup**: Use `--separate-files` to preserve complete schemas
3. **For large datasets**: Use `--limit` for testing, then remove for full export
4. **For specific periods**: Use `--start-time` and `--end-time` with ISO 8601 format
5. **Check schemas first**: Always run `--test-connection` to understand your data structure

## Troubleshooting

### Connection Issues

**Test your connection parameters**
```bash
poetry run python src/export_tool/__main__.py --host YOUR_HOST --port YOUR_PORT --db YOUR_DB --test-connection
```

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
poetry run python src/export_tool/__main__.py --separate-files --format csv
```

**OR use merged time series**
```bash
poetry run python src/export_tool/__main__.py --merge-on-timestamp --format csv
```

### Large Dataset Performance

**Test with small sample first**
```bash
poetry run python src/export_tool/__main__.py --limit 100 --format csv
```

**Then export specific time ranges**
```bash
poetry run python src/export_tool/__main__.py --start-time "2024-12-01" --format csv
```
