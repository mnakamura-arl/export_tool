import argparse
import os
import sys
from datetime import datetime
import pandas as pd
from export_tool import ExportTool

def read_secret_file(filepath):
    """Read a secret from a file, return None if file doesn't exist"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r') as f:
                return f.read().strip()
        return None
    except Exception as e:
        print(f"⚠️ Warning: Could not read secret file {filepath}: {e}")
        return None

def get_db_credentials(secrets_dir="secrets"):
    """Get database credentials from secret files or environment variables"""
    credentials = {}
    
    # Try to read from secret files first
    if os.path.exists(secrets_dir):
        user_file = os.path.join(secrets_dir, "db_user.txt")
        password_file = os.path.join(secrets_dir, "db_password.txt")
        
        credentials['user'] = read_secret_file(user_file)
        credentials['password'] = read_secret_file(password_file)
        
        if credentials['user'] and credentials['password']:
            print(f"✅ Loaded credentials from {secrets_dir}/ directory")
            return credentials
    
    # Fallback to environment variables
    credentials['user'] = os.getenv('DB_USER')
    credentials['password'] = os.getenv('DB_PASSWORD')
    
    if credentials['user'] and credentials['password']:
        print("✅ Loaded credentials from environment variables")
        return credentials
    
    # No credentials found
    return {'user': None, 'password': None}

def get_table_columns(db_params, table_name):
    """Get column names and types for a specific table"""
    try:
        import psycopg2
        with psycopg2.connect(**db_params) as conn:
            cur = conn.cursor()
            cur.execute("""
                SELECT column_name, data_type 
                FROM information_schema.columns 
                WHERE table_name = %s 
                ORDER BY ordinal_position
            """, (table_name,))
            return [(row[0], row[1]) for row in cur.fetchall()]
    except Exception as e:
        print(f"❌ Failed to get columns for {table_name}: {e}")
        return []

def find_common_columns(db_params, tables):
    """Find columns that exist in all tables"""
    if not tables:
        return []
    
    # Get columns for each table
    table_columns = {}
    for table in tables:
        columns = get_table_columns(db_params, table)
        table_columns[table] = {col[0]: col[1] for col in columns}
    
    # Find intersection of all column sets
    if not table_columns:
        return []
    
    # Start with first table's columns
    common_cols = set(table_columns[tables[0]].keys())
    
    # Find intersection with other tables
    for table in tables[1:]:
        common_cols = common_cols.intersection(set(table_columns[table].keys()))
    
    return sorted(list(common_cols))

def build_pivot_query(db_params, tables, start_time, end_time, timestamp_col='timestamp'):
    """Build queries to get all data for pivot/merge on timestamp"""
    if not tables:
        return []
    
    queries = []
    for table in tables:
        conditions = []
        
        if start_time:
            conditions.append(f"{timestamp_col} >= '{start_time}'")
        if end_time:
            conditions.append(f"{timestamp_col} <= '{end_time}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        # Get all columns for this table
        columns_info = get_table_columns(db_params, table)
        column_names = [col[0] for col in columns_info]
        
        # Create query with sensor prefix for non-timestamp columns
        select_parts = [timestamp_col]
        for col in column_names:
            if col != timestamp_col:
                select_parts.append(f"{col} as {table}_{col}")
        
        select_clause = ', '.join(select_parts)
        query = f"SELECT {select_clause} FROM {table} {where_clause}"
        queries.append((table, query))
    
    return queries

def build_pivot_query(db_params, tables, start_time, end_time, timestamp_col='timestamp'):
    """Build queries to get all data for pivot/merge on timestamp"""
    if not tables:
        return []
    
    queries = []
    for table in tables:
        conditions = []
        
        if start_time:
            conditions.append(f"{timestamp_col} >= '{start_time}'")
        if end_time:
            conditions.append(f"{timestamp_col} <= '{end_time}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        
        # Get all columns for this table
        columns_info = get_table_columns(db_params, table)
        column_names = [col[0] for col in columns_info]
        
        # Create query with sensor prefix for non-timestamp columns
        select_parts = [timestamp_col]
        for col in column_names:
            if col != timestamp_col:
                select_parts.append(f"{col} as {table}_{col}")
        
        select_clause = ', '.join(select_parts)
        query = f"SELECT {select_clause} FROM {table} {where_clause}"
        queries.append((table, query))
    
    return queries

def merge_sensor_data(db_params, tables, start_time, end_time, timestamp_col='timestamp'):
    """Merge all sensor data on timestamp into a single wide DataFrame"""
    import pandas as pd
    import psycopg2
    
    queries = build_pivot_query(db_params, tables, start_time, end_time, timestamp_col)
    
    if not queries:
        return pd.DataFrame()
    
    print(f"Merging {len(tables)} sensors on {timestamp_col} column...")
    
    # Execute each query and collect DataFrames
    dfs = []
    with psycopg2.connect(**db_params) as conn:
        for table, query in queries:
            try:
                print(f"  Loading {table}...")
                df = pd.read_sql_query(query, conn)
                if not df.empty:
                    # Ensure timestamp is datetime
                    df[timestamp_col] = pd.to_datetime(df[timestamp_col])
                    df.set_index(timestamp_col, inplace=True)
                    dfs.append(df)
                    print(f"    ✅ {len(df)} records")
                else:
                    print(f"    ⚠️ No data")
            except Exception as e:
                print(f"    ❌ Error loading {table}: {e}")
    
    if not dfs:
        print("No data loaded from any sensor")
        return pd.DataFrame()
    
    # Merge all DataFrames on timestamp index
    print("Merging data on timestamp...")
    merged_df = dfs[0]
    
    for df in dfs[1:]:
        merged_df = merged_df.join(df, how='outer', rsuffix='_dup')
    
    # Reset index to make timestamp a column again
    merged_df.reset_index(inplace=True)
    
    # Sort by timestamp
    merged_df.sort_values(timestamp_col, inplace=True)
    
    print(f"✅ Merged dataset: {len(merged_df)} timestamps x {len(merged_df.columns)} columns")
    print(f"   Time range: {merged_df[timestamp_col].min()} to {merged_df[timestamp_col].max()}")
    print(f"   Columns: {list(merged_df.columns)}")
    
    return merged_df

def build_dynamic_query(db_params, tables, start_time, end_time):
    """Build SQL query based on filters - handles different table schemas"""
    if not tables:
        return "SELECT 1 WHERE 1=0;"  # Empty result if no tables
    
    if len(tables) == 1:
        # Single table - simple query
        table = tables[0]
        conditions = []
        
        if start_time:
            conditions.append(f"timestamp >= '{start_time}'")
        if end_time:
            conditions.append(f"timestamp <= '{end_time}'")

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        return f"SELECT *, '{table}' as sensor_id FROM {table} {where_clause} ORDER BY timestamp;"
    
    else:
        # Multiple tables - find common columns
        common_columns = find_common_columns(db_params, tables)
        
        if not common_columns:
            print("❌ No common columns found between tables. Cannot union data.")
            return "SELECT 1 WHERE 1=0;"
        
        print(f"Found common columns: {common_columns}")
        
        # Build column list
        col_list = ', '.join(common_columns)
        
        queries = []
        for table in tables:
            conditions = []
            
            if start_time:
                conditions.append(f"timestamp >= '{start_time}'")
            if end_time:
                conditions.append(f"timestamp <= '{end_time}'")

            where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
            
            # Select only common columns plus sensor_id
            query = f"SELECT {col_list}, '{table}' as sensor_id FROM {table} {where_clause}"
            queries.append(query)
        
        # Union all table queries together
        full_query = " UNION ALL ".join(queries) + " ORDER BY timestamp;"
        return full_query

def get_available_tables(db_params):
    """Get list of all available tables in the database"""
    try:
        import psycopg2
        with psycopg2.connect(**db_params) as conn:
            cur = conn.cursor()
            cur.execute("""SELECT table_name FROM information_schema.tables
                        WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
                        ORDER BY table_name""")
            tables = [t[0] for t in cur.fetchall()]
            return tables
    except Exception as e:
        print(f"❌ Failed to get table list: {e}")
        return []

def sanitize_filename_part(s):
    """Clean string for use in filename"""
    return s.replace(":", "").replace(" ", "_").replace("-", "").replace("/", "")

def generate_filename(base, ext, sensors=None, use_timestamp=True):
    """Generate output filename with optional sensor IDs and timestamp"""
    parts = [base]
    if sensors:
        sensor_part = "_".join([sanitize_filename_part(str(s)) for s in sensors])
        parts.append(f"S_{sensor_part}")
    if use_timestamp:
        parts.append(datetime.utcnow().strftime("%Y%m%dT%H%M%SZ"))
    return f"{'_'.join(parts)}.{ext}"

def test_connection(db_params):
    """Test database connection and show available tables"""
    try:
        import psycopg2
        with psycopg2.connect(**db_params) as conn:
            cur = conn.cursor()
            
            # List tables
            tables = get_available_tables(db_params)
            print(f"✅ Connection successful!")
            print(f"Available tables (sensors): {tables}")
            print(f"Total sensors: {len(tables)}")
            
            # Show sample data and schema from each table
            all_columns = {}
            for table in tables:
                try:
                    # Get column info
                    columns_info = get_table_columns(db_params, table)
                    all_columns[table] = [col[0] for col in columns_info]
                    
                    cur.execute(f"SELECT COUNT(*) FROM {table}")
                    count = cur.fetchone()[0]
                    
                    cur.execute(f"SELECT * FROM {table} ORDER BY timestamp DESC LIMIT 1")
                    rows = cur.fetchall()
                    
                    print(f"\n--- Sensor: {table} ({count} records) ---")
                    print(f"Columns ({len(columns_info)}): {[f'{col[0]}({col[1]})' for col in columns_info]}")
                    
                    if rows:
                        column_names = [col[0] for col in columns_info]
                        print(f"Latest record: {dict(zip(column_names, rows[0]))}")
                    else:
                        print("No data in table")
                        
                except Exception as e:
                    print(f"⚠️ Error reading {table}: {e}")
            
            # Show common columns across all tables
            if len(tables) > 1:
                common_cols = find_common_columns(db_params, tables)
                print(f"\n📊 Common columns across all sensors: {common_cols}")
                if not common_cols:
                    print("⚠️ Warning: No common columns found - UNION queries will not work")
                    print("   Consider exporting sensors individually")
                
            return True, tables
            
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        return False, []

def main():
    parser = argparse.ArgumentParser(description="Query and export PostgreSQL sensor data.")
    
    # Query input
    parser.add_argument("--query", help="Raw SQL query (overrides all filters)")
    parser.add_argument("--sensors", nargs="+", help="Sensor names (table names) to include. Use 'all' for all sensors")
    parser.add_argument("--start-time", help="Start time (ISO 8601 format)")
    parser.add_argument("--end-time", help="End time (ISO 8601 format)")
    parser.add_argument("--limit", type=int, help="Limit number of rows returned")
    parser.add_argument("--list-sensors", action="store_true", help="List all available sensors and exit")
    parser.add_argument("--separate-files", action="store_true", 
                       help="Export each sensor to separate files (useful when schemas differ)")
    parser.add_argument("--merge-on-timestamp", action="store_true",
                       help="Merge all sensors into wide format table using timestamp as key")
    parser.add_argument("--timestamp-col", default="timestamp",
                       help="Name of timestamp column for merging (default: timestamp)")

    # Output control
    parser.add_argument("--format", nargs="+", 
                       choices=["csv", "json", "excel", "bufr", "grib"], 
                       default=["csv"], 
                       help="Output format(s)")
    parser.add_argument("--out-dir", default=".", help="Output directory")
    parser.add_argument("--no-timestamp", action="store_true", 
                       help="Omit timestamp in output filenames")

    # DB connection
    parser.add_argument("--host", default="localhost", help="Database host")
    parser.add_argument("--port", default=5432, type=int, help="Database port")
    parser.add_argument("--db", default="sensordata", help="Database name")
    parser.add_argument("--sslmode", default="disable", help="SSL mode")
    parser.add_argument("--secrets-dir", default="secrets", help="Directory containing secret files (default: secrets)")

    # Testing
    parser.add_argument("--test-connection", action="store_true", 
                       help="Test database connection and show available tables")

    args = parser.parse_args()

    # Get database credentials from secrets only
    credentials = get_db_credentials(args.secrets_dir)
    
    db_user = credentials['user']
    db_password = credentials['password']
    
    if not db_user or not db_password:
        print("❌ Database credentials not found!")
        print(f"Please either:")
        print(f"  1. Create {args.secrets_dir}/db_user.txt and {args.secrets_dir}/db_password.txt")
        print(f"  2. Set DB_USER and DB_PASSWORD environment variables")
        print(f"  3. Run: poetry run python setup_secrets.py")
        sys.exit(1)

    # Database connection parameters
    db_params = {
        "host": args.host,
        "port": args.port,
        "user": db_user,
        "password": db_password,
        "database": args.db,
        "sslmode": args.sslmode,
    }

    # Test connection if requested
    if args.test_connection:
        success, tables = test_connection(db_params)
        if not success:
            sys.exit(1)
        return

    # List sensors if requested
    if args.list_sensors:
        tables = get_available_tables(db_params)
        if tables:
            print("Available sensors (tables):")
            for i, table in enumerate(tables, 1):
                print(f"  {i}. {table}")
        else:
            print("No sensors found or connection failed.")
        return

    # Determine which tables/sensors to query
    available_tables = get_available_tables(db_params)
    if not available_tables:
        print("❌ No tables found in database")
        sys.exit(1)

    if args.query:
        # Use custom query as-is
        query = args.query
        selected_sensors = ["custom_query"]
    else:
        # Determine which sensors to include
        if args.sensors:
            if 'all' in args.sensors:
                selected_tables = available_tables
                selected_sensors = available_tables
            else:
                # Validate sensor names exist
                selected_tables = []
                selected_sensors = []
                for sensor in args.sensors:
                    if sensor in available_tables:
                        selected_tables.append(sensor)
                        selected_sensors.append(sensor)
                    else:
                        print(f"⚠️ Warning: Sensor '{sensor}' not found. Available: {available_tables}")
                
                if not selected_tables:
                    print("❌ No valid sensors specified")
                    sys.exit(1)
        else:
            # Default: use all available tables
            selected_tables = available_tables
            selected_sensors = available_tables
            print(f"No sensors specified, using all {len(selected_tables)} sensors")

        query = build_dynamic_query(db_params, selected_tables, args.start_time, args.end_time)
        if args.limit:
            query = query.rstrip(';') + f" LIMIT {args.limit};"

    print(f"Selected sensors: {selected_sensors}")
    print(f"Executing query: {query}")

    # Ensure output directory exists
    os.makedirs(args.out_dir, exist_ok=True)

    try:
        # Query DB
        tool = ExportTool(db_params)
        
        if args.separate_files and not args.query:
            # Export each sensor to separate files
            print(f"Exporting {len(selected_tables)} sensors to separate files...")
            
            for table in selected_tables:
                print(f"\n--- Processing sensor: {table} ---")
                
                # Build query for single table
                single_query = build_dynamic_query(db_params, [table], args.start_time, args.end_time)
                if args.limit:
                    single_query = single_query.rstrip(';') + f" LIMIT {args.limit};"
                
                df = tool.query_db(single_query)
                
                if df.empty:
                    print(f"⚠️ No data for sensor {table}")
                    continue
                
                print(f"✅ Retrieved {len(df)} rows with columns: {list(df.columns)}")
                
                # Export each format for this sensor
                timestamp_flag = not args.no_timestamp
                for fmt in args.format:
                    filename = generate_filename(f"sensor_{table}", fmt, None, timestamp_flag)
                    full_path = os.path.join(args.out_dir, filename)
                    tool.write_file(df, fmt, full_path)
                    
        elif args.merge_on_timestamp and not args.query:
            # Merge all sensors on timestamp
            print(f"Merging {len(selected_tables)} sensors on timestamp...")
            
            df = merge_sensor_data(db_params, selected_tables, args.start_time, args.end_time, args.timestamp_col)
            
            if df.empty:
                print("⚠️ No data returned after merging. Exiting.")
                sys.exit(0)
            
            # Apply limit after merging if specified
            if args.limit:
                print(f"Limiting to {args.limit} most recent records...")
                df = df.tail(args.limit)
                print(f"Final dataset: {len(df)} records")

            # Export merged data
            timestamp_flag = not args.no_timestamp
            for fmt in args.format:
                filename = generate_filename("merged_timeseries", fmt, selected_sensors, timestamp_flag)
                full_path = os.path.join(args.out_dir, filename)
                tool.write_file(df, fmt, full_path)
                    
        else:
            # Single combined export (original behavior)
            df = tool.query_db(query)

            # Check if data was returned
            if df.empty:
                print("⚠️ No data returned by the query. Exiting.")
                sys.exit(0)

            print(f"✅ Retrieved {len(df)} rows with columns: {list(df.columns)}")

            # Export data in requested formats
            timestamp_flag = not args.no_timestamp

            for fmt in args.format:
                filename = generate_filename("output", fmt, selected_sensors, timestamp_flag)
                full_path = os.path.join(args.out_dir, filename)
                tool.write_file(df, fmt, full_path)

        print(f"\n✅ Export completed successfully!")
        
    except Exception as e:
        print(f"❌ Export failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
