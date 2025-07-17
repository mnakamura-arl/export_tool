# Add this import at the top of __main__.py (after existing imports)
from .loki_export import LokiExporter, add_loki_args, handle_loki_operations

# Modify your main() function - add these lines after creating the parser:

def main():
    parser = argparse.ArgumentParser(description="Query and export PostgreSQL sensor data and Loki logs.")
    
    # ... existing PostgreSQL arguments ...
    
    # Add Loki arguments
    add_loki_args(parser)
    
    # Add combined export options
    parser.add_argument("--export-both", action="store_true",
                       help="Export both PostgreSQL and Loki data to separate files")
    
    args = parser.parse_args()

    # Handle Loki-only operations first
    if args.loki_labels:
        handle_loki_operations(args)
        return
    
    # Check if this is a Loki-only export
    is_loki_only = (args.loki_query or args.loki_metrics) and not (args.sensors or args.query)
    
    if is_loki_only:
        # Loki-only export
        loki_df = handle_loki_operations(args)
        if loki_df is not None and not loki_df.empty:
            export_loki_dataframe(loki_df, args)
        return
    
    # ... existing PostgreSQL credential handling ...
    
    # Rest of your existing PostgreSQL logic here...
    
    # At the end, check if we also need to export Loki data
    if args.loki_query or args.loki_metrics:
        print("\n" + "="*50)
        print("LOKI DATA EXPORT")
        print("="*50)
        
        loki_df = handle_loki_operations(args)
        if loki_df is not None and not loki_df.empty:
            export_loki_dataframe(loki_df, args)

def export_loki_dataframe(df, args):
    """Export Loki DataFrame to files"""
    print(f"Exporting Loki data ({len(df)} records)...")
    
    timestamp_flag = not args.no_timestamp
    
    for fmt in args.format:
        if fmt in ['bufr', 'grib']:
            print(f"⚠️ Skipping {fmt.upper()} format for Loki data (not applicable)")
            continue
            
        filename = generate_filename("loki_export", fmt, None, timestamp_flag)
        full_path = os.path.join(args.out_dir, filename)
        
        try:
            if fmt == "csv":
                df.to_csv(full_path, index=False)
            elif fmt == "json":
                df.to_json(full_path, orient="records", lines=True)
            elif fmt == "excel":
                df.to_excel(full_path, index=False)
            
            print(f"✅ Successfully wrote {fmt.upper()} to {full_path}")
            
        except Exception as e:
            print(f"❌ Failed to write {fmt.upper()} file: {e}")

# Usage examples:
"""
# List Loki labels
python __main__.py --loki-labels

# Export Loki logs only
python __main__.py --loki-query '{job="systemd-journal"}' --format csv

# Export PostgreSQL data only (existing functionality)
python __main__.py --sensors temperature,pressure --format csv

# Export both PostgreSQL and Loki data
python __main__.py --sensors temperature --loki-query '{job="myapp"}' --format csv excel
"""
