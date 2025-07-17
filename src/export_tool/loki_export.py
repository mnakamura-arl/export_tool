#!/usr/bin/env python3
"""
Loki log export functionality for the sensor data export tool.
"""

import requests
import json
import pandas as pd
from datetime import datetime, timezone
import sys

class LokiExporter:
    def __init__(self, loki_url="http://localhost:3100"):
        self.loki_url = loki_url.rstrip('/')
        self.session = requests.Session()
    
    def test_connection(self):
        """Test connection to Loki and show available labels"""
        try:
            # Test basic connectivity
            response = self.session.get(f"{self.loki_url}/ready", timeout=10)
            response.raise_for_status()
            print("✅ Loki connection successful!")
            
            # Get label names
            labels_response = self.session.get(f"{self.loki_url}/loki/api/v1/labels", timeout=10)
            labels_response.raise_for_status()
            labels = labels_response.json()
            
            print(f"📊 Available labels: {labels.get('data', [])}")
            
            # Get some sample label values
            for label in labels.get('data', [])[:5]:  # Show first 5 labels
                values_response = self.session.get(
                    f"{self.loki_url}/loki/api/v1/label/{label}/values", 
                    timeout=10
                )
                if values_response.status_code == 200:
                    values = values_response.json()
                    print(f"  {label}: {values.get('data', [])[:10]}")  # Show first 10 values
            
            return True
            
        except Exception as e:
            print(f"❌ Loki connection failed: {e}")
            return False
    
    def query_logs(self, query, start_time=None, end_time=None, limit=1000):
        """Query Loki logs using LogQL"""
        try:
            # Build query parameters
            params = {
                'query': query,
                'limit': limit,
                'direction': 'backward'  # Most recent first
            }
            
            # Add time range if specified
            if start_time:
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                params['start'] = str(int(start_time.timestamp() * 1000000000))  # nanoseconds
            
            if end_time:
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                params['end'] = str(int(end_time.timestamp() * 1000000000))  # nanoseconds
            
            print(f"Querying Loki: {query}")
            if start_time or end_time:
                print(f"Time range: {start_time} to {end_time}")
            
            # Make request
            response = self.session.get(
                f"{self.loki_url}/loki/api/v1/query_range",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Loki query failed: {e}")
            raise
    
    def logs_to_dataframe(self, loki_response):
        """Convert Loki response to pandas DataFrame"""
        try:
            data = loki_response.get('data', {})
            result = data.get('result', [])
            
            if not result:
                print("⚠️ No log data returned")
                return pd.DataFrame()
            
            rows = []
            for stream in result:
                labels = stream.get('labels', {})
                values = stream.get('values', [])
                
                for timestamp_ns, log_line in values:
                    # Convert nanosecond timestamp to datetime
                    timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1000000000, tz=timezone.utc)
                    
                    row = {
                        'timestamp': timestamp,
                        'log_line': log_line,
                        **labels  # Add all labels as columns
                    }
                    rows.append(row)
            
            df = pd.DataFrame(rows)
            
            # Sort by timestamp
            if not df.empty:
                df = df.sort_values('timestamp')
                df.reset_index(drop=True, inplace=True)
            
            print(f"✅ Converted {len(df)} log entries to DataFrame")
            return df
            
        except Exception as e:
            print(f"❌ Failed to convert logs to DataFrame: {e}")
            raise
    
    def get_metrics(self, query, start_time=None, end_time=None, step="1m"):
        """Query Loki metrics using LogQL metric queries"""
        try:
            params = {
                'query': query,
                'step': step
            }
            
            if start_time:
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                params['start'] = str(int(start_time.timestamp() * 1000000000))
            
            if end_time:
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                params['end'] = str(int(end_time.timestamp() * 1000000000))
            
            print(f"Querying Loki metrics: {query}")
            
            response = self.session.get(
                f"{self.loki_url}/loki/api/v1/query_range",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            
            return response.json()
            
        except Exception as e:
            print(f"❌ Loki metrics query failed: {e}")
            raise
    
    def metrics_to_dataframe(self, loki_response):
        """Convert Loki metrics response to pandas DataFrame"""
        try:
            data = loki_response.get('data', {})
            result = data.get('result', [])
            
            if not result:
                print("⚠️ No metrics data returned")
                return pd.DataFrame()
            
            rows = []
            for series in result:
                labels = series.get('labels', {})
                values = series.get('values', [])
                
                for timestamp_ns, value in values:
                    timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1000000000, tz=timezone.utc)
                    
                    row = {
                        'timestamp': timestamp,
                        'value': float(value),
                        **labels
                    }
                    rows.append(row)
            
            df = pd.DataFrame(rows)
            
            if not df.empty:
                df = df.sort_values('timestamp')
                df.reset_index(drop=True, inplace=True)
            
            print(f"✅ Converted {len(df)} metric points to DataFrame")
            return df
            
        except Exception as e:
            print(f"❌ Failed to convert metrics to DataFrame: {e}")
            raise

def add_loki_args(parser):
    """Add Loki-specific arguments to the parser"""
    loki_group = parser.add_argument_group('Loki options')
    loki_group.add_argument("--loki-url", default="http://localhost:3100", 
                           help="Loki server URL (default: http://localhost:3100)")
    loki_group.add_argument("--loki-query", 
                           help="LogQL query (e.g., '{job=\"systemd-journal\"}')")
    loki_group.add_argument("--loki-metrics", 
                           help="LogQL metrics query (e.g., 'rate({job=\"app\"}[5m])')")
    loki_group.add_argument("--loki-labels", action="store_true",
                           help="List available Loki labels and exit")
    loki_group.add_argument("--loki-limit", type=int, default=1000,
                           help="Limit number of log entries (default: 1000)")
    loki_group.add_argument("--loki-step", default="1m",
                           help="Step size for metrics queries (default: 1m)")

def handle_loki_operations(args):
    """Handle Loki-specific operations"""
    loki = LokiExporter(args.loki_url)
    
    if args.loki_labels:
        success = loki.test_connection()
        if not success:
            sys.exit(1)
        return None
    
    if args.loki_query:
        print(f"Executing Loki log query: {args.loki_query}")
        
        response = loki.query_logs(
            args.loki_query, 
            args.start_time, 
            args.end_time, 
            args.loki_limit
        )
        
        df = loki.logs_to_dataframe(response)
        
        if df.empty:
            print("⚠️ No log data returned.")
            return df
        
        print(f"✅ Retrieved {len(df)} log entries with columns: {list(df.columns)}")
        return df
    
    if args.loki_metrics:
        print(f"Executing Loki metrics query: {args.loki_metrics}")
        
        response = loki.get_metrics(
            args.loki_metrics,
            args.start_time,
            args.end_time,
            args.loki_step
        )
        
        df = loki.metrics_to_dataframe(response)
        
        if df.empty:
            print("⚠️ No metrics data returned.")
            return df
        
        print(f"✅ Retrieved {len(df)} metric points with columns: {list(df.columns)}")
        return df
    
    return None
