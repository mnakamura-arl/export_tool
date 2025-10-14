#!/usr/bin/env python3
"""
Loki log export functionality for the sensor data export tool.
"""

import requests
import json
import pandas as pd
from datetime import datetime, timezone, timedelta
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
    
    def query_logs(self, query, start_time=None, end_time=None, limit=None):
        """Query Loki logs using LogQL with automatic pagination. If limit=None, gets ALL data."""
        # If no limit specified, dump everything
        if limit is None:
            print(f"🚛 DUMP MODE: Getting ALL available data (no limit)...")
            return self._dump_all_data(query, start_time, end_time)
        
        # If limit is <= 5000, use single query
        if limit <= 5000:
            return self._single_query(query, start_time, end_time, limit)
        
        # If limit > 5000, use pagination
        print(f"📄 Large query detected (limit={limit}). Using pagination with 5000-record chunks...")
        return self._paginated_query(query, start_time, end_time, limit)
    
    def _dump_all_data(self, query, start_time=None, end_time=None):
        """Dump ALL available data using pagination (no limit)"""
        try:
            all_results = []
            chunk_size = 5000  # Max safe limit per request
            total_retrieved = 0
            page = 1
            
            # Convert time strings to datetime objects
            if start_time and isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            if end_time and isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            current_end = end_time
            
            print(f"🔍 Scanning for all available data...")
            if start_time:
                print(f"   Start time: {start_time}")
            if end_time:
                print(f"   End time: {end_time}")
            else:
                print(f"   End time: now (latest data)")
            
            while True:  # Keep going until no more data
                print(f"  📦 Chunk {page}: fetching up to {chunk_size} records...")
                
                # Build params for this chunk
                params = {
                    'query': query,
                    'limit': chunk_size,
                    'direction': 'backward'  # Get most recent first
                }
                
                if start_time:
                    params['start'] = str(int(start_time.timestamp() * 1000000000))
                if current_end:
                    params['end'] = str(int(current_end.timestamp() * 1000000000))
                
                # Make request
                response = self.session.get(
                    f"{self.loki_url}/loki/api/v1/query_range",
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                page_data = response.json()
                page_results = page_data.get('data', {}).get('result', [])
                
                if not page_results:
                    print(f"  ✅ No more data streams on chunk {page}")
                    break
                
                # Count entries in this page
                page_entries = sum(len(stream.get('values', [])) for stream in page_results)
                
                if page_entries == 0:
                    print(f"  ✅ No more log entries on chunk {page}")
                    break
                
                all_results.extend(page_results)
                total_retrieved += page_entries
                
                print(f"  📊 Chunk {page}: +{page_entries} entries (total: {total_retrieved:,})")
                
                # If we got less than requested, we've reached the end
                if page_entries < chunk_size:
                    print(f"  🏁 Reached end of available data (chunk {page} was partial)")
                    break
                
                # Update end time for next page (get older data)
                oldest_timestamp = None
                for stream in page_results:
                    for timestamp_ns, _ in stream.get('values', []):
                        timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1000000000, tz=timezone.utc)
                        if oldest_timestamp is None or timestamp < oldest_timestamp:
                            oldest_timestamp = timestamp
                
                if oldest_timestamp:
                    # Move end time to just before the oldest record we got
                    current_end = oldest_timestamp - timedelta(microseconds=1)
                    if page % 5 == 0:  # Show progress every 5 chunks
                        print(f"    ⏰ Now fetching data older than {oldest_timestamp}")
                else:
                    print(f"  ⚠️ Could not determine oldest timestamp, stopping")
                    break
                
                page += 1
                
                # Safety check to prevent infinite loops
                if page > 1000:  # Max 1000 chunks = 5M records
                    print(f"  🛑 Safety limit reached (1000 chunks = ~5M records)")
                    print(f"     If you have more data, add a --start-time filter")
                    break
            
            # Combine all results
            combined_response = {
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": all_results,
                    "stats": {
                        "summary": {
                            "totalEntriesReturned": total_retrieved,
                            "chunks": page - 1
                        }
                    }
                }
            }
            
            print(f"🎉 DUMP COMPLETE!")
            print(f"   📊 Total entries: {total_retrieved:,}")
            print(f"   📦 Chunks processed: {page-1}")
            if total_retrieved >= 5000000:
                print(f"   💡 Tip: Use --start-time to get specific time ranges for very large datasets")
            
            return combined_response
            
        except Exception as e:
            print(f"❌ Data dump failed: {e}")
            raise
    
    def _single_query(self, query, start_time=None, end_time=None, limit=1000):
        """Execute a single Loki query"""
        try:
            # Build query parameters
            params = {
                'query': query,
                'limit': limit
            }
            
            # Add time range if specified
            if start_time:
                if isinstance(start_time, str):
                    start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
                params['start'] = str(int(start_time.timestamp() * 1000000000))
            
            if end_time:
                if isinstance(end_time, str):
                    end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
                params['end'] = str(int(end_time.timestamp() * 1000000000))
            
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
    
    def _paginated_query(self, query, start_time=None, end_time=None, target_limit=10000):
        """Execute paginated Loki queries to get more than 5000 records"""
        try:
            all_results = []
            chunk_size = 5000  # Max safe limit
            total_retrieved = 0
            page = 1
            
            # Convert time strings to datetime objects
            if start_time and isinstance(start_time, str):
                start_time = datetime.fromisoformat(start_time.replace('Z', '+00:00'))
            if end_time and isinstance(end_time, str):
                end_time = datetime.fromisoformat(end_time.replace('Z', '+00:00'))
            
            current_end = end_time
            
            print(f"Starting paginated query: target={target_limit}, chunk_size={chunk_size}")
            
            while total_retrieved < target_limit:
                remaining = target_limit - total_retrieved
                current_limit = min(chunk_size, remaining)
                
                print(f"  📄 Page {page}: fetching {current_limit} records...")
                
                # Build params for this chunk
                params = {
                    'query': query,
                    'limit': current_limit,
                    'direction': 'backward'  # Get most recent first
                }
                
                if start_time:
                    params['start'] = str(int(start_time.timestamp() * 1000000000))
                if current_end:
                    params['end'] = str(int(current_end.timestamp() * 1000000000))
                
                # Make request
                response = self.session.get(
                    f"{self.loki_url}/loki/api/v1/query_range",
                    params=params,
                    timeout=30
                )
                response.raise_for_status()
                
                page_data = response.json()
                page_results = page_data.get('data', {}).get('result', [])
                
                if not page_results:
                    print(f"  ✅ No more data on page {page}")
                    break
                
                # Count entries in this page
                page_entries = sum(len(stream.get('values', [])) for stream in page_results)
                
                if page_entries == 0:
                    print(f"  ✅ No more entries on page {page}")
                    break
                
                all_results.extend(page_results)
                total_retrieved += page_entries
                
                print(f"  📊 Page {page}: {page_entries} entries (total: {total_retrieved})")
                
                # If we got less than requested, we've reached the end
                if page_entries < current_limit:
                    print(f"  ✅ Reached end of data (page {page} had fewer entries)")
                    break
                
                # Update end time for next page (get older data)
                # Find the oldest timestamp from this page
                oldest_timestamp = None
                for stream in page_results:
                    for timestamp_ns, _ in stream.get('values', []):
                        timestamp = datetime.fromtimestamp(int(timestamp_ns) / 1000000000, tz=timezone.utc)
                        if oldest_timestamp is None or timestamp < oldest_timestamp:
                            oldest_timestamp = timestamp
                
                if oldest_timestamp:
                    # Move end time to just before the oldest record we got
                    current_end = oldest_timestamp - timedelta(microseconds=1)
                else:
                    break
                
                page += 1
                
                # Safety check
                if page > 100:  # Max 100 pages = 500k records
                    print(f"  ⚠️ Stopping after 100 pages for safety")
                    break
            
            # Combine all results into single response format
            combined_response = {
                "status": "success",
                "data": {
                    "resultType": "streams",
                    "result": all_results,
                    "stats": {
                        "summary": {
                            "totalEntriesReturned": total_retrieved,
                            "pages": page - 1
                        }
                    }
                }
            }
            
            print(f"✅ Pagination complete: {total_retrieved} total entries across {page-1} pages")
            return combined_response
            
        except Exception as e:
            print(f"❌ Paginated query failed: {e}")
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
    loki_group.add_argument("--loki-limit", type=int, default=None,
                           help="Limit number of log entries (default: unlimited - gets everything)")
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
        
        # Use the limit if specified, otherwise None for unlimited dump
        limit = args.loki_limit if hasattr(args, 'loki_limit') else None
        
        response = loki.query_logs(
            args.loki_query, 
            args.start_time, 
            args.end_time, 
            limit  # This will be None for unlimited
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
