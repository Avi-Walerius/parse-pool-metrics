#!/usr/bin/env python3
"""
Cassandra Pool Metrics Parser

This script parses pool metrics from Cassandra system logs and outputs them to CSV format.
It searches for the pattern:
Pool Name                                       Active        Pending   Backpressure   Delayed      Shared      Stolen      Completed   Blocked  All Time Blocked

Usage:
    python parse_pool_metrics.py [--pools POOL1,POOL2,...] file1.log file2.log ...
"""

import argparse
import csv
import re
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple


def extract_timestamp(line: str) -> Optional[str]:
    """Extract timestamp from a log line."""
    # Pattern to match Cassandra log timestamps: 2025-10-03 10:08:32,368
    timestamp_pattern = r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})'
    match = re.search(timestamp_pattern, line)
    return match.group(1) if match else None


def parse_pool_line(line: str) -> Optional[Dict[str, str]]:
    """Parse a single pool statistics line."""
    # Split the line into columns, handling variable spacing
    # The columns can be either:
    # Format 1: Pool Name, Active, Pending, Completed, Blocked, All Time Blocked (6 columns)
    # Format 2: Pool Name, Active, Pending, Backpressure, Delayed, Shared, Stolen, Completed, Blocked, All Time Blocked (10 columns)
    parts = line.strip().split()
    
    if len(parts) < 6:
        return None
    
    # Pool name might contain spaces, so we need to be careful
    # The first column is the pool name, and the rest are numeric values
    # We'll split on multiple spaces to separate columns properly
    columns = re.split(r'\s{2,}', line.strip())
    
    if len(columns) < 6:
        return None
    
    # Determine format based on number of columns
    if len(columns) >= 10:
        # Full format with all columns
        return {
            'pool_name': columns[0].strip(),
            'active': columns[1].strip(),
            'pending': columns[2].strip(),
            'backpressure': columns[3].strip(),
            'delayed': columns[4].strip(),
            'shared': columns[5].strip(),
            'stolen': columns[6].strip(),
            'completed': columns[7].strip(),
            'blocked': columns[8].strip(),
            'all_time_blocked': columns[9].strip()
        }
    else:
        # Short format (6 columns): Pool Name, Active, Pending, Completed, Blocked, All Time Blocked
        return {
            'pool_name': columns[0].strip(),
            'active': columns[1].strip(),
            'pending': columns[2].strip(),
            'backpressure': '0',  # Not present in this log format
            'delayed': '0',        # Not present in this log format
            'shared': '0',         # Not present in this log format
            'stolen': '0',         # Not present in this log format
            'completed': columns[3].strip(),
            'blocked': columns[4].strip(),
            'all_time_blocked': columns[5].strip()
        }


def find_pool_statistics_sections(file_path: str) -> List[Tuple[str, List[Dict[str, str]]]]:
    """Find all pool statistics sections in a log file."""
    sections = []
    
    try:
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            lines = f.readlines()
    except Exception as e:
        print(f"Error reading file {file_path}: {e}", file=sys.stderr)
        return sections
    
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        
        # Look for the header line - check for Pool Name, Active, and Pending
        if "Pool Name" in line and "Active" in line and "Pending" in line:
            # Try to find timestamp - first check the line before the header
            timestamp = None
            
            # Check if the line before contains StatusLogger.java
            if i > 0:
                prev_line = lines[i-1].strip()
                if "StatusLogger.java" in prev_line:
                    timestamp = extract_timestamp(prev_line)
            
            # If no timestamp found before, check after the header
            if not timestamp:
                # Look for StatusLogger.java in the next few lines
                j = i + 1
                while j < len(lines) and j < i + 10:  # Look up to 10 lines ahead
                    check_line = lines[j].strip()
                    if "StatusLogger.java" in check_line:
                        timestamp = extract_timestamp(check_line)
                        break
                    # Stop if we hit another log entry or empty line
                    if not check_line or re.match(r'^\d{4}-\d{2}-\d{2}', check_line) or re.match(r'^(INFO|WARN|ERROR|DEBUG)', check_line):
                        break
                    j += 1
            
            # If still no timestamp found, return error
            if not timestamp:
                print(f"Error: No timestamp found for pool statistics section at line {i+1} in {file_path}", file=sys.stderr)
                i += 1
                continue
            
            # Parse pool data - check if data comes before or after the header
            pool_data = []
            
            # First, try to find data after the header
            j = i + 1
            while j < len(lines):
                data_line = lines[j].strip()
                
                # Stop if we hit an empty line or a new log entry
                if not data_line or re.match(r'^\d{4}-\d{2}-\d{2}', data_line) or re.match(r'^(INFO|WARN|ERROR|DEBUG)', data_line):
                    break
                
                # Parse the pool line
                pool_info = parse_pool_line(data_line)
                if pool_info:
                    pool_data.append(pool_info)
                else:
                    # If we can't parse this line as pool data, stop
                    break
                
                j += 1
            
            # If no data found after header, try before the header
            if not pool_data:
                j = i - 1
                while j >= 0:
                    data_line = lines[j].strip()
                    
                    # Stop if we hit an empty line or a new log entry
                    if not data_line or re.match(r'^\d{4}-\d{2}-\d{2}', data_line) or re.match(r'^(INFO|WARN|ERROR|DEBUG)', data_line):
                        break
                    
                    # Parse the pool line
                    pool_info = parse_pool_line(data_line)
                    if pool_info:
                        pool_data.insert(0, pool_info)  # Insert at beginning to maintain order
                    else:
                        # If we can't parse this line as pool data, stop
                        break
                    
                    j -= 1
            
            if pool_data:
                sections.append((timestamp, pool_data))
            
            i += 1
        else:
            i += 1
    
    return sections


def filter_pools(pool_data: List[Dict[str, str]], target_pools: List[str]) -> List[Dict[str, str]]:
    """Filter pool data to only include specified pool names."""
    if not target_pools:
        return pool_data
    
    target_pools_lower = [pool.lower() for pool in target_pools]
    return [pool for pool in pool_data if pool['pool_name'].lower() in target_pools_lower]


def main():
    parser = argparse.ArgumentParser(
        description='Parse Cassandra pool metrics from system logs and output to CSV',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python parse_pool_metrics.py system.log
    python parse_pool_metrics.py --pools "CompactionExecutor,GossipStage" system.log
    python parse_pool_metrics.py --pools "TPC" system1.log system2.log
    python parse_pool_metrics.py --metrics "Active,Completed,Blocked" system.log
    python parse_pool_metrics.py --pools "CompactionExecutor" --metrics "Active,Completed" system.log
        """
    )
    
    parser.add_argument('files', nargs='+', help='Log files to parse')
    parser.add_argument('--pools', help='Comma-separated list of pool names to filter (case-insensitive)')
    parser.add_argument('--metrics', help='Comma-separated list of metrics to include (Active, Pending, Backpressure, Delayed, Shared, Stolen, Completed, Blocked, All_Time_Blocked)')
    parser.add_argument('--output', '-o', help='Output CSV file (optional, defaults to pool_metrics_<timestamp>.csv)')
    
    args = parser.parse_args()
    
    # Parse target pools
    target_pools = []
    if args.pools:
        target_pools = [pool.strip() for pool in args.pools.split(',') if pool.strip()]
    
    # Parse and validate columns
    valid_columns = ['Active', 'Pending', 'Backpressure', 'Delayed', 'Shared', 'Stolen', 'Completed', 'Blocked', 'All_Time_Blocked']
    selected_columns = valid_columns.copy()  # Default to all columns
    
    if args.metrics:
        requested_columns = [col.strip() for col in args.metrics.split(',') if col.strip()]
        # Validate requested columns
        invalid_columns = [col for col in requested_columns if col not in valid_columns]
        if invalid_columns:
            print(f"Error: Invalid columns specified: {', '.join(invalid_columns)}", file=sys.stderr)
            print(f"Valid columns are: {', '.join(valid_columns)}", file=sys.stderr)
            return 1
        selected_columns = requested_columns
    
    # Collect all pool statistics from all files
    all_sections = []
    for file_path in args.files:
        sections = find_pool_statistics_sections(file_path)
        all_sections.extend(sections)
    
    if not all_sections:
        print("No pool statistics found in the provided files.", file=sys.stderr)
        return 1
    
    # Map column names to pool data keys
    column_mapping = {
        'Active': 'active',
        'Pending': 'pending', 
        'Backpressure': 'backpressure',
        'Delayed': 'delayed',
        'Shared': 'shared',
        'Stolen': 'stolen',
        'Completed': 'completed',
        'Blocked': 'blocked',
        'All_Time_Blocked': 'all_time_blocked'
    }
    
    # Prepare CSV output
    if args.output:
        output_file = open(args.output, 'w', newline='')
    else:
        # Create default filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        default_filename = f"pool_metrics_{timestamp}.csv"
        output_file = open(default_filename, 'w', newline='')
        print(f"Output will be saved to: {default_filename}", file=sys.stderr)
    
    try:
        writer = csv.writer(output_file)
        
        # If no specific pools are requested, show all pools as rows (original format)
        if not target_pools:
            # Write header for row-based format
            header = ['timestamp', 'pool_name']
            
            for col in selected_columns:
                header.append(col.lower())
            writer.writerow(header)
            
            # Write data rows
            for timestamp, pool_data in all_sections:
                for pool in pool_data:
                    row = [timestamp, pool['pool_name']]
                    for col in selected_columns:
                        row.append(pool[column_mapping[col]])
                    writer.writerow(row)
        else:
            # Column-based format for specific pools
            # First, collect all unique pool names that match our filters
            all_pool_names = set()
            for timestamp, pool_data in all_sections:
                filtered_pools = filter_pools(pool_data, target_pools)
                for pool in filtered_pools:
                    all_pool_names.add(pool['pool_name'])
            
            if not all_pool_names:
                print("No matching pools found for the specified filters.", file=sys.stderr)
                return 1
            
            # Create column headers
            column_names = ['timestamp']
            for pool_name in sorted(all_pool_names):
                for col in selected_columns:
                    column_names.append(f"{pool_name}-{col}")
            
            writer.writerow(column_names)
            
            # Write data rows - one row per timestamp
            for timestamp, pool_data in all_sections:
                filtered_pools = filter_pools(pool_data, target_pools)
                
                # Create a dictionary for quick lookup
                pool_dict = {pool['pool_name']: pool for pool in filtered_pools}
                
                # Build the row
                row = [timestamp]
                for pool_name in sorted(all_pool_names):
                    if pool_name in pool_dict:
                        pool = pool_dict[pool_name]
                        for col in selected_columns:
                            row.append(pool[column_mapping[col]])
                    else:
                        # Fill with N/A if pool not found in this timestamp
                        row.extend(['N/A'] * len(selected_columns))
                
                writer.writerow(row)
    
    finally:
        output_file.close()
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
