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
import json
import os
import re
import sys
from datetime import datetime
from typing import List, Dict, Optional, Tuple


def extract_timestamp(line: str) -> Optional[str]:
    """Extract timestamp from a log line."""
    # Pattern to match Cassandra log timestamps:
    # Format 1: 2025-10-03 10:08:32,368 (with space and comma)
    # Format 2: 2025-11-28 00:04:21.835Z (with space, dot, and Z)
    # Format 3: 2025-11-28 00:04:21.835 (with space and dot, no Z)
    # Format 4: 2025-11-28T00:04:21.835Z (ISO 8601 with T and Z)
    # Format 5: 2025-11-28T00:04:21.835 (ISO 8601 with T, no Z)
    timestamp_patterns = [
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})',  # Format with space and comma
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d{3})',  # Format with space and dot (with or without Z)
        r'(\d{4}-\d{2}-\d{2})T(\d{2}:\d{2}:\d{2}\.\d{3})',  # ISO 8601 format with T (with or without Z)
    ]
    for pattern in timestamp_patterns:
        match = re.search(pattern, line)
        if match:
            # If we matched the ISO 8601 format (with T), convert T to space for consistency
            if len(match.groups()) == 2:
                return f"{match.group(1)} {match.group(2)}"
            else:
                return match.group(1)
    return None


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
            
            # If still no timestamp found, check the header line itself
            if not timestamp:
                # Check the original line (not stripped) to ensure we have all characters
                timestamp = extract_timestamp(lines[i])
            
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


def parse_timestamp_to_datetime(timestamp_str: str) -> Optional[datetime]:
    """Parse timestamp string to datetime object."""
    # Handle various timestamp formats
    formats = [
        '%Y-%m-%d %H:%M:%S,%f',  # Format with comma
        '%Y-%m-%d %H:%M:%S.%f',  # Format with dot
        '%Y-%m-%d %H:%M:%S.%fZ',  # Format with dot and Z
        '%Y-%m-%dT%H:%M:%S.%f',  # ISO 8601 with T
        '%Y-%m-%dT%H:%M:%S.%fZ',  # ISO 8601 with T and Z
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(timestamp_str, fmt)
        except ValueError:
            continue
    
    return None


def infer_node_name(file_path: str) -> str:
    """Infer node name from file path."""
    # Try to extract node name from filename patterns like:
    # node1-system.log, node1_system.log, node-1.log, etc.
    basename = os.path.basename(file_path)
    name_without_ext = os.path.splitext(basename)[0]
    
    # Try patterns: node1-system, node1_system, node-1, etc.
    patterns = [
        r'^([^_-]+)[_-]',  # Match prefix before first _ or -
        r'^([^\.]+)',       # Match everything before first dot
    ]
    
    for pattern in patterns:
        match = re.match(pattern, name_without_ext)
        if match:
            return match.group(1)
    
    # Fallback to filename without extension
    return name_without_ext


def generate_html_visualization(all_sections: List[Tuple[str, str, List[Dict[str, str]]]], 
                                output_file: str, 
                                target_pools: Optional[List[str]] = None) -> None:
    """Generate an interactive HTML visualization of completed metrics over time.
    
    Args:
        all_sections: List of tuples (node_name, timestamp, pool_data)
        output_file: Path to output HTML file
        target_pools: Optional list of pool names to filter
    """
    # Collect data: (pool_name, node_name) -> [(timestamp, completed_value), ...]
    pool_node_data_map: Dict[Tuple[str, str], List[Tuple[datetime, int]]] = {}
    
    for node_name, timestamp_str, pool_data in all_sections:
        # Filter pools if specified
        filtered_data = filter_pools(pool_data, target_pools) if target_pools else pool_data
        
        # Parse timestamp
        dt = parse_timestamp_to_datetime(timestamp_str)
        if not dt:
            continue
        
        # Extract completed metric for each pool
        for pool in filtered_data:
            pool_name = pool['pool_name']
            completed_str = pool.get('completed', '0')
            
            # Handle N/A or invalid values
            try:
                completed_value = int(completed_str) if completed_str != 'N/A' else 0
            except (ValueError, TypeError):
                completed_value = 0
            
            key = (pool_name, node_name)
            if key not in pool_node_data_map:
                pool_node_data_map[key] = []
            
            pool_node_data_map[key].append((dt, completed_value))
    
    if not pool_node_data_map:
        print("No pool data available for visualization.", file=sys.stderr)
        return
    
    # Sort data by timestamp for each pool-node combination
    for key in pool_node_data_map:
        pool_node_data_map[key].sort(key=lambda x: x[0])
    
    # Prepare data for Plotly
    # Group by pool name first, then create traces for each node
    pool_names = sorted(set(pool_name for pool_name, _ in pool_node_data_map.keys()))
    node_names = sorted(set(node_name for _, node_name in pool_node_data_map.keys()))
    traces_data = []
    trace_metadata = []  # Store pool and node info for each trace
    
    # Create a trace for each pool-node combination
    for pool_name in pool_names:
        for node_name in node_names:
            key = (pool_name, node_name)
            if key in pool_node_data_map:
                data_points = pool_node_data_map[key]
                timestamps = [dt.isoformat() for dt, _ in data_points]
                completed_values = [val for _, val in data_points]
                
                # Create trace name with node label
                trace_name = f"{pool_name} ({node_name})" if len(node_names) > 1 else pool_name
                
                traces_data.append({
                    'name': trace_name,
                    'x': timestamps,
                    'y': completed_values,
                    'type': 'scatter',
                    'mode': 'lines+markers',
                    'visible': False  # All traces hidden by default
                })
                
                # Store metadata for this trace
                trace_metadata.append({
                    'pool_name': pool_name,
                    'node_name': node_name,
                    'trace_index': len(traces_data) - 1
                })
    
    # Generate HTML
    html_content = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Pool Metrics - Completed Over Time</title>
    <script src="https://cdn.plot.ly/plotly-latest.min.js"></script>
    <style>
        body {{
            font-family: Arial, sans-serif;
            margin: 20px;
            background-color: #f5f5f5;
        }}
        .container {{
            max-width: 1400px;
            margin: 0 auto;
            background-color: white;
            padding: 20px;
            border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        h1 {{
            color: #333;
            margin-bottom: 20px;
        }}
        .controls {{
            margin-bottom: 20px;
            padding: 15px;
            background-color: #f9f9f9;
            border-radius: 5px;
            border: 1px solid #ddd;
        }}
        .controls h2 {{
            margin-top: 0;
            font-size: 16px;
            color: #555;
        }}
        .control-row {{
            margin-bottom: 15px;
        }}
        .control-row label {{
            display: block;
            margin-bottom: 5px;
            font-weight: bold;
            color: #555;
        }}
        select {{
            padding: 8px 12px;
            font-size: 14px;
            border: 1px solid #ddd;
            border-radius: 4px;
            background-color: white;
            cursor: pointer;
            min-width: 200px;
        }}
        select:hover {{
            border-color: #007bff;
        }}
        .node-checkbox-group {{
            display: flex;
            flex-wrap: wrap;
            gap: 15px;
            margin-top: 10px;
        }}
        .node-checkbox-item {{
            display: flex;
            align-items: center;
            gap: 5px;
        }}
        .node-checkbox-item input[type="checkbox"] {{
            cursor: pointer;
        }}
        .node-checkbox-item label {{
            cursor: pointer;
            user-select: none;
            font-weight: normal;
        }}
        .hidden {{
            display: none;
        }}
        #plotly-chart {{
            width: 100%;
            height: 600px;
        }}
    </style>
</head>
<body>
    <div class="container">
        <h1>Pool Metrics - Completed Over Time</h1>
        
        <div class="controls">
            <h2>Select Pool and Nodes:</h2>
            <div class="control-row">
                <label for="pool-selector">Select Pool:</label>
                <select id="pool-selector" onchange="onPoolSelect()">
                    <option value="">-- Select a Pool --</option>
                    <!-- Pool options will be generated here -->
                </select>
            </div>
            <div class="control-row" id="node-controls" style="display: none;">
                <label>Select Nodes:</label>
                <div class="node-checkbox-group" id="node-checkbox-group">
                    <!-- Node checkboxes will be generated here -->
                </div>
            </div>
        </div>
        
        <div id="plotly-chart"></div>
    </div>
    
    <script>
        // Data for all traces
        const tracesData = {json.dumps(traces_data)};
        
        // Metadata for traces: pool_name, node_name, trace_index
        const traceMetadata = {json.dumps(trace_metadata)};
        
        // Get unique pool names
        const poolNames = {json.dumps(pool_names)};
        
        // Get unique node names
        const nodeNames = {json.dumps(node_names)};
        
        // Populate pool selector dropdown
        const poolSelector = document.getElementById('pool-selector');
        poolNames.forEach(poolName => {{
            const option = document.createElement('option');
            option.value = poolName;
            option.textContent = poolName;
            poolSelector.appendChild(option);
        }});
        
        // Create the plot
        const layout = {{
            title: 'Completed Tasks Over Time',
            xaxis: {{
                title: 'Timestamp',
                type: 'date'
            }},
            yaxis: {{
                title: 'Completed Tasks'
            }},
            hovermode: 'closest',
            legend: {{
                orientation: 'h',
                y: -0.2,
                x: 0.5,
                xanchor: 'center'
            }},
            margin: {{ l: 60, r: 30, t: 60, b: 100 }}
        }};
        
        const config = {{
            responsive: true,
            displayModeBar: true
        }};
        
        Plotly.newPlot('plotly-chart', tracesData, layout, config);
        
        // Handle pool selection
        function onPoolSelect() {{
            const selectedPool = document.getElementById('pool-selector').value;
            const nodeControls = document.getElementById('node-controls');
            const nodeCheckboxGroup = document.getElementById('node-checkbox-group');
            
            // Hide all traces first
            const allTraceIndices = tracesData.map((_, i) => i);
            const visibilityUpdates = allTraceIndices.map(() => false);
            Plotly.restyle('plotly-chart', {{ visible: visibilityUpdates }}, allTraceIndices);
            
            // Clear existing checkboxes
            nodeCheckboxGroup.innerHTML = '';
            
            if (!selectedPool) {{
                // No pool selected, hide node controls
                nodeControls.style.display = 'none';
                return;
            }}
            
            // Show node controls
            nodeControls.style.display = 'block';
            
            // Find nodes that have data for the selected pool
            const availableNodes = new Set();
            traceMetadata.forEach(meta => {{
                if (meta.pool_name === selectedPool) {{
                    availableNodes.add(meta.node_name);
                }}
            }});
            
            // Create checkboxes for available nodes
            const sortedNodes = Array.from(availableNodes).sort();
            sortedNodes.forEach(nodeName => {{
                const checkboxItem = document.createElement('div');
                checkboxItem.className = 'node-checkbox-item';
                
                const checkbox = document.createElement('input');
                checkbox.type = 'checkbox';
                checkbox.id = 'node-' + nodeName;
                checkbox.value = nodeName;
                checkbox.checked = true;  // Select all nodes by default
                checkbox.onchange = function() {{ toggleNode(selectedPool, nodeName, checkbox.checked); }};
                
                const label = document.createElement('label');
                label.htmlFor = 'node-' + nodeName;
                label.textContent = nodeName;
                
                checkboxItem.appendChild(checkbox);
                checkboxItem.appendChild(label);
                nodeCheckboxGroup.appendChild(checkboxItem);
                
                // Show the trace for this node by default
                toggleNode(selectedPool, nodeName, true);
            }});
        }}
        
        // Toggle node visibility for selected pool
        function toggleNode(poolName, nodeName, isVisible) {{
            // Find all traces for this pool-node combination
            const traceIndices = [];
            traceMetadata.forEach((meta, index) => {{
                if (meta.pool_name === poolName && meta.node_name === nodeName) {{
                    traceIndices.push(meta.trace_index);
                }}
            }});
            
            // Update visibility for these traces
            if (traceIndices.length > 0) {{
                const visibilityUpdates = traceIndices.map(() => isVisible);
                Plotly.restyle('plotly-chart', {{ visible: visibilityUpdates }}, traceIndices);
            }}
        }}
    </script>
</body>
</html>"""
    
    # Write HTML file
    try:
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(html_content)
        print(f"Visualization saved to: {output_file}", file=sys.stderr)
    except Exception as e:
        print(f"Error writing visualization file: {e}", file=sys.stderr)
        raise


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
    python parse_pool_metrics.py --visualize --nodes "node1,node2,node3" node1.log node2.log node3.log
    python parse_pool_metrics.py --visualize --pools "CompactionExecutor" node1-system.log node2-system.log
        """
    )
    
    parser.add_argument('files', nargs='+', help='Log files to parse')
    parser.add_argument('--pools', help='Comma-separated list of pool names to filter (case-insensitive)')
    parser.add_argument('--metrics', help='Comma-separated list of metrics to include (Active, Pending, Backpressure, Delayed, Shared, Stolen, Completed, Blocked, All_Time_Blocked)')
    parser.add_argument('--output', '-o', help='Output CSV file (optional, defaults to pool_metrics_<timestamp>.csv)')
    parser.add_argument('--visualize', '--graph', dest='visualize', action='store_true', help='Generate interactive HTML visualization of completed metrics over time')
    parser.add_argument('--html-output', help='Output HTML file for visualization (optional, defaults to pool_metrics_<timestamp>.html)')
    parser.add_argument('--nodes', help='Comma-separated list of node names/labels matching file order (optional, will infer from filenames if not provided)')
    
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
    
    # Parse node labels
    node_labels = []
    if args.nodes:
        node_labels = [node.strip() for node in args.nodes.split(',') if node.strip()]
        if len(node_labels) != len(args.files):
            print(f"Warning: Number of node labels ({len(node_labels)}) doesn't match number of files ({len(args.files)}).", file=sys.stderr)
            print("Using provided labels and inferring remaining from filenames.", file=sys.stderr)
    
    # Collect all pool statistics from all files with node information
    all_sections = []
    for idx, file_path in enumerate(args.files):
        # Determine node name for this file
        if idx < len(node_labels):
            node_name = node_labels[idx]
        else:
            node_name = infer_node_name(file_path)
        
        sections = find_pool_statistics_sections(file_path)
        # Add node name to each section: (node_name, timestamp, pool_data)
        for timestamp, pool_data in sections:
            all_sections.append((node_name, timestamp, pool_data))
    
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
    
    # Check if we have multiple nodes
    unique_nodes = set(node_name for node_name, _, _ in all_sections)
    has_multiple_nodes = len(unique_nodes) > 1
    
    try:
        writer = csv.writer(output_file)
        
        # If no specific pools are requested, show all pools as rows (original format)
        if not target_pools:
            # Write header for row-based format
            header = ['timestamp', 'pool_name']
            if has_multiple_nodes:
                header.insert(1, 'node_name')  # Insert after timestamp
            
            for col in selected_columns:
                header.append(col.lower())
            writer.writerow(header)
            
            # Write data rows
            for node_name, timestamp, pool_data in all_sections:
                for pool in pool_data:
                    row = [timestamp]
                    if has_multiple_nodes:
                        row.append(node_name)
                    row.append(pool['pool_name'])
                    for col in selected_columns:
                        row.append(pool[column_mapping[col]])
                    writer.writerow(row)
        else:
            # Column-based format for specific pools
            # First, collect all unique pool names that match our filters
            all_pool_names = set()
            for _, _, pool_data in all_sections:
                filtered_pools = filter_pools(pool_data, target_pools)
                for pool in filtered_pools:
                    all_pool_names.add(pool['pool_name'])
            
            if not all_pool_names:
                print("No matching pools found for the specified filters.", file=sys.stderr)
                return 1
            
            # Create column headers
            column_names = ['timestamp']
            if has_multiple_nodes:
                column_names.append('node_name')
            for pool_name in sorted(all_pool_names):
                for col in selected_columns:
                    column_names.append(f"{pool_name}-{col}")
            
            writer.writerow(column_names)
            
            # Write data rows - one row per timestamp
            for node_name, timestamp, pool_data in all_sections:
                filtered_pools = filter_pools(pool_data, target_pools)
                
                # Create a dictionary for quick lookup
                pool_dict = {pool['pool_name']: pool for pool in filtered_pools}
                
                # Build the row
                row = [timestamp]
                if has_multiple_nodes:
                    row.append(node_name)
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
    
    # Generate visualization if requested
    if args.visualize:
        if args.html_output:
            html_filename = args.html_output
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            html_filename = f"pool_metrics_{timestamp}.html"
        
        try:
            generate_html_visualization(all_sections, html_filename, target_pools)
        except Exception as e:
            print(f"Error generating visualization: {e}", file=sys.stderr)
            return 1
    
    return 0


if __name__ == '__main__':
    sys.exit(main())
