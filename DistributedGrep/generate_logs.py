"""
Generate Test Log Files

Creates sample log files for testing the distributed grep system.
"""

import os
import random
from datetime import datetime, timedelta

LOG_LEVELS = ['INFO', 'WARN', 'ERROR', 'DEBUG']
MESSAGES = {
    'INFO': [
        'Server started successfully',
        'Request received from client',
        'Connection established',
        'Cache hit for key',
        'User logged in',
        'Session created',
        'Database query completed',
        'File uploaded successfully',
        'Configuration loaded',
        'Health check passed',
    ],
    'WARN': [
        'High latency detected',
        'Memory usage above 80%',
        'Slow database query',
        'Invalid token received',
        'Rate limit approaching',
        'Deprecated API called',
        'Connection pool running low',
        'Disk space below 20%',
    ],
    'ERROR': [
        'Connection failed',
        'Authentication failed',
        'Database connection timeout',
        'File not found',
        'Permission denied',
        'Invalid request format',
        'Service unavailable',
        'Disk full',
        'Out of memory',
        'Network unreachable',
    ],
    'DEBUG': [
        'Processing request',
        'Parsing JSON payload',
        'Executing query',
        'Building response',
        'Validating input',
    ],
}


def generate_log_line(vm_id, timestamp):
    """Generate a single log line."""
    level = random.choices(
        LOG_LEVELS,
        weights=[50, 20, 15, 15],  # INFO most common, then WARN, ERROR, DEBUG
        k=1
    )[0]
    message = random.choice(MESSAGES[level])
    return f"{timestamp.isoformat()} [{level}] VM{vm_id}: {message}"


def generate_log_file(vm_id, num_lines=100):
    """Generate a log file for a specific VM."""
    lines = []
    base_time = datetime.now() - timedelta(hours=1)

    for i in range(num_lines):
        timestamp = base_time + timedelta(seconds=i * random.randint(1, 5))
        lines.append(generate_log_line(vm_id, timestamp))

    return '\n'.join(lines)


def main():
    # Create logs directory if it doesn't exist
    os.makedirs('logs', exist_ok=True)

    print("Generating test log files...")

    for vm_id in range(1, 11):
        log_file = f'logs/machine.{vm_id}.log'
        content = generate_log_file(vm_id, num_lines=100)

        with open(log_file, 'w') as f:
            f.write(content)

        print(f"  Created {log_file} (100 lines)")

    print("\nDone! Generated 10 log files in logs/ directory.")
    print("\nSample queries to try:")
    print("  python client.py 'ERROR'")
    print("  python client.py -i 'connection'")
    print("  python client.py -e 'ERROR|WARN'")
    print("  python client.py -n 'VM1'")


if __name__ == "__main__":
    main()
