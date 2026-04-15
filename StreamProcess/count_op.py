#!/usr/bin/env python3
"""
AggregateByKey count operator - counts lines grouped by key.
Usage: ./count_op.py <task_id>
Input via stdin: group_key<TAB>value (where group_key was set by upstream filter)
Output to stdout: group_key<TAB>count

Note: This operator uses the INPUT KEY as the group key.
The upstream filter should set the key to the Nth column value for correct partitioning.

This is a stateful operator that maintains counts per task.
The task_id should include a unique run_id to ensure fresh state per job.

IMPORTANT: Before running a new job, clean up old state files:
    rm -f /tmp/count_state_*.json
"""
import sys
import json
import os

def main():
    # Task ID for unique state file - MUST be passed to ensure unique state per task
    # Format should be like "stage1_task0_<run_id>" to ensure uniqueness per job
    task_id = sys.argv[1] if len(sys.argv) > 1 else f"default_{os.getpid()}"
    state_file = f"/tmp/count_state_{task_id}.json"

    # Load existing state (for exactly-once recovery within same job)
    counts = {}
    if os.path.exists(state_file):
        try:
            with open(state_file, 'r') as f:
                counts = json.load(f)
        except Exception:
            counts = {}

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) == 2:
            group_key, value = parts
        else:
            group_key = ""
            value = line

        # Use the incoming key as the group key
        # (Filter stage should have set this to the Nth column value)

        # Increment count
        if group_key not in counts:
            counts[group_key] = 0
        counts[group_key] += 1

        # Output current count for this key
        print(f"{group_key}\t{counts[group_key]}")

    # Save state for recovery
    with open(state_file, 'w') as f:
        json.dump(counts, f)

if __name__ == '__main__':
    main()
