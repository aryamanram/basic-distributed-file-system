#!/usr/bin/env python3
"""
Filter operator - accepts lines containing a pattern.
Usage: ./filter_op.py <pattern> [group_column]
Input via stdin: key<TAB>value (line)
Output to stdout: key<TAB>value (if pattern in value)

Arguments:
  pattern       - Case-sensitive pattern to match
  group_column  - Optional: column index (0-based) to use as output key
                  for proper partitioning to AggregateByKey stage

Examples:
  Application 1 (Filter & Count): ./filter_op.py "Sign" 7
    - Filters for "Sign", sets output key to column 7 for counting
  Application 2 (Filter & Transform): ./filter_op.py "Street"
    - Filters for "Street", keeps original key
"""
import sys

def main():
    if len(sys.argv) < 2:
        print("Usage: filter_op.py <pattern> [group_column] [task_id]", file=sys.stderr)
        sys.exit(1)

    pattern = sys.argv[1]
    # Optional: column index for group key (for Application 1: Filter & Count)
    # Note: task_id may be appended at the end, so check if argv[2] is a number
    group_col = None
    if len(sys.argv) > 2:
        try:
            group_col = int(sys.argv[2])
        except ValueError:
            # argv[2] is probably the task_id, not a column index
            pass

    for line in sys.stdin:
        line = line.strip()
        if not line:
            continue

        parts = line.split('\t', 1)
        if len(parts) == 2:
            key, value = parts
        else:
            key = ""
            value = line

        # Case-sensitive string matching
        if pattern in value:
            # If group_col specified, use that column as the output key
            # This ensures proper partitioning for downstream aggregation
            if group_col is not None:
                csv_parts = value.split(',')
                if group_col < len(csv_parts):
                    key = csv_parts[group_col].strip()
                else:
                    key = ""  # Missing data = empty string
            print(f"{key}\t{value}")

if __name__ == '__main__':
    main()
