#!/bin/bash
# Cleanup script for StreamProcess / ReplicatedFS testing
# Run this to clean up files between test runs

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
STREAM_DIR="$SCRIPT_DIR"
RFS_DIR="$(dirname "$SCRIPT_DIR")/ReplicatedFS"
MEMBERSHIP_DIR="$(dirname "$SCRIPT_DIR")/MembershipProtocol"

echo "=== StreamProcess Cleanup ==="
echo "Working directory: $SCRIPT_DIR"

# --- Kill orphaned processes FIRST ---
echo ""
echo "--- Killing orphaned processes ---"

# Kill any running task.py processes
task_pids=$(pgrep -f "python.*task.py" 2>/dev/null || true)
if [ -n "$task_pids" ]; then
    echo "Killing task.py processes: $task_pids"
    echo "$task_pids" | xargs kill -9 2>/dev/null || true
fi

# Kill any running streamprocess.py processes (but not cleanup or submit commands)
streamprocess_pids=$(pgrep -f "python.*streamprocess.py.*(--leader|--vm-id)" 2>/dev/null || true)
if [ -n "$streamprocess_pids" ]; then
    echo "Killing streamprocess.py processes: $streamprocess_pids"
    echo "$streamprocess_pids" | xargs kill -9 2>/dev/null || true
fi

# Give processes time to die
sleep 1

# Count files before cleanup
count_files() {
    local pattern="$1"
    local dir="$2"
    find "$dir" -maxdepth 1 -name "$pattern" 2>/dev/null | wc -l | tr -d ' '
}

# --- StreamProcess cleanup ---
echo ""
echo "--- StreamProcess ---"

# Task logs: task_*.log
n=$(count_files "task_*.log" "$STREAM_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n task log files (task_*.log)"
    rm -f "$STREAM_DIR"/task_*.log
fi

# Leader/worker logs: leader_*.log, streamprocess_*.log
n=$(count_files "leader_*.log" "$STREAM_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n leader log files (leader_*.log)"
    rm -f "$STREAM_DIR"/leader_*.log
fi

n=$(count_files "streamprocess_*.log" "$STREAM_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n streamprocess log files (streamprocess_*.log)"
    rm -f "$STREAM_DIR"/streamprocess_*.log
fi

# Output files: output_*.txt
n=$(count_files "output_*.txt" "$STREAM_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n output files (output_*.txt)"
    rm -f "$STREAM_DIR"/output_*.txt
fi

# Exactly-once logs: eo_log_*.log
n=$(count_files "eo_log_*.log" "$STREAM_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n exactly-once log files (eo_log_*.log)"
    rm -f "$STREAM_DIR"/eo_log_*.log
fi

# --- ReplicatedFS cleanup ---
echo ""
echo "--- ReplicatedFS ---"

# Storage directories: rfs_storage_*
for dir in "$RFS_DIR"/rfs_storage_*; do
    if [ -d "$dir" ]; then
        echo "Removing ReplicatedFS storage: $dir"
        rm -rf "$dir"
    fi
done

# Also check in StreamProcess directory
for dir in "$STREAM_DIR"/rfs_storage_*; do
    if [ -d "$dir" ]; then
        echo "Removing ReplicatedFS storage: $dir"
        rm -rf "$dir"
    fi
done

# --- Membership cleanup ---
echo ""
echo "--- MembershipProtocol ---"

# Machine logs: machine.*.log
n=$(count_files "machine.*.log" "$MEMBERSHIP_DIR")
if [ "$n" -gt 0 ]; then
    echo "Removing $n machine log files (machine.*.log)"
    rm -f "$MEMBERSHIP_DIR"/machine.*.log
fi

# Also check sibling directories
for d in "$RFS_DIR" "$STREAM_DIR"; do
    n=$(count_files "machine.*.log" "$d")
    if [ "$n" -gt 0 ]; then
        echo "Removing $n machine log files in $(basename "$d")"
        rm -f "$d"/machine.*.log
    fi
done

# --- /tmp cleanup ---
echo ""
echo "--- /tmp files ---"

# StreamProcess source temp files
n=$(find /tmp -maxdepth 1 -name "streamprocess_src_*.csv" 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -gt 0 ]; then
    echo "Removing $n stream source temp files"
    rm -f /tmp/streamprocess_src_*.csv
fi

# Count operator state files
n=$(find /tmp -maxdepth 1 -name "count_state_*.json" 2>/dev/null | wc -l | tr -d ' ')
if [ "$n" -gt 0 ]; then
    echo "Removing $n count operator state files"
    rm -f /tmp/count_state_*.json
fi

# --- Python cache cleanup ---
echo ""
echo "--- Python cache ---"

for d in "$MEMBERSHIP_DIR" "$RFS_DIR" "$STREAM_DIR"; do
    if [ -d "$d/__pycache__" ]; then
        echo "Removing __pycache__ in $(basename "$d")"
        rm -rf "$d/__pycache__"
    fi
done

echo ""
echo "--- Setting permissions ---"

# Make operator scripts executable
chmod +x "$STREAM_DIR"/*.py 2>/dev/null && echo "Made Python scripts executable"

echo ""
echo "=== Cleanup complete ==="
