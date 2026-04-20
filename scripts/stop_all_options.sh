#!/bin/bash
# Stop all options-server and exeopt processes so you can start over.
# Run from anywhere. Frees port 8001 and kills any Python exeopt/options_server.
#
# Usage:
#   cd "/Volumes/Extreme Pro/App gpt/stock_api-main_updated"
#   bash scripts/stop_all_options.sh

set -e
echo "=== Checking what is running ==="
echo "Port 8001 (options server):"
lsof -nP -iTCP:8001 -sTCP:LISTEN 2>/dev/null || echo "  (nothing)"
echo ""
echo "Python options/exeopt processes:"
pgrep -fl "python.*run_options_server|python.*exeopt" 2>/dev/null || echo "  (none)"
echo ""
echo "=== Stopping options server and related processes ==="

# 1. Kill whatever is listening on port 8001 (options server)
if command -v lsof >/dev/null 2>&1; then
  PIDS=$(lsof -nP -iTCP:8001 -sTCP:LISTEN -t 2>/dev/null || true)
  if [ -n "$PIDS" ]; then
    echo "Stopping process(es) on port 8001: $PIDS"
    echo "$PIDS" | xargs kill 2>/dev/null || true
    sleep 1
    # Force kill if still there
    lsof -nP -iTCP:8001 -sTCP:LISTEN -t 2>/dev/null | xargs kill -9 2>/dev/null || true
  else
    echo "Nothing listening on port 8001."
  fi
else
  echo "lsof not found; skipping port 8001 check."
fi

# 2. Kill any Python process running run_options_server or exeopt (optional, in case of stuck subprocesses)
if command -v pgrep >/dev/null 2>&1; then
  for name in "run_options_server" "exeopt"; do
    PIDS=$(pgrep -f "python.*$name" 2>/dev/null || true)
    if [ -n "$PIDS" ]; then
      echo "Stopping Python ($name): $PIDS"
      echo "$PIDS" | xargs kill 2>/dev/null || true
    fi
  done
fi

echo "Done. You can start fresh with: python run_options_server.py"
