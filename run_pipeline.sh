#!/usr/bin/env bash
# Cron wrapper: cron decides WHEN; run_pipeline.py decides WHAT/HOW. Uses the project's own virtualenv.
# Exit code is passed through from run_pipeline.py (0 ok, 1 task failure, 2 fatal, 3 already running).
set -u
PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_DIR" || exit 2

if   [ -x "$PROJECT_DIR/.venv/bin/python" ];         then PY="$PROJECT_DIR/.venv/bin/python"          # Linux / WSL
elif [ -x "$PROJECT_DIR/.venv/Scripts/python.exe" ]; then PY="$PROJECT_DIR/.venv/Scripts/python.exe"  # Git Bash on Windows
else echo "No .venv found in $PROJECT_DIR (create it: python3 -m venv .venv && .venv/bin/pip install -r requirements.txt)" >&2; exit 2; fi

mkdir -p logs
exec "$PY" run_pipeline.py >> logs/cron.log 2>&1
