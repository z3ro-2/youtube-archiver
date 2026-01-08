#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="${PROJECT_DIR:-$(cd "$SCRIPT_DIR/.." && pwd)}"
LOG_DIR="${YT_ARCHIVER_LOG_DIR:-$PROJECT_DIR/logs}"
VENV_PIP="$PROJECT_DIR/.venv/bin/pip"

mkdir -p "$LOG_DIR"
LOGFILE="$LOG_DIR/yt-dlp-update.log"

{
  echo "----- $(date) -----"

  if [ -x "$VENV_PIP" ]; then
    "$VENV_PIP" install -U yt-dlp
    echo "yt-dlp updated via venv pip"
    exit 0
  fi

  if command -v python >/dev/null 2>&1; then
    python -m pip install -U yt-dlp
    echo "yt-dlp updated via python -m pip"
    exit 0
  fi

  if command -v python3 >/dev/null 2>&1; then
    python3 -m pip install -U yt-dlp
    echo "yt-dlp updated via python3 -m pip"
    exit 0
  fi

  echo "No Python interpreter found; yt-dlp update failed."
  exit 1
} >> "$LOGFILE" 2>&1
