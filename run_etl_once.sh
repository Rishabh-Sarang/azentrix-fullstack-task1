#!/usr/bin/env bash
set -euo pipefail

# Change to the repository root so paths are consistent.
cd "$(dirname "$0")"

PYTHON_BIN=".venv/bin/python"
if [[ ! -x "$PYTHON_BIN" ]]; then
  echo "Warning: virtualenv Python not found at $PYTHON_BIN. Falling back to system Python."
  PYTHON_BIN="python"
fi

# Run the ETL once and exit.
exec "$PYTHON_BIN" pipeline/etl.py --once
