#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")"

if [[ ! -d .venv ]]; then
  echo "Tworzę venv (Python 3.11)…"
  python3.11 -m venv .venv
  .venv/bin/pip install -r requirements.txt
fi

if ! .venv/bin/python -c "import pyflink" 2>/dev/null; then
  echo "Błąd: brak pyflink. Usuń .venv i uruchom ponownie: rm -rf .venv && ./run.sh"
  exit 1
fi

export KAFKA_BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-localhost:9092}"
export PYFLINK_CLIENT_EXECUTABLE="$(pwd)/.venv/bin/python"
exec .venv/bin/python fraud_detector.py
