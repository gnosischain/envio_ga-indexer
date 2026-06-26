#!/bin/bash
set -e

export PYTHONPATH="/app:${PYTHONPATH}"

echo "Waiting for ClickHouse to be ready..."
until python -c "
import sys
sys.path.insert(0, '/app')
from src.services.clickhouse import ClickHouse
try:
    ClickHouse().client.command('SELECT 1')
    print('ClickHouse is ready!')
except Exception as e:
    print(f'ClickHouse not ready: {e}')
    sys.exit(1)
"; do
    echo "ClickHouse is unavailable - sleeping"
    sleep 5
done

echo "Starting: $@"
exec "$@"
