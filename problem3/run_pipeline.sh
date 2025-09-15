#!/bin/bash

if [ $# -lt 1 ]; then
    echo "Usage: $0 <url1> [url2] [url3] ..."
    exit 1
fi

echo "Starting Multi-Container Pipeline"
echo "================================="

MSYS_NO_PATHCONV=1 docker-compose down -v 2>/dev/null || true

TEMP_DIR=$(mktemp -d)
trap "rm -rf $TEMP_DIR" EXIT

for url in "$@"; do
    echo "$url" >> "$TEMP_DIR/urls.txt"
done

echo "URLs to process:"
cat "$TEMP_DIR/urls.txt"
echo ""

echo "Building containers..."
MSYS_NO_PATHCONV=1 docker-compose build --quiet

echo "Starting pipeline..."
MSYS_NO_PATHCONV=1 docker-compose up -d

sleep 3

echo "Injecting URLs..."
MSYS_NO_PATHCONV=1 docker exec pipeline-fetcher sh -lc 'mkdir -p /shared/input || true'

URLS_SRC="$TEMP_DIR/urls.txt"
UNAME_LC="$(uname 2>/dev/null | tr '[:upper:]' '[:lower:]')"
if echo "$UNAME_LC" | grep -qE 'msys|mingw'; then
  if command -v cygpath >/dev/null 2>&1; then
    URLS_SRC_WIN="$(cygpath -w "$URLS_SRC")"
  else
    if [[ "$URLS_SRC" =~ ^/([a-zA-Z])/(.*)$ ]]; then
      DRIVE="${BASH_REMATCH[1]}"; REST="${BASH_REMATCH[2]}"; URLS_SRC_WIN="${DRIVE}:/${REST}"
    else
      URLS_SRC_WIN="$URLS_SRC"
    fi
  fi
else
  URLS_SRC_WIN="$URLS_SRC"
fi

OK=0
for i in $(seq 1 5); do
    MSYS_NO_PATHCONV=1 docker cp "$URLS_SRC_WIN" pipeline-fetcher:/shared/input/urls.txt 2>/dev/null || true
    if MSYS_NO_PATHCONV=1 docker exec pipeline-fetcher sh -lc 'test -s /shared/input/urls.txt'; then
        OK=1
        break
    fi
    echo "Retry injecting URLs... ($i/5)"
    sleep 2
done

if [ "$OK" -ne 1 ]; then
    echo "Failed to inject URLs into fetcher."
    MSYS_NO_PATHCONV=1 docker-compose logs fetcher || true
    MSYS_NO_PATHCONV=1 docker-compose down || true
    exit 1
fi

echo "Processing..."
MAX_WAIT=300
ELAPSED=0
while [ $ELAPSED -lt $MAX_WAIT ]; do
    if MSYS_NO_PATHCONV=1 docker run --rm -v pipeline-shared-data:/shared alpine sh -lc 'test -f /shared/analysis/final_report.json'; then
        echo "Pipeline complete"
        break
    fi
    sleep 5
    ELAPSED=$((ELAPSED + 5))
done

if [ $ELAPSED -ge $MAX_WAIT ]; then
    echo "Pipeline timeout after ${MAX_WAIT} seconds"
    MSYS_NO_PATHCONV=1 docker-compose logs || true
    MSYS_NO_PATHCONV=1 docker-compose down || true
    exit 1
fi

mkdir -p output output/status
MSYS_NO_PATHCONV=1 docker run --rm -v pipeline-shared-data:/shared alpine sh -lc 'cat /shared/analysis/final_report.json' > output/final_report.json
MSYS_NO_PATHCONV=1 docker run --rm -v pipeline-shared-data:/shared alpine sh -lc 'cat /shared/status/fetch_complete.json 2>/dev/null' > output/status/fetch_complete.json
MSYS_NO_PATHCONV=1 docker run --rm -v pipeline-shared-data:/shared alpine sh -lc 'cat /shared/status/process_complete.json 2>/dev/null' > output/status/process_complete.json
MSYS_NO_PATHCONV=1 docker run --rm -v pipeline-shared-data:/shared alpine sh -lc 'cat /shared/status/analyze_complete.json 2>/dev/null' > output/status/analyze_complete.json

MSYS_NO_PATHCONV=1 docker-compose down

if [ -f "output/final_report.json" ]; then
    echo ""
    echo "Results saved to output/final_report.json"
    python3 -m json.tool output/final_report.json | head -20
else
    echo "Pipeline failed - no output generated"
    exit 1
fi
