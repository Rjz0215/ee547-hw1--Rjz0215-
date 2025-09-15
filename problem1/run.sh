#!/bin/bash
set -euo pipefail

# Accept exactly 2 arguments
if [ "$#" -ne 2 ]; then
  echo "Usage: $0 <input_file> <output_directory>"
  exit 1
fi

INPUT_FILE="$1"
OUTPUT_DIR="$2"

# Verify input file exists
if [ ! -f "$INPUT_FILE" ]; then
  echo "Error: Input file $INPUT_FILE does not exist"
  exit 1
fi

# Create output directory if it doesn't exist
mkdir -p "$OUTPUT_DIR"

# Use realpath to convert relative paths to absolute paths
INPUT_PATH="$(realpath "$INPUT_FILE")"
OUTPUT_PATH="$(realpath "$OUTPUT_DIR")"

# If running under Git Bash (MINGW/MSYS), convert /c/... -> C:/...
UNAME_STR="$(uname 2>/dev/null || echo '')"
if echo "$UNAME_STR" | grep -iqE 'mingw|msys'; then
  if [[ "$INPUT_PATH" =~ ^/([a-zA-Z])/(.*)$ ]]; then
    DRIVE="${BASH_REMATCH[1]}"
    REST="${BASH_REMATCH[2]}"
    INPUT_PATH="${DRIVE^^}:/${REST}"
  fi
  if [[ "$OUTPUT_PATH" =~ ^/([a-zA-Z])/(.*)$ ]]; then
    DRIVE="${BASH_REMATCH[1]}"
    REST="${BASH_REMATCH[2]}"
    OUTPUT_PATH="${DRIVE^^}:/${REST}"
  fi
fi

# Run container
docker run --rm \
  --name http-fetcher \
  -v "$INPUT_PATH":/data/input/urls.txt:ro \
  -v "$OUTPUT_PATH":/data/output \
  http-fetcher:latest
