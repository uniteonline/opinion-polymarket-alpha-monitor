#!/usr/bin/env bash
set -euo pipefail

TS=$(date +"%Y-%m-%d")
SRC_DIR="./records"
DST_DIR="./records/archive"

mkdir -p "$DST_DIR"

if [ -f "$SRC_DIR/alpha_log.jsonl" ]; then
  mv "$SRC_DIR/alpha_log.jsonl" "$DST_DIR/alpha_log_${TS}.jsonl"
  gzip -9 "$DST_DIR/alpha_log_${TS}.jsonl"
fi

if [ -f "$SRC_DIR/alpha_log.csv" ]; then
  mv "$SRC_DIR/alpha_log.csv" "$DST_DIR/alpha_log_${TS}.csv"
  gzip -9 "$DST_DIR/alpha_log_${TS}.csv"
fi
