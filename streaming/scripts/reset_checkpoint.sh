#!/usr/bin/env bash
set -euo pipefail

CHECKPOINT_PATH="${CHECKPOINT_PATH:-$HOME/checkpoints/news-stream}"

echo "Removing checkpoint path: $CHECKPOINT_PATH"
rm -rf "$CHECKPOINT_PATH"
mkdir -p "$CHECKPOINT_PATH"
