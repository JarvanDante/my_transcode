#!/usr/bin/env bash
# 本机验证：mp4 → H.264 HLS（不依赖 Kafka/MinIO）
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 <input.mp4> [out_dir]"
  exit 1
fi

IN="$1"
OUT="${2:-./out_hls}"
mkdir -p "$OUT"

ffmpeg -y -i "$IN" \
  -c:v libx264 -preset veryfast -crf 23 \
  -c:a aac -b:a 128k \
  -hls_time 6 -hls_list_size 0 -hls_playlist_type vod \
  -f hls "$OUT/index.m3u8"

echo "done: $OUT/index.m3u8"
ls -la "$OUT"
