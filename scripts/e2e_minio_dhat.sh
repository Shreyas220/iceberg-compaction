#!/usr/bin/env bash
set -euo pipefail

TABLE_NAME=${TABLE_NAME:-"default.compaction_test"}
CATALOG_URI=${CATALOG_URI:-"http://localhost:8181"}
S3_ENDPOINT=${S3_ENDPOINT:-"http://localhost:9000"}
S3_ACCESS_KEY=${S3_ACCESS_KEY:-"minioadmin"}
S3_SECRET_KEY=${S3_SECRET_KEY:-"minioadmin"}
S3_BUCKET=${S3_BUCKET:-"warehouse"}
TOTAL_SIZE=${TOTAL_SIZE:-"3GB"}
FILE_SIZE=${FILE_SIZE:-"64MB"}
PARALLELISM=${PARALLELISM:-"8"}

export RUST_LOG=${RUST_LOG:-"info"}
export DHAT_OUT=${DHAT_OUT:-"dhat-local-compaction.json"}

echo "==> Generating ${TOTAL_SIZE} of data for ${TABLE_NAME}"
cargo run -p compaction-planner --example data_generator -- \
  --table "${TABLE_NAME}" \
  --total-size "${TOTAL_SIZE}" \
  --file-size "${FILE_SIZE}" \
  --parallelism "${PARALLELISM}" \
  --catalog-uri "${CATALOG_URI}" \
  --s3-endpoint "${S3_ENDPOINT}" \
  --s3-access-key "${S3_ACCESS_KEY}" \
  --s3-secret-key "${S3_SECRET_KEY}" \
  --s3-bucket "${S3_BUCKET}"

echo "==> Running local compaction with dhat heap profiling"
cargo run -p compaction-planner --example local_compaction --features dhat-heap -- \
  --table "${TABLE_NAME}" \
  --s3-endpoint "${S3_ENDPOINT}" \
  --s3-access-key "${S3_ACCESS_KEY}" \
  --s3-secret-key "${S3_SECRET_KEY}" \
  --s3-bucket "${S3_BUCKET}" \
  --verify-multipart

echo "==> dhat output: ${DHAT_OUT}"
