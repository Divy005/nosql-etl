#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Script: get_data.sh
# Purpose: Idempotently download and decompress NASA HTTP access logs (Jul/Aug 1995)
#          If data/NASA_access_log_*95.gz already exist (downloaded manually), they are
#          decompressed instead of re-downloaded. Decompression is the only change made.
# ==============================================================================

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
DATA_DIR="${ROOT_DIR}/data"

mkdir -p "${DATA_DIR}"

JUL_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"
AUG_URL="https://ita.ee.lbl.gov/traces/NASA_access_log_Aug95.gz"

download_and_extract() {
    local url="$1"
    local gz_file="$2"
    local raw_file="$3"

    if [ -f "${raw_file}" ]; then
        echo "[INFO] ${raw_file} already exists. Skipping download and decompression."
        return 0
    fi

    if [ -f "${gz_file}" ]; then
        # Archive already downloaded by hand (e.g. in a browser): just decompress it.
        echo "[INFO] Found ${gz_file}; decompressing (keeping the .gz)..."
        gzip -dkf "${gz_file}"
        echo "[INFO] Created ${raw_file}."
        return 0
    fi

    echo "[INFO] Downloading ${url}..."
    if command -v curl &>/dev/null; then
        curl -fSL -o "${gz_file}" "${url}"
    elif command -v wget &>/dev/null; then
        wget -O "${gz_file}" "${url}"
    else
        echo "[ERROR] Neither curl nor wget found. Please install one of them." >&2
        exit 1
    fi

    echo "[INFO] Decompressing ${gz_file}..."
    gzip -d -f "${gz_file}"
    echo "[INFO] Created ${raw_file}."
}

echo "=== Fetching NASA Access Log Datasets ==="
download_and_extract "${JUL_URL}" "${DATA_DIR}/NASA_access_log_Jul95.gz" "${DATA_DIR}/NASA_access_log_Jul95"
download_and_extract "${AUG_URL}" "${DATA_DIR}/NASA_access_log_Aug95.gz" "${DATA_DIR}/NASA_access_log_Aug95"

echo "=== Datasets are ready in ${DATA_DIR} ==="
ls -lh "${DATA_DIR}"
