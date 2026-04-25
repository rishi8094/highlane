#!/usr/bin/env bash
# Fetch the Lighter Schnorr signer shared library for the host platform.
# The library is the Go-compiled signer that the official Lighter Python SDK ships.
# We dlopen it from Rust via libloading; same C ABI either way.
#
# Output: vendor/lighter/<platform>/lighter-signer-<platform>.{so|dylib|dll}

set -euo pipefail

REPO="elliottech/lighter-python"
BRANCH="main"
BASE_URL="https://raw.githubusercontent.com/${REPO}/${BRANCH}/lighter/signers"

uname_s="$(uname -s)"
uname_m="$(uname -m)"

case "${uname_s}-${uname_m}" in
    Darwin-arm64)
        platform="darwin-arm64"
        ext="dylib"
        ;;
    Linux-x86_64)
        platform="linux-amd64"
        ext="so"
        ;;
    Linux-aarch64)
        platform="linux-arm64"
        ext="so"
        ;;
    *)
        echo "Unsupported host platform: ${uname_s}-${uname_m}" >&2
        echo "Supported: Darwin-arm64, Linux-x86_64, Linux-aarch64" >&2
        exit 1
        ;;
esac

filename="lighter-signer-${platform}.${ext}"
script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
out_dir="${script_dir}/../vendor/lighter/${platform}"
mkdir -p "${out_dir}"
out="${out_dir}/${filename}"

if [[ -f "${out}" ]]; then
    echo "Already present: ${out}" >&2
    exit 0
fi

echo "Downloading ${filename} from ${REPO}@${BRANCH}..." >&2
curl --fail --location --silent --show-error \
    --output "${out}" \
    "${BASE_URL}/${filename}"

# Also fetch the matching header for reference (not used at runtime).
curl --fail --location --silent --show-error \
    --output "${out_dir}/lighter-signer-${platform}.h" \
    "${BASE_URL}/lighter-signer-${platform}.h" || true

bytes="$(wc -c < "${out}" | tr -d ' ')"
echo "Wrote ${out} (${bytes} bytes)" >&2
