#!/usr/bin/env bash
set -euo pipefail

export CINDER_DATABASE_URL="${CINDER_DATABASE_URL:-postgres://cinder:cinder@127.0.0.1:55432/cinder}"

cargo test -p cinder-runtime --test e2e -- --nocapture

